// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/pooling"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// These tests are for what the event loop made possible to get wrong. A session
// used to be two goroutines with blocking reads, and blocking gave three things
// for free: a client waiting for a backend was simply parked, a message larger
// than a buffer was simply read in a loop, and a batch that arrived in two
// segments was simply read twice. As a state machine each of those is a case
// somebody had to write.

// Many more clients than backends, every one of them pipelining, for long
// enough that the queue for a backend is never empty and is being popped from
// every worker at once.
//
// The precise defect this guards against is pinned step by step in
// TestSessionClaimedButNotYetGrantedDoesNotQueueAgain, because under load its
// window is a few instructions wide and this test does not reliably find it. What
// this one is for is everything around it: that a queued request is sent when
// its turn comes, that a client which sends again while queued is not served out
// of order, and that no reply is ever delivered to a client that did not ask for
// it — which would not show up as an error.
func TestProxyServesMoreClientsThanBackendsWithoutLosingOne(t *testing.T) {
	const (
		backends = 2
		clients  = 32
		rounds   = 150
	)
	url := startProxy(t, pooling.Config{MaxConns: backends})

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	failures := make(chan error, clients)
	for client := range clients {
		wg.Go(func() {
			conn, err := pgx.Connect(ctx, url)
			if err != nil {
				failures <- fmt.Errorf("client %d: connect: %w", client, err)
				return
			}
			defer conn.Close(context.Background())

			pipeline := conn.PgConn().StartPipeline(ctx)
			defer pipeline.Close()

			for round := range rounds {
				first, second := client*rounds*2+round*2, client*rounds*2+round*2+1
				for _, value := range []int{first, second} {
					pipeline.SendQueryParams("SELECT $1::int4", [][]byte{[]byte(strconv.Itoa(value))}, nil, nil, nil)
					pipeline.SendPipelineSync()
					if err := pipeline.Flush(); err != nil {
						failures <- fmt.Errorf("client %d, round %d: flush: %w", client, round, err)
						return
					}
				}
				for _, want := range []int{first, second} {
					got, err := pipelinedAnswer(pipeline)
					if err != nil {
						failures <- fmt.Errorf("client %d, round %d: %w", client, round, err)
						return
					}
					if got != strconv.Itoa(want) {
						failures <- fmt.Errorf("client %d, round %d: got %s, want %d", client, round, got, want)
						return
					}
				}
			}
		})
	}
	wg.Wait()
	close(failures)

	for err := range failures {
		t.Error(err)
	}
}

// pipelinedAnswer reads one single-value result and the sync that follows it.
func pipelinedAnswer(pipeline *pgconn.Pipeline) (string, error) {
	results, err := pipeline.GetResults()
	if err != nil {
		return "", err
	}
	reader, ok := results.(*pgconn.ResultReader)
	if !ok {
		return "", fmt.Errorf("pipeline returned %T, want a result", results)
	}
	result := reader.Read()
	if result.Err != nil {
		return "", result.Err
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		return "", fmt.Errorf("pipeline returned %d rows, want one value", len(result.Rows))
	}

	if _, err := pipeline.GetResults(); err != nil {
		return "", err
	}
	return string(result.Rows[0][0]), nil
}

// A backend was handed on as soon as a ReadyForQuery arrived with nothing
// pending. A pipelining client whose second batch reached the proxy split — the
// Parse, Bind and Execute in one segment, the Sync in the next — had those three
// forwarded before the first batch's ReadyForQuery came back, so the backend was
// released with a statement bound and a portal open on it. The Sync then went to
// whichever backend was next, and the results of the Execute were delivered to
// whoever synced the original one.
//
// One backend makes the handover certain rather than likely: if it is released,
// the second client is the one who gets it.
func TestProxyKeepsTheBackendForABatchThatArrivesInTwoSegments(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 1})

	config, err := pgconn.ParseConfig(url)
	if err != nil {
		t.Fatalf("ParseConfig() = %v", err)
	}
	conn, err := pgconn.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatalf("ConnectConfig() = %v", err)
	}
	raw, err := conn.Hijack()
	if err != nil {
		t.Fatalf("Hijack() = %v", err)
	}
	defer raw.Conn.Close()
	_ = raw.Conn.SetDeadline(time.Now().Add(30 * time.Second))

	batch := func(sql string) []pgproto3.FrontendMessage {
		return []pgproto3.FrontendMessage{
			&pgproto3.Parse{Query: sql},
			&pgproto3.Bind{},
			&pgproto3.Execute{},
		}
	}

	// The whole first batch, and the second up to but not including its Sync.
	for _, message := range append(append(batch("SELECT 'first'"), &pgproto3.Sync{}), batch("SELECT 'second'")...) {
		raw.Frontend.Send(message)
	}
	if err := raw.Frontend.Flush(); err != nil {
		t.Fatalf("Flush() = %v", err)
	}
	if rows := readUntilReady(t, raw.Frontend); !rowsEqual(rows, "first") {
		t.Fatalf("first batch returned %q, want [first]", rows)
	}

	// Another client, while the second batch is still open. With one backend it
	// can only be served by taking the one the first client is part-way through.
	intruder := make(chan error, 1)
	go func() {
		other, err := pgx.Connect(context.Background(), url)
		if err != nil {
			intruder <- err
			return
		}
		defer other.Close(context.Background())

		var got string
		if err := other.QueryRow(context.Background(), "SELECT 'intruder'").Scan(&got); err != nil {
			intruder <- err
			return
		}
		if got != "intruder" {
			err = fmt.Errorf("the second client received %q, which was not its own result", got)
		}
		intruder <- err
	}()

	select {
	case err := <-intruder:
		t.Fatalf("a second client was served (err = %v) while the first still had a batch open on the only backend", err)
	case <-time.After(300 * time.Millisecond):
	}

	raw.Frontend.Send(&pgproto3.Sync{})
	if err := raw.Frontend.Flush(); err != nil {
		t.Fatalf("Flush() = %v", err)
	}
	if rows := readUntilReady(t, raw.Frontend); !rowsEqual(rows, "second") {
		t.Fatalf("second batch returned %q, want [second]", rows)
	}

	select {
	case err := <-intruder:
		if err != nil {
			t.Fatalf("second client = %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the second client was never served after the first reached its boundary")
	}
}

// readUntilReady collects the first column of every DataRow up to the next
// ReadyForQuery.
func readUntilReady(t *testing.T, frontend *pgproto3.Frontend) []string {
	t.Helper()

	var rows []string
	for {
		message, err := frontend.Receive()
		if err != nil {
			t.Fatalf("Receive() = %v", err)
		}
		switch message := message.(type) {
		case *pgproto3.DataRow:
			rows = append(rows, string(message.Values[0]))
		case *pgproto3.ErrorResponse:
			t.Fatalf("server error: %s (%s)", message.Message, message.Code)
		case *pgproto3.ReadyForQuery:
			return rows
		}
	}
}

func rowsEqual(got []string, want ...string) bool {
	return strings.Join(got, "\x00") == strings.Join(want, "\x00")
}

// Rows far larger than the buffer they are read through, in both directions.
// Each crosses the proxy as a header followed by several reads' worth of body,
// which is the path where a miscounted remainder turns the next row's bytes into
// a message header.
func TestProxyRelaysRowsLargerThanItsBuffer(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 2}))

	const rows = 40
	size := readBufSize*3 + 17

	result, err := conn.Query(t.Context(),
		"SELECT n, repeat(chr(65 + n % 26), $1::int) FROM generate_series(1, $2::int) AS n", size, rows)
	if err != nil {
		t.Fatalf("Query() = %v", err)
	}
	defer result.Close()

	seen := 0
	for result.Next() {
		var n int
		var payload string
		if err := result.Scan(&n, &payload); err != nil {
			t.Fatalf("Scan() = %v", err)
		}
		seen++
		if n != seen {
			t.Fatalf("row %d arrived as row %d", n, seen)
		}
		want := strings.Repeat(string(rune('A'+n%26)), size)
		if payload != want {
			t.Fatalf("row %d: %d bytes, starting %q; want %d of %q", n, len(payload), payload[:1], size, want[:1])
		}
	}
	if err := result.Err(); err != nil {
		t.Fatalf("rows = %v", err)
	}
	if seen != rows {
		t.Fatalf("got %d rows, want %d", seen, rows)
	}
}

// The same, client to server: a parameter larger than the buffer, which the
// server has to receive intact for the length it reports to match.
func TestProxyRelaysParametersLargerThanItsBuffer(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 2})
	payload := bytes.Repeat([]byte("0123456789abcdef"), readBufSize/4)

	// Once unnamed and once through a named prepared statement. The second is
	// a Bind the proxy has to read whole before it can forward it.
	for name, url := range map[string]string{"unnamed": url, "named": cachingURL(url)} {
		t.Run(name, func(t *testing.T) {
			conn := connect(t, url)

			for range 2 {
				var length int
				var digest string
				if err := conn.QueryRow(t.Context(), "SELECT length($1::bytea), md5($1::bytea)", payload).Scan(&length, &digest); err != nil {
					t.Fatalf("QueryRow() = %v", err)
				}
				if length != len(payload) {
					t.Fatalf("server received %d bytes, want %d", length, len(payload))
				}

				var want string
				if err := conn.QueryRow(t.Context(), "SELECT md5(repeat('0123456789abcdef', $1::int)::bytea)", readBufSize/4).Scan(&want); err != nil {
					t.Fatalf("QueryRow() = %v", err)
				}
				if digest != want {
					t.Fatalf("server received different bytes: md5 %s, want %s", digest, want)
				}
			}
		})
	}
}

// COPY is the one exchange where the client sends continuously while the server
// says nothing, so it is what exercises backpressure towards the server: the
// proxy must stop reading a client the backend is not keeping up with, and start
// again when it does.
func TestProxyRelaysCopyIn(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 1}))

	const rows = 20_000
	source := make([][]any, rows)
	for i := range source {
		source[i] = []any{i, strings.Repeat("x", 200)}
	}

	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	defer tx.Rollback(context.Background())

	if _, err := tx.Exec(t.Context(), "CREATE TEMP TABLE gpoolproxy_copy (id int, payload text) ON COMMIT DROP"); err != nil {
		t.Fatalf("CREATE TEMP TABLE = %v", err)
	}
	copied, err := tx.CopyFrom(t.Context(), pgx.Identifier{"gpoolproxy_copy"}, []string{"id", "payload"}, pgx.CopyFromRows(source))
	if err != nil {
		t.Fatalf("CopyFrom() = %v", err)
	}
	if copied != rows {
		t.Fatalf("CopyFrom() copied %d rows, want %d", copied, rows)
	}

	var count, total int
	if err := tx.QueryRow(t.Context(), "SELECT count(*), sum(length(payload)) FROM gpoolproxy_copy").Scan(&count, &total); err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}
	if count != rows || total != rows*200 {
		t.Fatalf("table holds %d rows and %d bytes, want %d and %d", count, total, rows, rows*200)
	}
}

// A TLS client cannot be served as a bare descriptor: what is readable on its
// socket is ciphertext. It is bridged to one instead, and this is the only test
// that goes through the bridge.
func TestProxyServesTLSClients(t *testing.T) {
	certificate, key := selfSigned(t)
	url := startProxyWith(t, func(config *Config) {
		config.Pool = pooling.Config{MaxConns: 2}
		config.TLSCert, config.TLSKey = certificate, key
	})
	conn := connect(t, strings.Replace(url, "sslmode=disable", "sslmode=require", 1))

	if state, ok := conn.PgConn().Conn().(interface{ Handshake() error }); !ok || state.Handshake() != nil {
		t.Fatal("the client connection is not TLS, so this did not test the bridge")
	}

	// Small, then large enough to need several records each way.
	for _, size := range []int{1, readBufSize * 2} {
		var got string
		if err := conn.QueryRow(t.Context(), "SELECT repeat('t', $1::int)", size).Scan(&got); err != nil {
			t.Fatalf("QueryRow(%d) = %v", size, err)
		}
		if len(got) != size {
			t.Fatalf("got %d bytes, want %d", len(got), size)
		}
	}
}

// selfSigned writes a throwaway certificate and key and returns their paths.
func selfSigned(t *testing.T) (certificate, key string) {
	t.Helper()

	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() = %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gpoolproxy-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &private.PublicKey, private)
	if err != nil {
		t.Fatalf("CreateCertificate() = %v", err)
	}
	encodedKey, err := x509.MarshalECPrivateKey(private)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey() = %v", err)
	}

	dir := t.TempDir()
	certificate, key = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}
	if err := os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: encodedKey}), 0o600); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}
	return certificate, key
}

// Cancellation crosses three boundaries the session no longer guards with a
// lock held across the dial: the request arrives on another connection, has to
// find which backend the session holds, and is sent from a goroutine while the
// workers carry on. What must survive that is the obvious thing — the query that
// was running stops, and the session is usable afterwards.
func TestProxyCancelsARunningQuery(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 2}))

	finished := make(chan error, 1)
	go func() {
		_, err := conn.Exec(context.Background(), "SELECT pg_sleep(30)")
		finished <- err
	}()

	// Long enough for the query to be on a backend, which is what a
	// cancellation is translated against.
	time.Sleep(300 * time.Millisecond)
	if err := conn.PgConn().CancelRequest(t.Context()); err != nil {
		t.Fatalf("CancelRequest() = %v", err)
	}

	select {
	case err := <-finished:
		var serverErr *pgconn.PgError
		if !errors.As(err, &serverErr) || serverErr.Code != "57014" {
			t.Fatalf("the query ended with %v, want SQLSTATE 57014 (query_canceled)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the query was still running ten seconds after it was cancelled")
	}

	var answer int
	if err := conn.QueryRow(t.Context(), "SELECT 7").Scan(&answer); err != nil || answer != 7 {
		t.Fatalf("the session after a cancellation: got %d, %v", answer, err)
	}
}
