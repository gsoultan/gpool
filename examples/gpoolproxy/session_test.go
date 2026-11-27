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

	"github.com/gsoultan/nextar/pkg/pooling"
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

	var wg sync.WaitGroup
	failures := make(chan error, clients)
	for client := range clients {
		wg.Go(func() {
			conn, err := dialClient(context.Background(), url)
			if err != nil {
				failures <- fmt.Errorf("client %d: connect: %w", client, err)
				return
			}
			defer conn.close()

			for round := range rounds {
				first, second := client*rounds*2+round*2, client*rounds*2+round*2+1
				// Two batches, each in a write of its own, and neither answer
				// read until both are sent.
				for _, value := range []int{first, second} {
					batch := append(frontendBatch("", "SELECT $1::int4", value), frontendSync())
					if err := conn.send(batch...); err != nil {
						failures <- fmt.Errorf("client %d, round %d: send: %w", client, round, err)
						return
					}
				}
				for _, want := range []int{first, second} {
					got, err := pipelinedAnswer(conn)
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

// pipelinedAnswer reads one single-value result up to the boundary after it.
func pipelinedAnswer(conn *wireClient) (string, error) {
	rows, err := conn.untilReady()
	if err != nil {
		return "", err
	}
	return single(rows)
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

	raw := connect(t, url)
	raw.timeout = 30 * time.Second

	// The whole first batch, and the second up to but not including its Sync.
	opening := append(frontendBatch("", "SELECT 'first'"), frontendSync())
	opening = append(opening, frontendBatch("", "SELECT 'second'")...)
	if err := raw.send(opening...); err != nil {
		t.Fatalf("send = %v", err)
	}
	if rows := readUntilReady(t, raw); !rowsEqual(rows, "first") {
		t.Fatalf("first batch returned %q, want [first]", rows)
	}

	// Another client, while the second batch is still open. With one backend it
	// can only be served by taking the one the first client is part-way through.
	intruder := make(chan error, 1)
	go func() {
		other, err := dialClient(context.Background(), url)
		if err != nil {
			intruder <- err
			return
		}
		defer other.close()

		got, err := other.value("SELECT 'intruder'")
		if err != nil {
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

	if err := raw.send(frontendSync()); err != nil {
		t.Fatalf("send = %v", err)
	}
	if rows := readUntilReady(t, raw); !rowsEqual(rows, "second") {
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
func readUntilReady(t *testing.T, conn *wireClient) []string {
	t.Helper()

	rows, err := conn.untilReady()
	if err != nil {
		t.Fatalf("untilReady() = %v", err)
	}
	first := make([]string, 0, len(rows))
	for _, row := range rows {
		first = append(first, row[0])
	}
	return first
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

	result, err := conn.query(
		"SELECT n, repeat(chr(65 + n % 26), $1::int) FROM generate_series(1, $2::int) AS n", size, rows)
	if err != nil {
		t.Fatalf("query = %v", err)
	}

	seen := 0
	for _, row := range result {
		n, err := strconv.Atoi(row[0])
		if err != nil {
			t.Fatalf("row number = %v", err)
		}
		payload := row[1]
		seen++
		if n != seen {
			t.Fatalf("row %d arrived as row %d", n, seen)
		}
		want := strings.Repeat(string(rune('A'+n%26)), size)
		if payload != want {
			t.Fatalf("row %d: %d bytes, starting %.1q; want %d of %q", n, len(payload), payload, size, want[:1])
		}
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
	for name, named := range map[string]bool{"unnamed": false, "named": true} {
		t.Run(name, func(t *testing.T) {
			conn := connect(t, url)
			if named {
				conn.caching()
			}

			for range 2 {
				rows, err := conn.query("SELECT length($1::bytea), md5($1::bytea)", payload)
				if err != nil || len(rows) != 1 {
					t.Fatalf("query = %d rows, %v", len(rows), err)
				}
				length, digest := rows[0][0], rows[0][1]
				if length != strconv.Itoa(len(payload)) {
					t.Fatalf("server received %s bytes, want %d", length, len(payload))
				}

				want, err := conn.value("SELECT md5(repeat('0123456789abcdef', $1::int)::bytea)", readBufSize/4)
				if err != nil {
					t.Fatalf("query = %v", err)
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
	payload := strings.Repeat("x", 200)

	if _, err := conn.query("BEGIN"); err != nil {
		t.Fatalf("BEGIN = %v", err)
	}
	defer conn.query("ROLLBACK")

	if _, err := conn.query("CREATE TEMP TABLE gpoolproxy_copy (id int, payload text) ON COMMIT DROP"); err != nil {
		t.Fatalf("CREATE TEMP TABLE = %v", err)
	}

	sent := 0
	err := conn.copyIn("COPY gpoolproxy_copy (id, payload) FROM STDIN", func() (string, bool) {
		if sent == rows {
			return "", false
		}
		sent++
		return copyLine(strconv.Itoa(sent), payload), true
	})
	if err != nil {
		t.Fatalf("copyIn() = %v", err)
	}

	result, err := conn.query("SELECT count(*), sum(length(payload)) FROM gpoolproxy_copy")
	if err != nil || len(result) != 1 {
		t.Fatalf("query = %d rows, %v", len(result), err)
	}
	if count, total := result[0][0], result[0][1]; count != strconv.Itoa(rows) || total != strconv.Itoa(rows*200) {
		t.Fatalf("table holds %s rows and %s bytes, want %d and %d", count, total, rows, rows*200)
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

	if !conn.login.encrypted {
		t.Fatal("the client connection is not TLS, so this did not test the bridge")
	}

	// Small, then large enough to need several records each way.
	for _, size := range []int{1, readBufSize * 2} {
		got, err := conn.value("SELECT repeat('t', $1::int)", size)
		if err != nil {
			t.Fatalf("query(%d) = %v", size, err)
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
		_, err := conn.query("SELECT pg_sleep(30)")
		finished <- err
	}()

	// Long enough for the query to be on a backend, which is what a
	// cancellation is translated against.
	time.Sleep(300 * time.Millisecond)
	if err := conn.cancel(t.Context()); err != nil {
		t.Fatalf("cancel() = %v", err)
	}

	select {
	case err := <-finished:
		if sqlState(err) != "57014" {
			t.Fatalf("the query ended with %v, want SQLSTATE 57014 (query_canceled)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the query was still running ten seconds after it was cancelled")
	}

	if answer, err := conn.number("SELECT 7"); err != nil || answer != 7 {
		t.Fatalf("the session after a cancellation: got %d, %v", answer, err)
	}
}
