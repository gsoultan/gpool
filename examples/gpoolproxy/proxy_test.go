// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gsoultan/nextar-engine/pkg/pooling"
)

// These tests need a real PostgreSQL. The proxy is protocol code: a unit test
// can prove the SCRAM arithmetic, but nothing short of a live server proves it
// speaks the wire protocol a client will actually accept.
//
//	DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres' go test ./...
const (
	proxyPassword = "proxy-test-password"
	proxyUser     = "app"
	// The proxy's backends are counted by application_name, so the connection
	// doing the counting must not carry the same one or it counts itself.
	appName      = "gpoolproxy-test"
	observerName = "gpoolproxy-observer"
)

func upstreamURL(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv("DATABASE_URL")
	if value == "" {
		t.Skip("DATABASE_URL not set")
	}
	separator := "?"
	if strings.Contains(value, "?") {
		separator = "&"
	}
	return value + separator + "application_name=" + name
}

// startProxy runs a proxy on an ephemeral port and returns a connection string
// for it.
func startProxy(t *testing.T, pool pooling.Config) string {
	return startProxyLimited(t, pool, 0)
}

// startProxyLimited is startProxy with an explicit ceiling on remembered prepared
// statements, so a test can reach the limit without preparing five hundred.
func startProxyLimited(t *testing.T, pool pooling.Config, maxPrepared int) string {
	return startProxyWith(t, func(config *Config) {
		config.Pool = pool
		config.MaxPreparedStatements = maxPrepared
	})
}

// startProxyWith is startProxy for a test that needs to set something else.
func startProxyWith(t *testing.T, configure func(*Config)) string {
	t.Helper()

	credential, err := deriveVerifier(proxyPassword, defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}
	userlist := filepath.Join(t.TempDir(), "userlist.txt")
	if err := os.WriteFile(userlist, []byte(proxyUser+":"+credential.String()+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}

	config := Config{
		Listen:   "127.0.0.1:0",
		Upstream: upstreamURL(t, appName),
		Userlist: userlist,
	}
	configure(&config)

	proxy, err := NewProxy(config)
	if err != nil {
		t.Fatalf("NewProxy() = %v", err)
	}
	if err := proxy.Listen(); err != nil {
		t.Fatalf("Listen() = %v", err)
	}

	served := make(chan error, 1)
	go func() { served <- proxy.Serve() }()
	t.Cleanup(func() {
		proxy.Close()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})

	return fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", proxyUser, proxyPassword, proxy.Addr())
}

func TestProxyRunsQueries(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 4}))

	answer, err := conn.number("SELECT $1::int + 1", 41)
	if err != nil {
		t.Fatalf("query = %v", err)
	}
	if answer != 42 {
		t.Errorf("answer = %d, want 42", answer)
	}
}

// The result set has to survive being relayed without being decoded, including
// rows larger than the read buffer, which cross in pieces instead.
func TestProxyRelaysLargeResults(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 2}))

	rows, err := conn.query(
		"SELECT repeat('x', $1::int) AS payload FROM generate_series(1, 500)", readBufSize*3)
	if err != nil {
		t.Fatalf("query = %v", err)
	}
	if len(rows) != 500 {
		t.Fatalf("got %d rows, want 500", len(rows))
	}
	for _, row := range rows {
		if len(row[0]) != readBufSize*3 {
			t.Fatalf("got a row of %d bytes, want %d", len(row[0]), readBufSize*3)
		}
	}
}

// This is the question the proxy exists to answer. Many independent clients — standing in for separate applications — must not add up to more
// PostgreSQL backends than the proxy was told to open. An in-process library
// pool cannot enforce this, because it cannot see the other applications.
func TestProxyBoundsBackendsAcrossIndependentClients(t *testing.T) {
	const (
		applications   = 12
		perApplication = 5
		backends       = 4
	)

	url := startProxy(t, pooling.Config{MaxConns: backends})

	// A direct connection, so the observation itself does not go through the
	// pool being measured.
	observer := connect(t, upstreamURL(t, observerName))

	var wg sync.WaitGroup
	var peak, failures atomic.Int64

	for range applications * perApplication {
		// Every connection is open before any of them runs a query, so the
		// proxy is holding all of them at once for the whole measurement.
		conn := connect(t, url)

		wg.Go(func() {
			for range 20 {
				count, err := conn.number(
					"SELECT count(*)::int FROM pg_stat_activity WHERE application_name = $1", appName)
				if err != nil {
					failures.Add(1)
					return
				}
				seen := int64(count)
				for observed := peak.Load(); seen > observed; observed = peak.Load() {
					if peak.CompareAndSwap(observed, seen) {
						break
					}
				}
			}
		})
	}
	wg.Wait()

	if failures.Load() > 0 {
		t.Fatalf("%d queries through the proxy failed", failures.Load())
	}
	highest := peak.Load()

	if highest > backends {
		t.Errorf("PostgreSQL saw %d backends from the proxy, want at most MaxConns (%d)", highest, backends)
	}
	t.Logf("%d client connections across %d applications ran on at most %d PostgreSQL backends",
		applications*perApplication, applications, highest)

	remaining, err := observer.number(
		"SELECT count(*)::int FROM pg_stat_activity WHERE application_name = $1", appName)
	if err != nil {
		t.Fatalf("query = %v", err)
	}
	if remaining > backends {
		t.Errorf("%d backends left behind, want at most %d", remaining, backends)
	}
}

// Transaction-mode pooling hands out a backend per transaction, so everything
// inside BEGIN..COMMIT has to land on the same one. A statement that escaped to
// another backend would commit outside its transaction.
func TestProxyKeepsATransactionOnOneBackend(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 4})
	conn := connect(t, url)

	table := fmt.Sprintf("gpoolproxy_tx_%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := conn.query(fmt.Sprintf("CREATE TABLE %s (id int)", table)); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.query(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	if _, err := conn.query("BEGIN"); err != nil {
		t.Fatalf("BEGIN = %v", err)
	}
	for id := range 25 {
		if _, err := conn.query(fmt.Sprintf("INSERT INTO %s VALUES ($1)", table), id); err != nil {
			t.Fatalf("INSERT = %v", err)
		}
	}

	// Visible inside the transaction...
	inside, err := conn.number(fmt.Sprintf("SELECT count(*)::int FROM %s", table))
	if err != nil {
		t.Fatalf("count inside = %v", err)
	}
	if inside != 25 {
		t.Errorf("inside the transaction: %d rows, want 25", inside)
	}

	if _, err := conn.query("ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK = %v", err)
	}

	// ...and gone after rollback, whichever backend answers next.
	after, err := conn.number(fmt.Sprintf("SELECT count(*)::int FROM %s", table))
	if err != nil {
		t.Fatalf("count after = %v", err)
	}
	if after != 0 {
		t.Errorf("after rollback: %d rows, want 0", after)
	}
}

// A client that vanishes mid-transaction must not hand the next client its
// locks. With MaxConns 1 the next client can only be served by the very same
// backend, so if the transaction were not rolled back this blocks until the
// test times out.
func TestProxyRollsBackAnAbandonedTransaction(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 1})

	table := fmt.Sprintf("gpoolproxy_abandoned_%d", time.Now().UnixNano()%1_000_000_000)
	setup := connect(t, url)
	if _, err := setup.query(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}
	t.Cleanup(func() {
		_, _ = setup.query(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	abandoner, err := dialClient(t.Context(), url)
	if err != nil {
		t.Fatalf("dialClient() = %v", err)
	}
	if _, err := abandoner.query("BEGIN"); err != nil {
		t.Fatalf("BEGIN = %v", err)
	}
	if _, err := abandoner.query(fmt.Sprintf("INSERT INTO %s VALUES (1)", table)); err != nil {
		t.Fatalf("INSERT = %v", err)
	}

	// Walk away holding the row lock, without so much as a Terminate.
	if err := abandoner.conn.Close(); err != nil {
		t.Fatalf("closing the socket = %v", err)
	}

	setup.timeout = 20 * time.Second
	if _, err := setup.query(fmt.Sprintf("INSERT INTO %s VALUES (1)", table)); err != nil {
		t.Fatalf("the abandoned transaction was not rolled back, so its lock outlived its client: %v", err)
	}
}

func TestProxyRejectsTheWrongPassword(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 2})
	wrong := strings.Replace(url, proxyPassword, "not-the-password", 1)

	conn, err := dialClient(t.Context(), wrong)
	if err == nil {
		conn.close()
		t.Fatal("connecting with the wrong password succeeded")
	}
	if code := sqlState(err); code != "28P01" {
		t.Errorf("SQLSTATE = %q (%v), want 28P01", code, err)
	}
}

func TestProxyRejectsAnUnknownUser(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 2})
	unknown := strings.Replace(url, "//"+proxyUser+":", "//nobody:", 1)

	conn, err := dialClient(t.Context(), unknown)
	if err == nil {
		conn.close()
		t.Fatal("connecting as an unknown user succeeded")
	}
}

// Refusing to load a credentials file the whole host can read is the difference
// between a proxy that is secure and one that merely looks it.
func TestUserlistRefusesWorldReadableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "userlist.txt")
	if err := os.WriteFile(path, []byte("app:secret\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}

	_, err := loadUserlist(path)
	if err == nil {
		t.Fatal("loaded a world-readable userlist")
	}
	if !strings.Contains(err.Error(), "chmod 600") {
		t.Errorf("error should say how to fix it, got: %v", err)
	}
}

func TestUserlistAcceptsBothSecretForms(t *testing.T) {
	credential, err := deriveVerifier("stored", defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}

	path := filepath.Join(t.TempDir(), "userlist.txt")
	content := "# a comment\n\nplain:hunter2\nhashed:" + credential.String() + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}

	users, err := loadUserlist(path)
	if err != nil {
		t.Fatalf("loadUserlist() = %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("loaded %d users, want 2", len(users))
	}
	if _, ok := users.lookup("plain"); !ok {
		t.Error("plaintext password was not converted to a verifier")
	}
	if got, _ := users.lookup("hashed"); got.String() != credential.String() {
		t.Error("stored verifier was not preserved")
	}
}

// Named prepared statements have to survive a client moving between backends,
// which is what transaction pooling does on every transaction. Without the proxy
// replaying the Parse, the second backend has never heard of the statement and
// the client fails — the limitation PgBouncer carried until 1.21, and the one
// this example's README used to name as its gap.
//
// A caching client is the test: it prepares each query once under a generated
// name, as client libraries do by default, so every query after the first is a
// Bind against a name that only one backend knows.
func TestProxyKeepsPreparedStatementsAcrossBackends(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 2})

	// More clients than backends, so each is repeatedly handed a connection some
	// other client last prepared on.
	var wg sync.WaitGroup
	var failures atomic.Int64

	for range 8 {
		wg.Go(func() {
			conn, err := dialClient(context.Background(), url)
			if err != nil {
				failures.Add(1)
				return
			}
			defer conn.close()
			conn.caching()

			for i := range 40 {
				answer, err := conn.number("SELECT $1::int + 1", i)
				if err != nil {
					t.Errorf("query %d: %v", i, err)
					failures.Add(1)
					return
				}
				if answer != i+1 {
					t.Errorf("query %d returned %d", i, answer)
					failures.Add(1)
					return
				}
			}
		})
	}
	wg.Wait()

	if failures.Load() > 0 {
		t.Fatalf("%d clients failed with the statement cache enabled", failures.Load())
	}
}

// Two clients may use the same statement name for different SQL. Replaying
// blindly would leave whichever got there first in place and silently run their
// query for the other — wrong results rather than an error, which is the worst
// outcome available. The proxy has to notice and re-parse.
func TestProxyIsolatesIdenticallyNamedStatements(t *testing.T) {
	// One backend, so both clients are guaranteed to share it.
	url := startProxy(t, pooling.Config{MaxConns: 1})

	first := connect(t, url)
	second := connect(t, url)

	// The same name, deliberately, for different SQL.
	if err := first.prepare("shared", "SELECT 111"); err != nil {
		t.Fatalf("first prepare = %v", err)
	}
	if err := second.prepare("shared", "SELECT 222"); err != nil {
		t.Fatalf("second prepare = %v", err)
	}

	// Interleaved, so each turn lands on a backend the other one last used.
	for range 10 {
		rows, err := first.execute("shared")
		if err != nil {
			t.Fatalf("first query = %v", err)
		}
		if got, _ := single(rows); got != "111" {
			t.Fatalf("first client got %q, want 111 — it ran the other client's statement", got)
		}

		rows, err = second.execute("shared")
		if err != nil {
			t.Fatalf("second query = %v", err)
		}
		if got, _ := single(rows); got != "222" {
			t.Fatalf("second client got %q, want 222 — it ran the other client's statement", got)
		}
	}
}

// Deallocating a statement has to reach the backend and be forgotten on both
// sides, or a later client inherits a name the proxy believes is prepared.
func TestProxyForgetsClosedStatements(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 1}))

	answer := func() string {
		t.Helper()
		rows, err := conn.execute("temporary")
		if err != nil {
			t.Fatalf("prepared query = %v", err)
		}
		got, _ := single(rows)
		return got
	}

	if err := conn.prepare("temporary", "SELECT 7"); err != nil {
		t.Fatalf("prepare = %v", err)
	}
	if got := answer(); got != "7" {
		t.Fatalf("prepared query = %q, want 7", got)
	}

	if err := conn.deallocate("temporary"); err != nil {
		t.Fatalf("deallocate = %v", err)
	}

	// Re-preparing the same name with different SQL must take effect rather than
	// resurrect what was deallocated.
	if err := conn.prepare("temporary", "SELECT 8"); err != nil {
		t.Fatalf("second prepare = %v", err)
	}
	if got := answer(); got != "8" {
		t.Fatalf("after re-preparing = %q, want 8; the old statement survived", got)
	}
}

// The bound has to reach the server, not just the proxy's own bookkeeping. A
// prepared statement occupies memory in the backend that parsed it until
// something deallocates it, and a pooled connection outlives every client that
// ever used it — so "something" can only be the proxy.
//
// pg_prepared_statements is session-local, which with a single backend makes it
// exactly the right question: asked through the proxy, it is that backend
// reporting what it is really holding.
func TestProxyBoundsPreparedStatementsOnTheServer(t *testing.T) {
	const limit = 8
	conn := connect(t, startProxyLimited(t, pooling.Config{MaxConns: 1}, limit)).caching()

	// Distinct SQL every time, so the client names and prepares a new statement
	// for each rather than binding one it has already cached.
	for i := range 60 {
		sql := fmt.Sprintf("SELECT %d::int + $1::int", i)
		if _, err := conn.number(sql, 1); err != nil {
			t.Fatalf("query %d = %v", i, err)
		}
	}

	// Asked over the simple protocol, which prepares nothing, so counting does
	// not itself add to the count.
	rows, err := conn.simple("SELECT count(*)::int FROM pg_prepared_statements")
	if err != nil {
		t.Fatalf("counting prepared statements = %v", err)
	}
	text, _ := single(rows)
	held, err := strconv.Atoi(text)
	if err != nil {
		t.Fatalf("counting prepared statements = %v", err)
	}

	t.Logf("the backend holds %d prepared statements after 60 were prepared", held)
	if held > limit {
		t.Errorf("the backend holds %d prepared statements, want at most the limit (%d)", held, limit)
	}
	if held == 0 {
		t.Error("the backend holds none at all, so this proved nothing about eviction")
	}
}

// Eviction has to be invisible to a client whose own working set fits under the
// limit, however much pressure everybody else is putting on the backend it lands
// on. Each eviction injects a Close, and the next use of that statement injects a
// replayed Parse, both in the middle of somebody's traffic — which is where a
// protocol mistake would surface as a client reading somebody else's reply.
//
// Six clients hold three statements each against a limit of eight, so the shared
// backends evict continuously while no client ever exceeds its own share.
func TestProxyEvictionIsTransparentWithinTheLimit(t *testing.T) {
	url := startProxyLimited(t, pooling.Config{MaxConns: 2}, 8)

	var wg sync.WaitGroup
	var failures atomic.Int64

	for client := range 6 {
		wg.Go(func() {
			conn, err := dialClient(context.Background(), url)
			if err != nil {
				t.Errorf("client %d connect = %v", client, err)
				failures.Add(1)
				return
			}
			defer conn.close()
			conn.caching()

			for i := range 40 {
				// The constant identifies the statement, and the argument the
				// call, so a reply from the wrong prepared statement is a wrong
				// answer rather than a passing test.
				constant := client*3 + i%3
				sql := fmt.Sprintf("SELECT %d::int + $1::int", constant)

				answer, err := conn.number(sql, i)
				if err != nil {
					t.Errorf("client %d query %d = %v", client, i, err)
					failures.Add(1)
					return
				}
				if answer != constant+i {
					t.Errorf("client %d query %d = %d, want %d", client, i, answer, constant+i)
					failures.Add(1)
					return
				}
			}
		})
	}
	wg.Wait()

	if got := failures.Load(); got > 0 {
		t.Fatalf("%d clients failed while the backends were evicting", got)
	}
}

// A client is told the server's settings during startup, and they are captured
// from a real backend rather than invented — so until the pool has opened one
// there is nothing to tell. The first client to connect used to be handed an
// empty set: one widely used client library survives that on the extended
// protocol and refuses the simple protocol without standard_conforming_strings,
// and another is entitled to do worse.
//
// This connects before anything has run a query, which is the only moment the
// defect was reachable.
func TestProxyReportsServerParametersToItsFirstClient(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 2}))

	for _, name := range []string{"standard_conforming_strings", "server_version", "client_encoding"} {
		if value := conn.login.statuses[name]; value == "" {
			t.Errorf("the %s setting is empty; the client was told nothing about the server", name)
		}
	}

	rows, err := conn.simple("SELECT 1")
	if err != nil {
		t.Fatalf("simple protocol query = %v", err)
	}
	if answer, _ := single(rows); answer != "1" {
		t.Errorf("answer = %q, want 1", answer)
	}
}

// A client asking for protocol 3.2, as one does for PostgreSQL 18, is offered
// 3.0 the way a server older than 18 offers it, with the protocol options it
// sent named as not recognised, and then logs in. The proxy used to refuse the
// packet outright, which cost such a client a second dial.
func TestProxyOffers30ToAClientAsking32(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 1})
	target, err := parseUpstream(url)
	if err != nil {
		t.Fatal(err)
	}
	session, err := target.connectAsking(t.Context(), protocolVersion32, [2]string{"_pq_.something", "on"})
	if err != nil {
		t.Fatalf("logging in asking for 3.2 = %v", err)
	}
	defer session.conn.Close()
	want := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	want = append(want, "_pq_.something\x00"...)
	if !bytes.Equal(session.offered, want) {
		t.Fatalf("NegotiateProtocolVersion = %q, want 3.0 and the option named", session.offered)
	}
	if len(session.secret) != 4 {
		t.Fatalf("a %d-byte cancel key in protocol 3.0", len(session.secret))
	}
}
