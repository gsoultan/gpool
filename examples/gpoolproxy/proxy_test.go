// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/pooling"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	t.Helper()

	credential, err := deriveVerifier(proxyPassword, defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}
	userlist := filepath.Join(t.TempDir(), "userlist.txt")
	if err := os.WriteFile(userlist, []byte(proxyUser+":"+credential.String()+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}

	proxy, err := NewProxy(Config{
		Listen:   "127.0.0.1:0",
		Upstream: upstreamURL(t, appName),
		Userlist: userlist,
		Pool:     pool,
	})
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

	// Transaction-mode pooling moves a client between backends, so a client-side
	// prepared statement cache would reference names the next backend has never
	// seen. Exec mode still binds parameters server-side; it only stops the names
	// being cached. PgBouncer needs the same of a client below 1.21.
	return fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable&default_query_exec_mode=exec",
		proxyUser, proxyPassword, proxy.Addr())
}

func connect(t *testing.T, url string) *pgx.Conn {
	t.Helper()

	conn, err := pgx.Connect(t.Context(), url)
	if err != nil {
		t.Fatalf("pgx.Connect() = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})
	return conn
}

func TestProxyRunsQueries(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 4}))

	var answer int
	if err := conn.QueryRow(t.Context(), "SELECT $1::int + 1", 41).Scan(&answer); err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}
	if answer != 42 {
		t.Errorf("answer = %d, want 42", answer)
	}
}

// The result set has to survive being relayed without being decoded, including
// rows larger than the relay buffer, which take the streaming path instead.
func TestProxyRelaysLargeResults(t *testing.T) {
	conn := connect(t, startProxy(t, pooling.Config{MaxConns: 2}))

	var total, size int
	err := conn.QueryRow(t.Context(),
		"SELECT count(*), max(length(payload)) FROM (SELECT repeat('x', $1::int) AS payload FROM generate_series(1, 500)) s",
		relayChunk*40).Scan(&total, &size)
	if err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}
	if total != 500 || size != relayChunk*40 {
		t.Errorf("got %d rows of %d bytes, want 500 of %d", total, size, relayChunk*40)
	}
}

// This is the question the proxy exists to answer. Many independent client
// pools — standing in for separate applications — must not add up to more
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

	for range applications {
		config, err := pgxpool.ParseConfig(url)
		if err != nil {
			t.Fatalf("ParseConfig() = %v", err)
		}
		config.MaxConns = perApplication
		config.MinConns = perApplication

		application, err := pgxpool.NewWithConfig(t.Context(), config)
		if err != nil {
			t.Fatalf("pgxpool.NewWithConfig() = %v", err)
		}
		t.Cleanup(application.Close)

		for range perApplication {
			wg.Go(func() {
				for range 20 {
					var seen int64
					if err := application.QueryRow(t.Context(),
						"SELECT count(*)::int FROM pg_stat_activity WHERE application_name = $1", appName,
					).Scan(&seen); err != nil {
						failures.Add(1)
						return
					}
					for observed := peak.Load(); seen > observed; observed = peak.Load() {
						if peak.CompareAndSwap(observed, seen) {
							break
						}
					}
				}
			})
		}
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

	var remaining int
	if err := observer.QueryRow(t.Context(),
		"SELECT count(*)::int FROM pg_stat_activity WHERE application_name = $1", appName).Scan(&remaining); err != nil {
		t.Fatalf("QueryRow() = %v", err)
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
	if _, err := conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s (id int)", table)); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	for id := range 25 {
		if _, err := tx.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s VALUES ($1)", table), id); err != nil {
			t.Fatalf("INSERT = %v", err)
		}
	}

	// Visible inside the transaction...
	var inside int
	if err := tx.QueryRow(t.Context(), fmt.Sprintf("SELECT count(*)::int FROM %s", table)).Scan(&inside); err != nil {
		t.Fatalf("count inside = %v", err)
	}
	if inside != 25 {
		t.Errorf("inside the transaction: %d rows, want 25", inside)
	}

	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() = %v", err)
	}

	// ...and gone after rollback, whichever backend answers next.
	var after int
	if err := conn.QueryRow(t.Context(), fmt.Sprintf("SELECT count(*)::int FROM %s", table)).Scan(&after); err != nil {
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
	if _, err := setup.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = setup.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	abandoner, err := pgx.Connect(t.Context(), url)
	if err != nil {
		t.Fatalf("pgx.Connect() = %v", err)
	}
	tx, err := abandoner.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	if _, err := tx.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s VALUES (1)", table)); err != nil {
		t.Fatalf("INSERT = %v", err)
	}

	// Walk away holding the row lock.
	if err := abandoner.PgConn().Conn().Close(); err != nil {
		t.Fatalf("closing the underlying socket = %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	if _, err := setup.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", table)); err != nil {
		t.Fatalf("the abandoned transaction was not rolled back, so its lock outlived its client: %v", err)
	}
}

func TestProxyRejectsTheWrongPassword(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 2})
	wrong := strings.Replace(url, proxyPassword, "not-the-password", 1)

	if _, err := pgx.Connect(t.Context(), wrong); err == nil {
		t.Fatal("connecting with the wrong password succeeded")
	}
}

func TestProxyRejectsAnUnknownUser(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 2})
	unknown := strings.Replace(url, "//"+proxyUser+":", "//nobody:", 1)

	if _, err := pgx.Connect(t.Context(), unknown); err == nil {
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
