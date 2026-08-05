// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

// gpool replaces PgBouncer, but plenty of deployments cannot remove it: a managed
// service that only exposes a pooled endpoint, or a database with a hard
// connection limit already fronted by one. These tests establish what works when
// gpool is stacked behind PgBouncer rather than instead of it.
//
//	PGBOUNCER_URL='postgres://user:pass@127.0.0.1:6432/db' go test ./integration/
func pgBouncerURL(t *testing.T) string {
	t.Helper()

	value := os.Getenv("PGBOUNCER_URL")
	if value == "" {
		t.Skip("PGBOUNCER_URL not set")
	}
	return value
}

func newPooledPool(t *testing.T, config postgrespool.Config) gpool.Pool {
	t.Helper()

	config.ConnString = pgBouncerURL(t)

	pool, err := gpool.NewPool(gpool.Postgres, config)
	if err != nil {
		t.Fatalf("NewPool() = %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// hammer runs the same parameterised query from many goroutines. Repeating one
// statement across several backends is what exposes a client-side statement cache
// whose names the server does not share.
func hammer(t *testing.T, pool gpool.Pool, workers, each int) error {
	t.Helper()

	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for range workers {
		wg.Go(func() {
			for i := range each {
				var value int
				if err := pool.QueryRow(context.Background(), "SELECT $1::int", i).Scan(&value); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	return <-errs
}

// Disabling the statement cache is the configuration that works against every
// PgBouncer, of any version and any pool_mode. It costs the cache, not correctness.
func TestPgBouncerWithStatementCacheDisabled(t *testing.T) {
	pool := newPooledPool(t, postgrespool.Config{
		MaxConns:               8,
		StatementCacheCapacity: postgrespool.DisableCache,
	})

	if err := hammer(t, pool, 40, 5); err != nil {
		t.Fatalf("gpool with the statement cache disabled should work behind any PgBouncer: %v", err)
	}
}

// With the cache left on, the outcome depends on the proxy's own prepared
// statement support. PgBouncer gained it in 1.21 and enables it by default from
// 1.24 (max_prepared_statements = 200); before that, named statements collide or
// vanish as clients move between backends.
//
// This test does not assert a failure, because the correct outcome depends on the
// server it is pointed at. It reports which regime it observed, so the answer is
// evidence rather than assumption.
func TestPgBouncerWithStatementCacheEnabled(t *testing.T) {
	pool := newPooledPool(t, postgrespool.Config{MaxConns: 8})

	err := hammer(t, pool, 40, 5)
	if err == nil {
		t.Log("statement caching works: the proxy tracks prepared statements " +
			"(PgBouncer >= 1.21 with max_prepared_statements > 0)")
		return
	}

	// 42P05 duplicate_prepared_statement, 26000 invalid_sql_statement_name.
	message := err.Error()
	if strings.Contains(message, "42P05") || strings.Contains(message, "26000") {
		t.Logf("statement caching is unsupported by this proxy, as expected without "+
			"max_prepared_statements: %v", err)
		t.Log("remedy: postgrespool.Config{StatementCacheCapacity: postgrespool.DisableCache}")
		return
	}
	t.Fatalf("unexpected failure through PgBouncer: %v", err)
}

// Transaction-mode pooling hands a backend out per transaction, so a transaction
// opened through gpool must still be atomic end to end.
func TestPgBouncerTransactionsAreAtomic(t *testing.T) {
	pool := newPooledPool(t, postgrespool.Config{
		MaxConns:               4,
		StatementCacheCapacity: postgrespool.DisableCache,
	})

	table := fmt.Sprintf("gpool_pgb_%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := pool.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	if _, err := tx.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table)); err != nil {
		t.Fatalf("Exec() = %v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() = %v", err)
	}

	var count int
	if err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatalf("count = %v", err)
	}
	if count != 0 {
		t.Fatalf("row count = %d after rollback, want 0", count)
	}
}

// gpool's own accounting still describes gpool's connections — which, stacked this
// way, are connections to the proxy rather than to PostgreSQL. Worth knowing
// before wiring Stat to a dashboard labelled "database connections".
func TestPgBouncerStatDescribesTheProxyHop(t *testing.T) {
	pool := newPooledPool(t, postgrespool.Config{
		MaxConns:               4,
		StatementCacheCapacity: postgrespool.DisableCache,
	})

	if err := hammer(t, pool, 8, 2); err != nil {
		t.Fatalf("query through PgBouncer = %v", err)
	}

	stat := pool.Stat()
	if stat.TotalConnections() > 4 {
		t.Errorf("TotalConnections() = %d, want at most MaxConns (4)", stat.TotalConnections())
	}
	if stat.AcquireCount() == 0 {
		t.Error("AcquireCount() = 0, want the acquisitions to be counted")
	}
	t.Logf("gpool holds %d connection(s) to the proxy; PgBouncer's own server-side "+
		"pool is reported by SHOW POOLS, not here", stat.TotalConnections())
}
