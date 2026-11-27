// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gsoultan/caduceus/wire"
	"github.com/gsoultan/nextar-engine/pkg/gpool"
	postgrespool "github.com/gsoultan/nextar-engine/pkg/vendors/postgres/pool"
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

// errOtherStatement is an answer the statement asked could not have given: its
// Bind reached a backend where some other statement had been parsed last.
var errOtherStatement = errors.New("the answer is another statement's")

// hammer runs parameterised queries from many goroutines and returns the first
// failure: an error, or an answer that is not the statement's own.
//
// Every goroutine runs one statement they all share and statements of its own,
// alone and in a batch. Repeating one statement across several backends is what
// exposes a client-side statement cache whose names the server does not share.
// A statement of its own for each goroutine is what exposes a statement bound
// on a backend where someone else's was parsed, because only then is the answer
// one that no other statement gives.
func hammer(t *testing.T, pool gpool.Pool, workers, each int) error {
	t.Helper()

	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for worker := range workers {
		wg.Go(func() {
			for i := range each {
				if err := hammerOnce(pool, worker, i); err != nil {
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

// hammerOnce is one round of one goroutine of hammer.
func hammerOnce(pool gpool.Pool, worker, i int) error {
	ctx := context.Background()
	// own returns a statement only this goroutine runs, and its answer for i.
	own := func(n int) (string, int) {
		offset := 1000 * (10*worker + n)
		return fmt.Sprintf("SELECT $1::int + %d", offset), i + offset
	}
	answered := func(sql string, want int, scan func(*int) error) error {
		var got int
		if err := scan(&got); err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("%w: %s with %d gave %d, want %d", errOtherStatement, sql, i, got, want)
		}
		return nil
	}

	const shared = "SELECT $1::int"
	if err := answered(shared, i, func(got *int) error { return pool.QueryRow(ctx, shared, i).Scan(got) }); err != nil {
		return err
	}
	sql, want := own(0)
	if err := answered(sql, want, func(got *int) error { return pool.QueryRow(ctx, sql, i).Scan(got) }); err != nil {
		return err
	}
	if i%5 != 0 {
		return nil
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	batch := &gpool.Batch{}
	for _, n := range []int{1, 2, 1} {
		sql, _ := own(n)
		batch.Queue(sql, i)
	}
	results := conn.(gpool.Batcher).SendBatch(ctx, batch)
	for _, n := range []int{1, 2, 1} {
		sql, want := own(n)
		if err := answered(sql, want, func(got *int) error { return results.QueryRow().Scan(got) }); err != nil {
			return errors.Join(err, results.Close())
		}
	}
	return results.Close()
}

// Disabling the statement cache is the configuration that works against every
// PgBouncer, of any version and any pool_mode. It costs the cache, not correctness.
//
// It used to cost correctness. A statement was parsed and described in one
// Sync and bound in the next, on the unnamed statement, and in transaction mode
// the two could be given different backends: the Bind then ran whatever another
// client had parsed last there, with this client's arguments. This test did not
// see it, because every worker ran the same statement and nobody looked at the
// answers. With a statement of its own for each of 12 clients, 4,393 of 4,800
// answers through PgBouncer 1.22 were another client's. A statement is now
// parsed in the Sync that binds it.
func TestPgBouncerWithStatementCacheDisabled(t *testing.T) {
	for name, descriptions := range map[string]int{
		"remembering descriptions": 0,
		"remembering nothing":      postgrespool.DisableCache,
	} {
		t.Run(name, func(t *testing.T) {
			pool := newPooledPool(t, postgrespool.Config{
				MaxConns:                 8,
				StatementCacheCapacity:   postgrespool.DisableCache,
				DescriptionCacheCapacity: descriptions,
			})

			if err := hammer(t, pool, 40, 25); err != nil {
				t.Fatalf("gpool with the statement cache disabled should work behind any PgBouncer: %v", err)
			}
		})
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

	// 42P05 duplicate_prepared_statement, 26000 invalid_sql_statement_name: a
	// name the backend already has from another client, or does not have at
	// all. A name it has from another client for another statement is not
	// refused, and shows as that statement's answer.
	var refused *wire.Error
	named := errors.As(err, &refused) && (refused.Code == "42P05" || refused.Code == "26000")
	if named || errors.Is(err, errOtherStatement) {
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
