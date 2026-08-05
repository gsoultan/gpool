// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Package integration exercises gpool against a real PostgreSQL server through the
// public API, the way a consumer would use it.
//
// Every test skips unless DATABASE_URL is set. The CDC tests additionally need
// wal_level=logical and a role with the REPLICATION attribute.
//
//	DATABASE_URL='postgres://postgres:postgres@localhost:5432/postgres' go test ./integration/...
package integration

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

func connString(t *testing.T) string {
	t.Helper()

	conn := os.Getenv("DATABASE_URL")
	if conn == "" {
		t.Skip("DATABASE_URL not set")
	}
	return conn
}

func newPool(t *testing.T, config postgrespool.Config) gpool.Pool {
	t.Helper()

	config.ConnString = connString(t)

	pool, err := gpool.NewPool(gpool.Postgres, config)
	if err != nil {
		t.Fatalf("NewPool() = %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// The vendor package registers itself from init(), so a consumer that imports it
// can resolve the vendor by name. This is the whole contract of the registry.
func TestVendorSelfRegisters(t *testing.T) {
	connString(t)

	if _, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{ConnString: connString(t)}); err != nil {
		t.Fatalf("NewPool() = %v", err)
	}
}

func TestPoolQueryRoundTrip(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 4})

	var value int
	if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("QueryRow().Scan() = %v", err)
	}
	if value != 1 {
		t.Fatalf("got %d, want 1", value)
	}
}

func TestPoolIteratorReleasesTheConnection(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})

	// With one permit, a leaked connection makes the second query hang, so this
	// also proves the iterator returns the connection when it finishes.
	for range 3 {
		rows, err := pool.Query(t.Context(), "SELECT generate_series(1, 3)")
		if err != nil {
			t.Fatalf("Query() = %v", err)
		}

		seen := 0
		for row := range rows.All() {
			var n int
			if err := row.Scan(&n); err != nil {
				t.Fatalf("Scan() = %v", err)
			}
			seen++
		}
		if seen != 3 {
			t.Fatalf("iterated %d rows, want 3", seen)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Err() = %v", err)
		}
	}
}

// The idiom that used to panic: a deferred Close alongside a range over All.
func TestPoolDeferredCloseAlongsideIterator(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})

	func() {
		rows, err := pool.Query(t.Context(), "SELECT generate_series(1, 2)")
		if err != nil {
			t.Fatalf("Query() = %v", err)
		}
		defer rows.Close()

		for range rows.All() {
		}
	}()

	// The permit came back exactly once, so this query can proceed.
	var value int
	if err := pool.QueryRow(t.Context(), "SELECT 42").Scan(&value); err != nil {
		t.Fatalf("QueryRow() after a doubled close = %v", err)
	}
}

// Releasing a row without scanning it must still return the connection, or a pool
// with one permit deadlocks on the next acquisition.
func TestPoolQueryRowReleaseWithoutScan(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})

	pool.QueryRow(t.Context(), "SELECT 1").Release()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var value int
	if err := pool.QueryRow(ctx, "SELECT 2").Scan(&value); err != nil {
		t.Fatalf("the connection was not returned by Release: %v", err)
	}
	if value != 2 {
		t.Fatalf("got %d, want 2", value)
	}
}

func TestPoolRespectsMaxConns(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2})

	first, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer first.Release()

	second, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer second.Release()

	// The third acquisition must wait rather than open a third connection.
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	if _, err := pool.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire() beyond MaxConns = %v, want a deadline", err)
	}

	if got := pool.Stat().TotalConnections(); got > 2 {
		t.Fatalf("TotalConnections() = %d, want at most MaxConns (2)", got)
	}
}

// The pool is the shared object in a server; this is the load it actually sees.
func TestPoolUnderConcurrentLoad(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 8, MinConns: 2})

	var wg sync.WaitGroup
	errs := make(chan error, 64)

	for range 64 {
		wg.Go(func() {
			var value int
			if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent query = %v", err)
	}

	stat := pool.Stat()
	if stat.TotalConnections() > 8 {
		t.Errorf("TotalConnections() = %d, want at most 8", stat.TotalConnections())
	}
	if stat.ActiveConnections() != 0 {
		t.Errorf("ActiveConnections() = %d, want 0 once every caller has finished", stat.ActiveConnections())
	}
}

func TestPoolResetQueryKeepsConnectionsUsable(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2, ResetQuery: "DISCARD ALL"})

	for range 5 {
		conn, err := pool.Acquire(t.Context())
		if err != nil {
			t.Fatalf("Acquire() = %v", err)
		}
		if _, err := conn.Exec(t.Context(), "SET application_name = 'gpool_reset_probe'"); err != nil {
			conn.Release()
			t.Fatalf("Exec() = %v", err)
		}
		conn.Release()
	}

	// DISCARD ALL runs on release, so no caller inherits the previous one's session state.
	var name string
	if err := pool.QueryRow(t.Context(), "SHOW application_name").Scan(&name); err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}
	if name == "gpool_reset_probe" {
		t.Fatal("session state leaked across a release despite ResetQuery")
	}

	// The same parameterised query, repeatedly, across releases. DISCARD ALL
	// deallocates the server's prepared statements, so a driver still caching them
	// client-side fails here with SQLSTATE 26000 on the second iteration.
	for i := range 10 {
		var value int
		if err := pool.QueryRow(t.Context(), "SELECT $1::int", i).Scan(&value); err != nil {
			t.Fatalf("iteration %d: prepared statement cache went stale across DISCARD ALL: %v", i, err)
		}
		if value != i {
			t.Fatalf("iteration %d: got %d", i, value)
		}
	}
}

// The canonical transaction idiom, against a real driver.
func TestTransactionCommitWithDeferredRollback(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()

	var value int
	if err := tx.QueryRow(t.Context(), "SELECT 7").Scan(&value); err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() = %v", err)
	}

	// The connection survives the deferred rollback and stays usable.
	if err := conn.Ping(t.Context()); err != nil {
		t.Fatalf("Ping() after commit and deferred rollback = %v", err)
	}
}

// The transaction-mode contract: a caller that returns a connection without
// settling its transaction must not leak it onward. Without this gate the next
// caller inherits the open transaction, its locks, and its snapshot.
func TestPoolUnwindsTransactionLeftOpenOnRelease(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})
	ctx := t.Context()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	if _, err := conn.Begin(ctx); err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("Exec() inside the transaction = %v", err)
	}
	// Released without commit or rollback - the mistake this gate exists for.
	conn.Release()

	// MaxConns is 1, so the next caller gets that same connection back.
	next, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire() after a leaked transaction = %v", err)
	}
	defer next.Release()

	// VACUUM is rejected inside a transaction block, so it answers the question
	// directly. A plain SELECT would succeed either way - it would simply run
	// inside the previous caller's transaction without saying so.
	if _, err := next.Exec(ctx, "VACUUM"); err != nil {
		t.Fatalf("the next caller inherited an open transaction: %v", err)
	}
}

// A failed transaction is worse than an open one: every statement is rejected
// until it is unwound.
func TestPoolUnwindsFailedTransactionOnRelease(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})
	ctx := t.Context()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	if _, err := conn.Begin(ctx); err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	// Poison the transaction, then abandon it.
	if _, err := conn.Exec(ctx, "SELECT * FROM a_table_that_does_not_exist"); err == nil {
		t.Fatal("expected the bad statement to fail")
	}
	conn.Release()

	next, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire() after a failed transaction = %v", err)
	}
	defer next.Release()

	var value int
	if err := next.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("the next caller inherited a failed transaction: %v", err)
	}
	if value != 1 {
		t.Fatalf("got %d, want 1", value)
	}
}

// Repeatedly abandoning transactions must not drain the pool: unwinding costs one
// round trip, where destroying the connection would cost a full reconnect each time.
func TestPoolReusesConnectionAfterUnwinding(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})
	ctx := t.Context()

	for i := range 5 {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("iteration %d: Acquire() = %v", i, err)
		}
		if _, err := conn.Begin(ctx); err != nil {
			t.Fatalf("iteration %d: Begin() = %v", i, err)
		}
		conn.Release()
	}

	// One connection, established once and unwound five times.
	if got := pool.Stat().TotalConnections(); got != 1 {
		t.Fatalf("TotalConnections() = %d, want 1 - the connection was replaced rather than unwound", got)
	}
}

// MaxConns must bound the connections that exist on the server, not merely the
// callers holding one. Those are different guarantees: holding a permit before
// dialling gives only the second, because a permit released by one caller orders
// nothing with respect to another caller's freshly pooled connection. A caller
// can hold a permit, fail to see an idle connection that already exists, and dial
// a surplus one — which is how a pool with MaxConns 4 ends up with five open.
//
// This is an end-to-end guard, not the reproduction. A real dial is slow enough
// that the losing interleaving is rare here; it passed both with and without the
// fix. The reliable reproduction is TestCoreHoldsCapacityUnderContention in
// pkg/pooling, where the fake driver returns instantly and widens the window.
func TestPoolNeverExceedsMaxConnsUnderContention(t *testing.T) {
	const capacity = 4
	pool := newPool(t, postgrespool.Config{MaxConns: capacity})

	var peak atomic.Int32
	var wg sync.WaitGroup

	for range 128 {
		wg.Go(func() {
			for range 40 {
				conn, err := pool.Acquire(context.Background())
				if err != nil {
					return
				}

				// Sample while connections are actually checked out, so a transient
				// overshoot is caught rather than only the settled state.
				for {
					current := pool.Stat().TotalConnections()
					high := peak.Load()
					if current <= high || peak.CompareAndSwap(high, current) {
						break
					}
				}
				conn.Release()
			}
		})
	}
	wg.Wait()

	if got := peak.Load(); got > capacity {
		t.Fatalf("peak TotalConnections() = %d, want at most MaxConns (%d)", got, capacity)
	}
	if got := pool.Stat().TotalConnections(); got > capacity {
		t.Fatalf("TotalConnections() = %d, want at most MaxConns (%d)", got, capacity)
	}
}

func TestPoolCloseIsSafeUnderLoad(t *testing.T) {
	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: connString(t),
		MaxConns:   4,
	})
	if err != nil {
		t.Fatalf("NewPool() = %v", err)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			var value int
			// Some of these race Close and must return an error, never panic.
			_ = pool.QueryRow(context.Background(), "SELECT 1").Scan(&value)
		})
	}

	pool.Close()
	wg.Wait()

	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("Acquire() on a closed pool = nil, want an error")
	}
	// Close is idempotent.
	pool.Close()
}

func TestPoolReapsExpiredConnections(t *testing.T) {
	pool := newPool(t, postgrespool.Config{
		MaxConns:          2,
		MaxConnLifetime:   50 * time.Millisecond,
		HealthCheckPeriod: 25 * time.Millisecond,
	})

	var value int
	if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pool.Stat().TotalConnections() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expired connections were never reaped: %d still open", pool.Stat().TotalConnections())
}
