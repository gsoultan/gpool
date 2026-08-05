// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
)

func newTestPool(t *testing.T, connector *fakeConnector, config Config) *Pool {
	t.Helper()

	config.Connector = connector
	if config.MaxConns == 0 {
		config.MaxConns = 2
	}

	pool, err := New(config)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()

	if _, err := New(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("New() without a connector = %v, want ErrInvalidConfig", err)
	}
	if _, err := New(Config{Connector: &fakeConnector{}, MaxConns: 2, MinConns: 5}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("New() with MinConns > MaxConns = %v, want ErrInvalidConfig", err)
	}
}

func TestPoolExec(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, &fakeConnector{}, Config{})

	result, err := pool.Exec(t.Context(), "UPDATE t SET x = ?", 1)
	if err != nil {
		t.Fatalf("Exec() = %v", err)
	}
	if got := result.RowsAffected(); got != 1 {
		t.Errorf("RowsAffected() = %d, want 1", got)
	}

	// LastInsertID is exposed because these engines have one, unlike PostgreSQL.
	id, ok := result.(pgResult).LastInsertID()
	if !ok || id != 7 {
		t.Errorf("LastInsertID() = (%d, %v), want (7, true)", id, ok)
	}

	if got := pool.Stat().ActiveConnections(); got != 0 {
		t.Errorf("ActiveConnections() = %d after Exec, want 0", got)
	}
}

func TestPoolQueryIterates(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{
		columns: []string{"id", "name"},
		rows: [][]driver.Value{
			{int64(1), "alice"},
			{int64(2), "bob"},
		},
	}
	// One permit, so a leaked connection would hang the second iteration.
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	for range 2 {
		rows, err := pool.Query(t.Context(), "SELECT id, name FROM t")
		if err != nil {
			t.Fatalf("Query() = %v", err)
		}

		seen := 0
		for row := range rows.All() {
			var id int
			var name string
			if err := row.Scan(&id, &name); err != nil {
				t.Fatalf("Scan() = %v", err)
			}
			seen++
			if id != seen {
				t.Errorf("row %d: id = %d", seen, id)
			}
		}
		if seen != 2 {
			t.Fatalf("iterated %d rows, want 2", seen)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Err() = %v", err)
		}
	}
}

// The idiom that panics if Close is not idempotent.
func TestPoolDeferredCloseAlongsideIterator(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{columns: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	func() {
		rows, err := pool.Query(t.Context(), "SELECT id FROM t")
		if err != nil {
			t.Fatalf("Query() = %v", err)
		}
		defer rows.Close()

		for range rows.All() {
		}
	}()

	// The permit came back exactly once.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := pool.Acquire(ctx); err != nil {
		t.Fatalf("the connection was not returned: %v", err)
	}
}

func TestPoolQueryRow(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{columns: []string{"n"}, rows: [][]driver.Value{{int64(42)}}}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	var value int
	if err := pool.QueryRow(t.Context(), "SELECT n").Scan(&value); err != nil {
		t.Fatalf("Scan() = %v", err)
	}
	if value != 42 {
		t.Fatalf("got %d, want 42", value)
	}

	// The connection came back, so a second query can run on the single permit.
	if err := pool.QueryRow(t.Context(), "SELECT n").Scan(&value); err != nil {
		t.Fatalf("second Scan() = %v", err)
	}
}

func TestPoolQueryRowReportsNoRows(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, &fakeConnector{columns: []string{"n"}}, Config{MaxConns: 1})

	var value int
	if err := pool.QueryRow(t.Context(), "SELECT n").Scan(&value); !errors.Is(err, ErrNoRows) {
		t.Fatalf("Scan() on an empty result = %v, want ErrNoRows", err)
	}
}

// Releasing without scanning must still close the query and free the connection.
func TestPoolQueryRowReleaseWithoutScan(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{columns: []string{"n"}, rows: [][]driver.Value{{int64(1)}}}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	pool.QueryRow(t.Context(), "SELECT n").Release()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := pool.Acquire(ctx); err != nil {
		t.Fatalf("the connection was not returned by Release: %v", err)
	}
}

// The canonical transaction idiom: a deferred rollback alongside a commit.
func TestTransactionCommitWithDeferredRollback(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}

	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() = %v", err)
	}
	if err := tx.Rollback(t.Context()); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Rollback() after Commit = %v, want ErrTxClosed", err)
	}

	driverConn := conn.(*connWrapper).conn().driver.(*fakeDriverConn)
	if got := driverConn.commits.Load(); got != 1 {
		t.Errorf("driver Commit called %d times, want 1", got)
	}
	if got := driverConn.rollback.Load(); got != 0 {
		t.Errorf("driver Rollback called %d times after a commit, want 0", got)
	}
	conn.Release()
}

// A transaction the caller abandoned must be unwound before the connection can
// serve anyone else, or the next caller inherits its locks and snapshot.
func TestAbandonedTransactionIsUnwoundOnRelease(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	if _, err := conn.Begin(t.Context()); err != nil {
		t.Fatalf("Begin() = %v", err)
	}

	driverConn := conn.(*connWrapper).conn().driver.(*fakeDriverConn)
	conn.Release() // released without commit or rollback

	if got := driverConn.rollback.Load(); got != 1 {
		t.Fatalf("driver Rollback called %d times on release, want 1", got)
	}
	// Unwinding must not have cost a reconnect.
	if got := connector.connects.Load(); got != 1 {
		t.Errorf("connector dialled %d times, want 1", got)
	}
	if got := pool.Stat().TotalConnections(); got != 1 {
		t.Errorf("TotalConnections() = %d, want 1", got)
	}
}

// ResetSession is the hook database/sql itself uses before reusing a connection,
// so a driver that implements it already knows what has to be cleared.
func TestReleaseCallsResetSession(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	driverConn := conn.(*connWrapper).conn().driver.(*fakeDriverConn)
	conn.Release()

	if got := driverConn.resets.Load(); got != 1 {
		t.Fatalf("ResetSession called %d times, want 1", got)
	}
}

// A connection whose reset fails is in an unknown state and must be discarded.
func TestFailedResetDiscardsTheConnection(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	driverConn := conn.(*connWrapper).conn().driver.(*fakeDriverConn)
	driverConn.resetErr = errors.New("session is wedged")
	conn.Release()

	if got := driverConn.closed.Load(); got != 1 {
		t.Errorf("driver Close called %d times, want 1", got)
	}
	if got := pool.Stat().TotalConnections(); got != 0 {
		t.Errorf("TotalConnections() = %d, want 0", got)
	}
}

// A driver that reports itself invalid must not be handed on.
func TestInvalidConnectionIsDiscarded(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	conn.(*connWrapper).conn().driver.(*fakeDriverConn).invalid.Store(true)
	conn.Release()

	if got := pool.Stat().TotalConnections(); got != 0 {
		t.Errorf("TotalConnections() = %d, want 0", got)
	}
}

func TestConnUseAfterReleaseIsRefused(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, &fakeConnector{}, Config{MaxConns: 1})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	conn.Release()

	if _, err := conn.Exec(t.Context(), "SELECT 1"); !errors.Is(err, ErrConnReleased) {
		t.Errorf("Exec() = %v, want ErrConnReleased", err)
	}
	if _, err := conn.Query(t.Context(), "SELECT 1"); !errors.Is(err, ErrConnReleased) {
		t.Errorf("Query() = %v, want ErrConnReleased", err)
	}
	if _, err := conn.Begin(t.Context()); !errors.Is(err, ErrConnReleased) {
		t.Errorf("Begin() = %v, want ErrConnReleased", err)
	}
	if err := conn.Ping(t.Context()); !errors.Is(err, ErrConnReleased) {
		t.Errorf("Ping() = %v, want ErrConnReleased", err)
	}
}

func TestAcquireAfterCloseFailsFast(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, &fakeConnector{}, Config{MaxConns: 1})
	pool.Close()

	if _, err := pool.Acquire(t.Context()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Acquire() = %v, want ErrPoolClosed", err)
	}
}

// A driver offering only the bare minimum must still work, via prepare-and-execute.
func TestBareDriverUsesThePreparedFallback(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{
		omitResetter: true,
		columns:      []string{"n"},
		rows:         [][]driver.Value{{int64(5)}},
	}
	pool := newTestPool(t, connector, Config{MaxConns: 1})

	var value int
	if err := pool.QueryRow(t.Context(), "SELECT n").Scan(&value); err != nil {
		t.Fatalf("Scan() = %v", err)
	}
	if value != 5 {
		t.Fatalf("got %d, want 5", value)
	}

	if _, err := pool.Exec(t.Context(), "UPDATE t SET x = 1"); err != nil {
		t.Fatalf("Exec() = %v", err)
	}
}

func TestPoolSatisfiesTheInterface(t *testing.T) {
	t.Parallel()

	var pool gpool.Pool = newTestPool(t, &fakeConnector{}, Config{})
	if got := pool.Stat().MaxConnections(); got != 2 {
		t.Fatalf("MaxConnections() = %d, want 2", got)
	}
}
