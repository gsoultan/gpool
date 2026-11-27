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

	"github.com/gsoultan/nextar/pkg/gpool"
	postgrespool "github.com/gsoultan/nextar/pkg/vendors/postgres/pool"
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

// A transaction begun with a statement and then with Begin is one
// transaction, which the server would only warn about: the Tx's Rollback
// would then undo what ran before Begin. Begin refuses, with an error that
// can be told apart, where it used to come back as caduceus's own sentinel,
// which a caller of this package had no name for.
func TestBeginOnAConnectionAlreadyInATransaction(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})
	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(t.Context(), "BEGIN"); err != nil {
		t.Fatalf("BEGIN = %v", err)
	}
	if _, err := conn.Begin(t.Context()); !errors.Is(err, postgrespool.ErrInTransaction) {
		t.Fatalf("Begin() in a transaction = %v, want ErrInTransaction", err)
	}
	if _, err := conn.Exec(t.Context(), "ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK = %v", err)
	}
	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() after the rollback = %v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() = %v", err)
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

// Begin sends nothing: BEGIN goes with the transaction's first statement. So a
// transaction begun and released unused is idle as far as the server knows, and
// a gate that asked only the wire would hand the connection on with that BEGIN
// still waiting. It would then go out in front of the next caller's first
// statement, which would run inside a transaction nobody is going to end.
func TestPoolUnwindsTransactionNeverSentOnRelease(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})
	ctx := t.Context()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	if _, err := conn.Begin(ctx); err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	conn.Release()

	next, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire() after a transaction begun and never used = %v", err)
	}
	defer next.Release()

	// VACUUM is rejected inside a transaction block, so it says whether a BEGIN
	// came with it.
	if _, err := next.Exec(ctx, "VACUUM"); err != nil {
		t.Fatalf("the next caller's statement carried the previous caller's BEGIN: %v", err)
	}
}

// nextTx is the transaction of a connection's next caller, however they began
// it.
type nextTx struct {
	exec     func(sql string) error
	rollback func() error
}

// beginThroughThePool is the transaction a caller would write.
func beginThroughThePool(ctx context.Context, conn gpool.Conn) (nextTx, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nextTx{}, err
	}
	return nextTx{
		exec: func(sql string) error {
			_, err := tx.Exec(ctx, sql)
			return err
		},
		rollback: func() error { return tx.Rollback(ctx) },
	}, nil
}

// beginInSQL is a transaction the pool's own bookkeeping knows nothing of, so
// nothing on the next caller's side can have ended a Tx kept by the caller
// before them.
func beginInSQL(ctx context.Context, conn gpool.Conn) (nextTx, error) {
	exec := func(sql string) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}
	return nextTx{exec: exec, rollback: func() error { return exec("ROLLBACK") }}, exec("BEGIN")
}

// A caller that releases a connection may still be holding the Tx it began
// there. That Tx used to stay live: its Exec ran inside whatever transaction
// the connection's next caller had open, and its Commit committed that
// transaction for them. Unwinding the connection on release then ended it, but
// only where there was a transaction to unwind: one the caller had ended in
// SQL, with a COMMIT run as a statement, stayed live through the release. A Tx
// now ends with the acquisition it began on, whatever state it was left in.
func TestTxIsSpentOnceItsConnectionIsReleased(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})
	ctx := t.Context()
	table := scratchTable(t, pool, "(id int PRIMARY KEY)")

	lefts := []struct {
		name  string
		leave func(conn gpool.Conn, tx gpool.Tx)
	}{
		{"begun and never used", func(gpool.Conn, gpool.Tx) {}},
		{"open", func(_ gpool.Conn, tx gpool.Tx) { _, _ = tx.Exec(ctx, "SELECT 1") }},
		{"failed", func(_ gpool.Conn, tx gpool.Tx) {
			_, _ = tx.Exec(ctx, "SELECT * FROM a_table_that_does_not_exist")
		}},
		{"committed in SQL through the Tx", func(_ gpool.Conn, tx gpool.Tx) {
			_, _ = tx.Exec(ctx, "SELECT 1")
			_, _ = tx.Exec(ctx, "COMMIT")
		}},
		{"committed in SQL on the connection", func(conn gpool.Conn, tx gpool.Tx) {
			_, _ = tx.Exec(ctx, "SELECT 1")
			_, _ = conn.Exec(ctx, "COMMIT")
		}},
		{"rolled back in SQL", func(_ gpool.Conn, tx gpool.Tx) {
			_, _ = tx.Exec(ctx, "SELECT 1")
			_, _ = tx.Exec(ctx, "ROLLBACK")
		}},
	}
	begins := []struct {
		name string
		open func(context.Context, gpool.Conn) (nextTx, error)
	}{
		{"through the pool", beginThroughThePool},
		{"in SQL", beginInSQL},
	}
	settles := []struct {
		name string
		run  func(tx gpool.Tx) error
	}{
		{"Commit", func(tx gpool.Tx) error { return tx.Commit(ctx) }},
		{"Rollback", func(tx gpool.Tx) error { return tx.Rollback(ctx) }},
	}

	for _, left := range lefts {
		for _, begin := range begins {
			for _, settle := range settles {
				name := left.name + ", the next transaction begun " + begin.name + ", then " + settle.name
				t.Run(name, func(t *testing.T) {
					if _, err := pool.Exec(ctx, "TRUNCATE "+table); err != nil {
						t.Fatalf("TRUNCATE = %v", err)
					}

					conn, err := pool.Acquire(ctx)
					if err != nil {
						t.Fatalf("Acquire() = %v", err)
					}
					defer conn.Release()
					kept, err := conn.Begin(ctx)
					if err != nil {
						t.Fatalf("Begin() = %v", err)
					}
					left.leave(conn, kept)
					conn.Release()

					// MaxConns is 1, so the next caller has the same connection.
					next, err := pool.Acquire(ctx)
					if err != nil {
						t.Fatalf("Acquire() = %v", err)
					}
					defer next.Release()
					theirs, err := begin.open(ctx, next)
					if err != nil {
						t.Fatalf("the next caller's BEGIN = %v", err)
					}
					if err := theirs.exec("INSERT INTO " + table + " VALUES (1)"); err != nil {
						t.Fatalf("the next caller's first statement = %v", err)
					}

					if _, err := kept.Exec(ctx, "INSERT INTO "+table+" VALUES (2)"); !errors.Is(err, postgrespool.ErrTxClosed) {
						t.Errorf("Exec() on the Tx kept past Release = %v, want ErrTxClosed", err)
					}
					if err := settle.run(kept); !errors.Is(err, postgrespool.ErrTxClosed) {
						t.Errorf("%s() on the Tx kept past Release = %v, want ErrTxClosed", settle.name, err)
					}

					// A statement of theirs after it: had the kept Tx ended their
					// transaction either way, this one would commit by itself.
					if err := theirs.exec("INSERT INTO " + table + " VALUES (3)"); err != nil {
						t.Fatalf("the next caller's second statement = %v", err)
					}
					if err := theirs.rollback(); err != nil {
						t.Fatalf("the next caller's rollback = %v", err)
					}
					var rows int
					if err := next.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rows); err != nil {
						t.Fatalf("count = %v", err)
					}
					if rows != 0 {
						t.Fatalf("%d rows survived the next caller's rollback: the kept Tx reached their transaction", rows)
					}
				})
			}
		}
	}
}

// What the saved round trip looks like from the server: a transaction begun is
// not one it knows of until the first statement arrives, and one that runs no
// statement it never hears of at all.
func TestBeginIsSentWithTheFirstStatement(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2})
	ctx := t.Context()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer conn.Release()
	var pid int32
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatalf("pg_backend_pid() = %v", err)
	}
	// Asked through the pool's other connection.
	state := func() string {
		t.Helper()
		var state string
		if err := pool.QueryRow(ctx, "SELECT state FROM pg_stat_activity WHERE pid = $1::int4", pid).Scan(&state); err != nil {
			t.Fatalf("pg_stat_activity = %v", err)
		}
		return state
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	if got := state(); got != "idle" {
		t.Fatalf("server state after Begin() = %q, want idle: BEGIN was sent on its own", got)
	}
	if _, err := tx.Exec(ctx, "SELECT $1::int4", 1); err != nil {
		t.Fatalf("Exec() = %v", err)
	}
	if got := state(); got != "idle in transaction" {
		t.Fatalf("server state after the first statement = %q, want idle in transaction", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() = %v", err)
	}
	if got := state(); got != "idle" {
		t.Fatalf("server state after Commit() = %q, want idle", got)
	}

	empty, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	if err := empty.Commit(ctx); err != nil {
		t.Fatalf("Commit() of a transaction that ran nothing = %v", err)
	}
	if _, err := conn.Exec(ctx, "VACUUM"); err != nil {
		t.Fatalf("a statement after an empty transaction found a BEGIN in front of it: %v", err)
	}
}

// sqlState is the SQLSTATE a server error carries, empty for any other error.
func sqlState(err error) string {
	var coded interface{ SQLState() string }
	if !errors.As(err, &coded) {
		return ""
	}
	return coded.SQLState()
}

// A first statement sent without arguments shares one string with its BEGIN,
// and the server parses a string whole before running any of it, so one it
// cannot parse takes the BEGIN down with it. For a while that left the caller
// holding a transaction the server had never opened, and whatever they then
// ran on the connection itself ran on its own and stayed committed. The string
// is now sent again as its two parts, which leaves what an unparseable
// statement has always left behind: a transaction open and failed, in which
// the server runs nothing until it is rolled back.
func TestTransactionWhoseFirstStatementCannotBeParsed(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})
	ctx := t.Context()
	table := scratchTable(t, pool, "(id int PRIMARY KEY)")

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	if _, err := tx.Exec(ctx, "SELEC 1"); sqlState(err) != "42601" {
		t.Fatalf("Exec() of a statement the server cannot parse = %v, want SQLSTATE 42601", err)
	}

	for _, attempt := range []struct {
		name string
		run  func() error
	}{
		{"Exec on the Tx", func() error {
			_, err := tx.Exec(ctx, "INSERT INTO "+table+" VALUES (1)")
			return err
		}},
		{"Exec on the connection", func() error {
			_, err := conn.Exec(ctx, "INSERT INTO "+table+" VALUES (2)")
			return err
		}},
		{"Exec with an argument on the connection", func() error {
			_, err := conn.Exec(ctx, "INSERT INTO "+table+" VALUES ($1)", 3)
			return err
		}},
		{"QueryRow on the connection", func() error {
			return conn.QueryRow(ctx, "INSERT INTO "+table+" VALUES (4) RETURNING id").Scan(new(int))
		}},
		{"SendBatch on the connection", func() error {
			batch := &gpool.Batch{}
			batch.Queue("INSERT INTO "+table+" VALUES ($1)", 5)
			return conn.(gpool.Batcher).SendBatch(ctx, batch).Close()
		}},
		{"CopyFrom on the connection", func() error {
			_, err := conn.(gpool.BulkCopier).CopyFrom(ctx, gpool.CopyRequest{
				Table:   []string{table},
				Columns: []string{"id"},
				Rows:    gpool.CopyFromSlice(1, func(int) ([]any, error) { return []any{6}, nil }),
			})
			return err
		}},
	} {
		if err := attempt.run(); sqlState(err) != "25P02" {
			t.Errorf("%s in the failed transaction = %v, want SQLSTATE 25P02", attempt.name, err)
		}
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() = %v", err)
	}
	var rows int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rows); err != nil {
		t.Fatalf("count = %v", err)
	}
	if rows != 0 {
		t.Fatalf("%d rows were committed by statements run after the transaction's first statement failed", rows)
	}
	// And the connection is idle: VACUUM would be refused inside a transaction.
	if _, err := conn.Exec(ctx, "VACUUM"); err != nil {
		t.Fatalf("the connection after it: %v", err)
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

// Resizable's contract is that headroom is "declared at construction". Until
// MaxConnsLimit reached the vendor configs there was no way to declare it, the
// engine defaulted the hard ceiling to MaxConns, and every pool was resizable
// downwards only — with an error telling the caller to raise a limit that no
// Config exposed.
func TestResizableGrowsWhenHeadroomIsDeclared(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2, MaxConnsLimit: 8, HealthCheckPeriod: -1})

	resizable, ok := pool.(gpool.Resizable)
	if !ok {
		t.Fatal("the PostgreSQL pool is not Resizable")
	}
	if err := resizable.SetMaxConns(8); err != nil {
		t.Fatalf("SetMaxConns(8) with a limit of 8 = %v", err)
	}

	// Returning nil is not the claim; holding eight at once is. Two would have
	// deadlocked this before the ceiling moved.
	conns := make([]gpool.Conn, 0, 8)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	for i := range 8 {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquiring connection %d of 8 after growing = %v", i+1, err)
		}
		conns = append(conns, conn)
	}
	for i := range conns {
		conns[i].Release()
	}

	if err := resizable.SetMaxConns(9); err == nil {
		t.Error("SetMaxConns(9) succeeded past the declared limit of 8")
	}
}

// Without declared headroom the pool still refuses to grow, which is the point:
// a pool that can grow without bound is how a database runs out of connections.
func TestResizableRefusesToGrowWithoutHeadroom(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2, HealthCheckPeriod: -1})

	resizable, ok := pool.(gpool.Resizable)
	if !ok {
		t.Fatal("the PostgreSQL pool is not Resizable")
	}
	if err := resizable.SetMaxConns(4); err == nil {
		t.Error("grew beyond MaxConns without MaxConnsLimit being declared")
	}
	// Shrinking never needed headroom.
	if err := resizable.SetMaxConns(1); err != nil {
		t.Errorf("SetMaxConns(1) = %v", err)
	}
}
