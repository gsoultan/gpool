// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package mssql_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/sqldriver"
	"github.com/gsoultan/gpool/vendors/mssql"
)

// dsn returns the configured DSN, skipping when none is set.
//
//	MSSQL_DSN='sqlserver://sa:pass@127.0.0.1:51433?database=master' go test ./...
func dsn(t *testing.T) string {
	t.Helper()

	value := os.Getenv("MSSQL_DSN")
	if value == "" {
		t.Skip("MSSQL_DSN not set")
	}
	return value
}

func newPool(t *testing.T, config mssql.Config) gpool.Pool {
	t.Helper()

	config.DSN = dsn(t)

	pool, err := mssql.New(config)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// scratchTable creates a table for one test and drops it afterwards.
func scratchTable(t *testing.T, pool gpool.Pool, definition string) string {
	t.Helper()

	name := fmt.Sprintf("gpool_%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := pool.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s %s", name, definition)); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	})
	return name
}

func TestVendorSelfRegisters(t *testing.T) {
	pool, err := gpool.NewPool(mssql.SQLServer, mssql.Config{DSN: dsn(t), MaxConns: 2})
	if err != nil {
		t.Fatalf("NewPool() = %v", err)
	}
	defer pool.Close()

	var value int
	if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}
	if value != 1 {
		t.Fatalf("got %d, want 1", value)
	}
}

// SQL Server uses @p1-style ordinal placeholders rather than ? or $1.
func TestQueryRoundTrip(t *testing.T) {
	pool := newPool(t, mssql.Config{MaxConns: 4})

	var number int64
	var text string
	if err := pool.QueryRow(t.Context(), "SELECT @p1, @p2", 42, "hello").Scan(&number, &text); err != nil {
		t.Fatalf("Scan() = %v", err)
	}
	if number != 42 || text != "hello" {
		t.Fatalf("got (%d, %q)", number, text)
	}
}

func TestIteratorReleasesTheConnection(t *testing.T) {
	pool := newPool(t, mssql.Config{MaxConns: 1})
	table := scratchTable(t, pool, "(id INT PRIMARY KEY, label NVARCHAR(32))")

	for i := range 3 {
		if _, err := pool.Exec(t.Context(),
			fmt.Sprintf("INSERT INTO %s (id, label) VALUES (@p1, @p2)", table), i, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("INSERT = %v", err)
		}
	}

	// One permit, so a leaked connection would hang the second pass.
	for range 2 {
		rows, err := pool.Query(t.Context(), fmt.Sprintf("SELECT id, label FROM %s ORDER BY id", table))
		if err != nil {
			t.Fatalf("Query() = %v", err)
		}

		seen := 0
		for row := range rows.All() {
			var id int
			var label string
			if err := row.Scan(&id, &label); err != nil {
				t.Fatalf("Scan() = %v", err)
			}
			if label != fmt.Sprintf("row-%d", id) {
				t.Errorf("row %d has label %q", id, label)
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

func TestQueryRowReportsNoRows(t *testing.T) {
	pool := newPool(t, mssql.Config{MaxConns: 2})
	table := scratchTable(t, pool, "(id INT PRIMARY KEY)")

	var id int
	err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT id FROM %s WHERE id = 999", table)).Scan(&id)
	if !errors.Is(err, sqldriver.ErrNoRows) {
		t.Fatalf("Scan() on an empty result = %v, want ErrNoRows", err)
	}
}

// The canonical transaction idiom, against a real server.
func TestTransactionCommitWithDeferredRollback(t *testing.T) {
	pool := newPool(t, mssql.Config{MaxConns: 2})
	table := scratchTable(t, pool, "(id INT PRIMARY KEY)")

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

	if _, err := tx.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table)); err != nil {
		t.Fatalf("Exec() = %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() = %v", err)
	}

	var count int
	if err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatalf("count = %v", err)
	}
	if count != 1 {
		t.Fatalf("committed row count = %d, want 1", count)
	}
}

// A transaction the caller abandoned must not leak onward. On SQL Server an
// abandoned transaction also holds locks, so the next caller would block rather
// than merely see stale data.
func TestAbandonedTransactionIsUnwound(t *testing.T) {
	pool := newPool(t, mssql.Config{MaxConns: 1})
	table := scratchTable(t, pool, "(id INT PRIMARY KEY)")

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}

	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() = %v", err)
	}
	if _, err := tx.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table)); err != nil {
		t.Fatalf("Exec() = %v", err)
	}
	conn.Release() // released without commit or rollback

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	var count int
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatalf("the next caller inherited the abandoned transaction: %v", err)
	}
	if count != 0 {
		t.Fatalf("row count = %d; the abandoned transaction was not rolled back", count)
	}

	// Unwinding must not have cost a reconnect.
	if got := pool.Stat().TotalConnections(); got != 1 {
		t.Errorf("TotalConnections() = %d, want 1", got)
	}
}

func TestPoolUnderConcurrentLoad(t *testing.T) {
	pool := newPool(t, mssql.Config{MaxConns: 8, MinConns: 2})

	var wg sync.WaitGroup
	errs := make(chan error, 64)

	for range 64 {
		wg.Go(func() {
			var value int
			if err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&value); err != nil {
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
		t.Errorf("TotalConnections() = %d, want at most MaxConns (8)", stat.TotalConnections())
	}
	if stat.ActiveConnections() != 0 {
		t.Errorf("ActiveConnections() = %d, want 0 once every caller has finished", stat.ActiveConnections())
	}
}

func TestNeverExceedsMaxConns(t *testing.T) {
	const capacity = 4
	pool := newPool(t, mssql.Config{MaxConns: capacity})

	var peak atomic.Int32
	var wg sync.WaitGroup

	for range 32 {
		wg.Go(func() {
			for range 20 {
				conn, err := pool.Acquire(context.Background())
				if err != nil {
					return
				}
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
}

func TestTypesRoundTrip(t *testing.T) {
	pool := newPool(t, mssql.Config{MaxConns: 2})
	table := scratchTable(t, pool, `(
		id INT PRIMARY KEY,
		name NVARCHAR(64),
		amount DECIMAL(10,2),
		ratio FLOAT,
		flag BIT,
		payload VARBINARY(16),
		created DATETIME2
	)`)

	created := time.Date(2026, 8, 5, 12, 30, 0, 0, time.UTC)
	_, err := pool.Exec(t.Context(),
		fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)", table),
		1, "widget", 19.99, 0.25, true, []byte{0x01, 0x02}, created)
	if err != nil {
		t.Fatalf("INSERT = %v", err)
	}

	var (
		id      int
		name    string
		amount  float64
		ratio   float64
		flag    bool
		payload []byte
		when    time.Time
	)
	err = pool.QueryRow(t.Context(), fmt.Sprintf("SELECT * FROM %s WHERE id = @p1", table), 1).
		Scan(&id, &name, &amount, &ratio, &flag, &payload, &when)
	if err != nil {
		t.Fatalf("Scan() = %v", err)
	}

	if id != 1 || name != "widget" || ratio != 0.25 || !flag {
		t.Errorf("got (%d, %q, %v, %v)", id, name, ratio, flag)
	}
	if len(payload) != 2 || payload[0] != 1 || payload[1] != 2 {
		t.Errorf("payload = %v", payload)
	}
	if !when.Equal(created) {
		t.Errorf("created = %v, want %v", when, created)
	}
}

func TestNullHandling(t *testing.T) {
	pool := newPool(t, mssql.Config{MaxConns: 2})

	var nullable any
	if err := pool.QueryRow(t.Context(), "SELECT NULL").Scan(&nullable); err != nil {
		t.Fatalf("Scan(NULL into any) = %v", err)
	}
	if nullable != nil {
		t.Errorf("got %v, want nil", nullable)
	}

	var text string
	if err := pool.QueryRow(t.Context(), "SELECT NULL").Scan(&text); !errors.Is(err, sqldriver.ErrScan) {
		t.Errorf("Scan(NULL into *string) = %v, want ErrScan", err)
	}
}

func TestNewRejectsBadDSN(t *testing.T) {
	t.Parallel()

	if _, err := mssql.New(mssql.Config{}); !errors.Is(err, mssql.ErrInvalidConfig) {
		t.Errorf("New() without a DSN = %v, want ErrInvalidConfig", err)
	}
}
