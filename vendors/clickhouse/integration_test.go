// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package clickhouse_test

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
	"github.com/gsoultan/gpool/vendors/clickhouse"
)

// dsn returns the configured DSN, skipping when none is set.
//
//	CLICKHOUSE_DSN='clickhouse://default:clickhouse@127.0.0.1:59000/gpool' go test ./...
func dsn(t *testing.T) string {
	t.Helper()

	value := os.Getenv("CLICKHOUSE_DSN")
	if value == "" {
		t.Skip("CLICKHOUSE_DSN not set")
	}
	return value
}

func newPool(t *testing.T, config clickhouse.Config) gpool.Pool {
	t.Helper()

	config.DSN = dsn(t)

	pool, err := clickhouse.New(config)
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
	pool, err := gpool.NewPool(clickhouse.ClickHouse, clickhouse.Config{DSN: dsn(t), MaxConns: 2})
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

func TestQueryRoundTrip(t *testing.T) {
	pool := newPool(t, clickhouse.Config{MaxConns: 4})

	var number int64
	var text string
	if err := pool.QueryRow(t.Context(), "SELECT toInt64(42), 'hello'").Scan(&number, &text); err != nil {
		t.Fatalf("Scan() = %v", err)
	}
	if number != 42 || text != "hello" {
		t.Fatalf("got (%d, %q)", number, text)
	}
}

func TestIteratorReleasesTheConnection(t *testing.T) {
	pool := newPool(t, clickhouse.Config{MaxConns: 1})

	// One permit, so a leaked connection would hang the second pass.
	for range 2 {
		rows, err := pool.Query(t.Context(), "SELECT number FROM system.numbers LIMIT 5")
		if err != nil {
			t.Fatalf("Query() = %v", err)
		}

		seen := 0
		for row := range rows.All() {
			var n uint64
			if err := row.Scan(&n); err != nil {
				t.Fatalf("Scan() = %v", err)
			}
			if n != uint64(seen) {
				t.Errorf("row %d = %d", seen, n)
			}
			seen++
		}
		if seen != 5 {
			t.Fatalf("iterated %d rows, want 5", seen)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Err() = %v", err)
		}
	}
}

// Batched inserts are the shape ClickHouse is built for; one row per INSERT is
// pathological against a column store.
func TestBatchedInsertAndAggregate(t *testing.T) {
	pool := newPool(t, clickhouse.Config{MaxConns: 4})
	table := scratchTable(t, pool, "(id UInt64, label String) ENGINE = MergeTree ORDER BY id")

	const rows = 1000
	values := make([]string, 0, rows)
	args := make([]any, 0, rows*2)
	for i := range rows {
		values = append(values, "(?, ?)")
		args = append(args, uint64(i), fmt.Sprintf("label-%d", i))
	}

	statement := fmt.Sprintf("INSERT INTO %s (id, label) VALUES %s", table, joinComma(values))
	if _, err := pool.Exec(t.Context(), statement, args...); err != nil {
		t.Fatalf("batched INSERT = %v", err)
	}

	var count uint64
	if err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&count); err != nil {
		t.Fatalf("count = %v", err)
	}
	if count != rows {
		t.Fatalf("count = %d, want %d", count, rows)
	}

	var total uint64
	if err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT sum(id) FROM %s", table)).Scan(&total); err != nil {
		t.Fatalf("sum = %v", err)
	}
	if want := uint64(rows * (rows - 1) / 2); total != want {
		t.Fatalf("sum = %d, want %d", total, want)
	}
}

func joinComma(values []string) string {
	out := ""
	for i, v := range values {
		if i > 0 {
			out += ", "
		}
		out += v
	}
	return out
}

func TestQueryRowReportsNoRows(t *testing.T) {
	pool := newPool(t, clickhouse.Config{MaxConns: 2})

	var value int
	err := pool.QueryRow(t.Context(), "SELECT 1 WHERE 0").Scan(&value)
	if !errors.Is(err, sqldriver.ErrNoRows) {
		t.Fatalf("Scan() on an empty result = %v, want ErrNoRows", err)
	}
}

func TestPoolUnderConcurrentLoad(t *testing.T) {
	pool := newPool(t, clickhouse.Config{MaxConns: 8, MinConns: 2})

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

// The ceiling must hold against a real server. ClickHouse spends real memory per
// concurrent query, so exceeding it is not merely untidy.
func TestNeverExceedsMaxConns(t *testing.T) {
	const capacity = 4
	pool := newPool(t, clickhouse.Config{MaxConns: capacity})

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
	pool := newPool(t, clickhouse.Config{MaxConns: 2})

	var (
		unsigned uint64
		signed   int64
		ratio    float64
		text     string
		when     time.Time
	)
	err := pool.QueryRow(t.Context(),
		"SELECT toUInt64(7), toInt64(-7), toFloat64(0.25), 'text', toDateTime('2026-08-05 12:30:00')").
		Scan(&unsigned, &signed, &ratio, &text, &when)
	if err != nil {
		t.Fatalf("Scan() = %v", err)
	}

	if unsigned != 7 || signed != -7 || ratio != 0.25 || text != "text" {
		t.Errorf("got (%d, %d, %v, %q)", unsigned, signed, ratio, text)
	}
	if when.IsZero() {
		t.Error("DateTime did not decode")
	}
}

// ClickHouse does not offer transactions outside an experimental mode. The
// server's refusal is reported rather than hidden, and the pool stays usable.
func TestTransactionsAreReportedNotHidden(t *testing.T) {
	pool := newPool(t, clickhouse.Config{MaxConns: 1})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}

	if _, err := conn.Begin(t.Context()); err != nil {
		t.Logf("Begin() = %v (expected without experimental transactions)", err)
	}
	conn.Release()

	// Whatever the server said, the connection must be fit for the next caller.
	var value int
	if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("the pool did not survive a refused Begin: %v", err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	t.Parallel()

	if _, err := clickhouse.New(clickhouse.Config{}); !errors.Is(err, clickhouse.ErrInvalidConfig) {
		t.Errorf("New() without a DSN = %v, want ErrInvalidConfig", err)
	}
	if _, err := clickhouse.New(clickhouse.Config{DSN: "://not a dsn"}); !errors.Is(err, clickhouse.ErrInvalidConfig) {
		t.Errorf("New() with a malformed DSN = %v, want ErrInvalidConfig", err)
	}
}
