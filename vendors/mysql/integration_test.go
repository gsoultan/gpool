// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package mysql_test

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
	"github.com/gsoultan/gpool/vendors/mysql"
)

// target is one server the suite runs against.
type target struct {
	name   string
	vendor gpool.Vendor
	dsn    string
}

// targets returns every server configured in the environment.
//
//	MYSQL_DSN='root:root@tcp(127.0.0.1:53306)/gpool?parseTime=true' \
//	MARIADB_DSN='root:root@tcp(127.0.0.1:53307)/gpool?parseTime=true' go test ./...
//
// One implementation serves both names because MariaDB speaks the MySQL wire
// protocol — but "speaks the same protocol" is not "behaves the same way", and
// the two have been diverging since 2009 over collations, auth plugins and
// reserved words. Running the suite against one of them proves nothing about the
// other, so both are exercised whenever both are configured.
func targets(t *testing.T) []target {
	t.Helper()

	candidates := []struct {
		name   string
		env    string
		vendor gpool.Vendor
	}{
		{"mysql", "MYSQL_DSN", mysql.MySQL},
		{"mariadb", "MARIADB_DSN", mysql.MariaDB},
	}

	var configured []target
	for _, candidate := range candidates {
		if value := os.Getenv(candidate.env); value != "" {
			configured = append(configured, target{candidate.name, candidate.vendor, value})
		}
	}
	if len(configured) == 0 {
		t.Skip("neither MYSQL_DSN nor MARIADB_DSN is set")
	}
	return configured
}

// eachTarget runs body against every configured server, as its own subtest so a
// MariaDB-only failure is named rather than hidden behind a passing MySQL run.
func eachTarget(t *testing.T, body func(*testing.T, target)) {
	t.Helper()

	for _, server := range targets(t) {
		t.Run(server.name, func(t *testing.T) { body(t, server) })
	}
}

func newPool(t *testing.T, server target, config mysql.Config) gpool.Pool {
	t.Helper()

	config.DSN = server.dsn

	pool, err := mysql.New(config)
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

// Each name must resolve through the registry and reach its own server.
func TestVendorSelfRegisters(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool, err := gpool.NewPool(server.vendor, mysql.Config{DSN: server.dsn, MaxConns: 2})
		if err != nil {
			t.Fatalf("NewPool(%s) = %v", server.vendor, err)
		}
		defer pool.Close()

		var value int
		if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
			t.Fatalf("QueryRow() = %v", err)
		}
		if value != 1 {
			t.Fatalf("got %d, want 1", value)
		}
	})
}

func TestQueryRoundTrip(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 4})

		var (
			number int64
			text   string
			flag   bool
		)
		err := pool.QueryRow(t.Context(), "SELECT ?, ?, ?", 42, "hello", true).Scan(&number, &text, &flag)
		if err != nil {
			t.Fatalf("Scan() = %v", err)
		}
		if number != 42 || text != "hello" || !flag {
			t.Fatalf("got (%d, %q, %v)", number, text, flag)
		}
	})
}

func TestIteratorReleasesTheConnection(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 1})
		table := scratchTable(t, pool, "(id INT PRIMARY KEY, label VARCHAR(32))")

		for i := range 3 {
			if _, err := pool.Exec(t.Context(),
				fmt.Sprintf("INSERT INTO %s (id, label) VALUES (?, ?)", table), i, fmt.Sprintf("row-%d", i)); err != nil {
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
	})
}

func TestQueryRowReportsNoRows(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 2})
		table := scratchTable(t, pool, "(id INT PRIMARY KEY)")

		var id int
		err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT id FROM %s WHERE id = 999", table)).Scan(&id)
		if !errors.Is(err, sqldriver.ErrNoRows) {
			t.Fatalf("Scan() on an empty result = %v, want ErrNoRows", err)
		}
	})
}

// MySQL reports LAST_INSERT_ID, which PostgreSQL has no equivalent for.
func TestExecReportsLastInsertID(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 2})
		table := scratchTable(t, pool, "(id INT AUTO_INCREMENT PRIMARY KEY, label VARCHAR(32))")

		result, err := pool.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s (label) VALUES (?)", table), "first")
		if err != nil {
			t.Fatalf("Exec() = %v", err)
		}
		if got := result.RowsAffected(); got != 1 {
			t.Errorf("RowsAffected() = %d, want 1", got)
		}

		withID, ok := result.(interface{ LastInsertID() (int64, bool) })
		if !ok {
			t.Fatal("the result should expose LastInsertID")
		}
		if id, present := withID.LastInsertID(); !present || id != 1 {
			t.Errorf("LastInsertID() = (%d, %v), want (1, true)", id, present)
		}
	})
}

// The canonical transaction idiom, against a real driver.
func TestTransactionCommitWithDeferredRollback(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 2})
		table := scratchTable(t, pool, "(id INT PRIMARY KEY) ENGINE=InnoDB")

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
	})
}

// A transaction the caller abandoned must not leak onward: the next caller would
// inherit its locks and its snapshot.
func TestAbandonedTransactionIsUnwound(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 1})
		table := scratchTable(t, pool, "(id INT PRIMARY KEY) ENGINE=InnoDB")

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
		// Released without commit or rollback.
		conn.Release()

		// MaxConns is 1, so the next caller gets that same connection back. The
		// uncommitted row must have been rolled away with it.
		var count int
		if err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count); err != nil {
			t.Fatalf("the next caller inherited the abandoned transaction: %v", err)
		}
		if count != 0 {
			t.Fatalf("row count = %d; the abandoned transaction was not rolled back", count)
		}

		// Unwinding must not have cost a reconnect.
		if got := pool.Stat().TotalConnections(); got != 1 {
			t.Errorf("TotalConnections() = %d, want 1", got)
		}
	})
}

func TestPoolUnderConcurrentLoad(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 8, MinConns: 2})

		var wg sync.WaitGroup
		errs := make(chan error, 128)

		for range 128 {
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
		if stat.AcquireCount() < 128 {
			t.Errorf("AcquireCount() = %d, want at least 128", stat.AcquireCount())
		}
	})
}

// The ceiling must hold against a real server, which is the number that matters:
// exceeding it means more backends than the operator authorised.
func TestNeverExceedsMaxConns(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		const capacity = 4
		pool := newPool(t, server, mysql.Config{MaxConns: capacity})

		var peak atomic.Int32
		var wg sync.WaitGroup

		for range 64 {
			wg.Go(func() {
				for range 25 {
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
	})
}

func TestTypesRoundTrip(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 2})
		table := scratchTable(t, pool, `(
			id INT PRIMARY KEY,
			name VARCHAR(64),
			amount DECIMAL(10,2),
			ratio DOUBLE,
			flag BOOLEAN,
			payload BLOB,
			created DATETIME
		)`)

		created := time.Date(2026, 8, 5, 12, 30, 0, 0, time.UTC)
		_, err := pool.Exec(t.Context(),
			fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?)", table),
			1, "widget", "19.99", 0.25, true, []byte{0x01, 0x02}, created)
		if err != nil {
			t.Fatalf("INSERT = %v", err)
		}

		var (
			id      int
			name    string
			amount  string
			ratio   float64
			flag    bool
			payload []byte
			when    time.Time
		)
		err = pool.QueryRow(t.Context(), fmt.Sprintf("SELECT * FROM %s WHERE id = ?", table), 1).
			Scan(&id, &name, &amount, &ratio, &flag, &payload, &when)
		if err != nil {
			t.Fatalf("Scan() = %v", err)
		}

		if id != 1 || name != "widget" || amount != "19.99" || ratio != 0.25 || !flag {
			t.Errorf("got (%d, %q, %q, %v, %v)", id, name, amount, ratio, flag)
		}
		if len(payload) != 2 || payload[0] != 1 || payload[1] != 2 {
			t.Errorf("payload = %v", payload)
		}
		// DATETIME needs parseTime=true in the DSN to arrive as a time.Time.
		if !when.Equal(created) {
			t.Errorf("created = %v, want %v", when, created)
		}
	})
}

func TestNullHandling(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 2})

		var nullable any
		if err := pool.QueryRow(t.Context(), "SELECT NULL").Scan(&nullable); err != nil {
			t.Fatalf("Scan(NULL into any) = %v", err)
		}
		if nullable != nil {
			t.Errorf("got %v, want nil", nullable)
		}

		// A NULL into a value type has no sensible answer and must say so.
		var text string
		if err := pool.QueryRow(t.Context(), "SELECT NULL").Scan(&text); !errors.Is(err, sqldriver.ErrScan) {
			t.Errorf("Scan(NULL into *string) = %v, want ErrScan", err)
		}
	})
}

func TestNewRejectsBadDSN(t *testing.T) {
	t.Parallel()

	if _, err := mysql.New(mysql.Config{}); !errors.Is(err, mysql.ErrInvalidConfig) {
		t.Errorf("New() without a DSN = %v, want ErrInvalidConfig", err)
	}
	if _, err := mysql.New(mysql.Config{DSN: "not a dsn"}); !errors.Is(err, mysql.ErrInvalidConfig) {
		t.Errorf("New() with a malformed DSN = %v, want ErrInvalidConfig", err)
	}
}

// The same on the database/sql side, which reaches the engine through a
// different Config and so needed the field wired separately.
func TestResizableGrowsWhenHeadroomIsDeclared(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 2, MaxConnsLimit: 6, HealthCheckPeriod: -1})

		resizable, ok := pool.(gpool.Resizable)
		if !ok {
			t.Fatal("the pool is not Resizable")
		}
		if err := resizable.SetMaxConns(6); err != nil {
			t.Fatalf("SetMaxConns(6) with a limit of 6 = %v", err)
		}

		conns := make([]gpool.Conn, 0, 6)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		for i := range 6 {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatalf("acquiring connection %d of 6 after growing = %v", i+1, err)
			}
			conns = append(conns, conn)
		}
		for i := range conns {
			conns[i].Release()
		}

		if err := resizable.SetMaxConns(7); err == nil {
			t.Error("SetMaxConns(7) succeeded past the declared limit of 6")
		}
	})
}
