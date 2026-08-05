// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package benchmarks

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/gsoultan/gpool/pkg/gpool"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

// requireDatabase returns the configured connection string, skipping when unset.
func requireDatabase(tb testing.TB) string {
	tb.Helper()

	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		tb.Skip("DATABASE_URL not set")
	}
	return connString
}

// newGpool builds a pool for benchmarking, skipping when no database is configured.
func newGpool(b *testing.B, config postgrespool.Config) gpool.Pool {
	b.Helper()

	config.ConnString = requireDatabase(b)

	pool, err := gpool.NewPool(gpool.Postgres, config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(pool.Close)
	return pool
}

// pgxpoolDefaultMaxConns mirrors what pgxpool.New picks when nothing is configured.
// The comparison benchmarks use it so both pools have the same capacity: a pool with
// fewer connections than there are callers queues, and that queueing would otherwise
// be read as driver overhead.
func pgxpoolDefaultMaxConns() int32 {
	return int32(max(4, runtime.GOMAXPROCS(0)))
}

// BenchmarkGpoolQueryRow is the counterpart to BenchmarkPgxPool, sized identically so
// the two numbers are directly comparable.
func BenchmarkGpoolQueryRow(b *testing.B) {
	pool := newGpool(b, postgrespool.Config{MaxConns: pgxpoolDefaultMaxConns()})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			var value int
			if err := pool.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGpoolQueryRowStress mirrors BenchmarkPgxPoolStress: far more goroutines
// than connections, which is where acquisition contention actually shows up.
func BenchmarkGpoolQueryRowStress(b *testing.B) {
	pool := newGpool(b, postgrespool.Config{MaxConns: 100})

	b.SetParallelism(100)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			var value int
			if err := pool.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGpoolAcquireRelease isolates the pool's own cost from any query, which is
// what the lock striping and the lock-free stat counters are there to keep low.
func BenchmarkGpoolAcquireRelease(b *testing.B) {
	pool := newGpool(b, postgrespool.Config{MaxConns: 100, MinConns: 16})

	// Warm the pool so the measurement is reuse, not connection establishment.
	conns := make([]gpool.Conn, 0, 16)
	for range 16 {
		conn, err := pool.Acquire(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		conn.Release()
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				b.Fatal(err)
			}
			conn.Release()
		}
	})
}

// BenchmarkGpoolQueryIterator measures the iterator path over a multi-row result.
func BenchmarkGpoolQueryIterator(b *testing.B) {
	pool := newGpool(b, postgrespool.Config{MaxConns: 10})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			rows, err := pool.Query(ctx, "SELECT generate_series(1, 10)")
			if err != nil {
				b.Fatal(err)
			}
			for row := range rows.All() {
				var value int
				if err := row.Scan(&value); err != nil {
					b.Fatal(err)
				}
			}
			if err := rows.Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGpoolResetQuery shows the cost of PgBouncer-style session isolation:
// one extra round trip per release, held while the caller's pool slot is still out.
func BenchmarkGpoolResetQuery(b *testing.B) {
	pool := newGpool(b, postgrespool.Config{MaxConns: 10, ResetQuery: "DISCARD ALL"})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			var value int
			if err := pool.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
				b.Fatal(err)
			}
		}
	})
}
