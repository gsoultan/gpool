// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Comparative benchmarks against a running PgBouncer and a running gpoolproxy.
//
// Both proxies must be pointed at the same PostgreSQL, with the same server-side
// pool size, or the result measures capacity rather than overhead. That mistake
// has already been made once in this repository: gpool looked 9% slower than
// pgxpool purely because their pools were different sizes.
//
//	DATABASE_URL=…  PGBOUNCER_URL=…  PROXY_URL=…  go test -bench=Throughput -benchtime=20000x
//
// A target with no URL set is skipped, so a partial comparison still runs.
var benchTargets = []struct {
	name string
	env  string
}{
	{"direct", "DATABASE_URL"},
	{"pgbouncer", "PGBOUNCER_URL"},
	{"gpoolproxy", "PROXY_URL"},
}

// clientCounts sweeps how many client goroutines are in flight at once.
//
// The sweep is the measurement that matters. Both proxies do the same work per
// query, so at one client they can only look alike; what separates a
// single-threaded event loop from one that uses every core is what happens when
// the clients arrive together.
var clientCounts = clientSweep()

// clientSweep reads the sweep from GPOOL_BENCH_CLIENTS so a run can be retargeted
// without a rebuild, which matters when the binary is cross-compiled into a
// container to keep it on the same network as the proxies it measures.
func clientSweep() []int {
	setting := os.Getenv("GPOOL_BENCH_CLIENTS")
	if setting == "" {
		return []int{1, 8, 32, 128, 512}
	}

	var counts []int
	for field := range strings.SplitSeq(setting, ",") {
		count, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || count < 1 {
			panic("GPOOL_BENCH_CLIENTS must be a comma-separated list of positive integers, got " + setting)
		}
		counts = append(counts, count)
	}
	return counts
}

func BenchmarkThroughput(b *testing.B) {
	// Client count outside, target inside, so the three targets at a given
	// concurrency run next to each other in time.
	//
	// The order matters more than it looks. Sweeping one target to completion
	// before starting the next lets any drift in the machine — thermal, noisy
	// neighbour, page cache — land entirely on whichever target held the slot,
	// and be read as a difference between them. Interleaving spreads it across
	// all three.
	for _, clients := range clientCounts {
		for _, target := range benchTargets {
			url := os.Getenv(target.env)
			if url == "" {
				continue
			}
			b.Run(fmt.Sprintf("clients=%d/%s", clients, target.name), func(b *testing.B) {
				benchmarkTarget(b, url, clients)
			})
		}
	}
}

func benchmarkTarget(b *testing.B, url string, clients int) {
	b.Helper()

	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		b.Fatalf("ParseConfig() = %v", err)
	}
	// Client-side capacity matches the client count so a queue on this side does
	// not get counted as proxy latency.
	config.MaxConns = int32(clients)
	config.MinConns = int32(clients)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		b.Fatalf("pgxpool.NewWithConfig() = %v", err)
	}
	defer pool.Close()

	// Warm the pool so connection setup is not charged to the first iterations.
	//
	// The direct target stops being able to do this well before the pooled ones
	// do, because every client is a PostgreSQL backend. That is not a benchmark
	// failure; it is the entire reason a pooler exists, so it is reported rather
	// than treated as one.
	if err := warm(pool, clients); err != nil {
		if strings.Contains(err.Error(), "53300") {
			b.Skipf("%d clients exceed the server's max_connections: %v", clients, err)
		}
		b.Fatalf("warming the pool = %v", err)
	}

	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var failures atomic.Int64

	b.ResetTimer()

	var wg sync.WaitGroup
	for range clients {
		wg.Go(func() {
			ctx := context.Background()
			for remaining.Add(-1) >= 0 {
				var answer int
				if err := pool.QueryRow(ctx, "SELECT $1::int", 1).Scan(&answer); err != nil {
					failures.Add(1)
					return
				}
			}
		})
	}
	wg.Wait()

	b.StopTimer()

	if count := failures.Load(); count > 0 {
		b.Fatalf("%d queries failed", count)
	}
	// ns/op is per query whatever the client count, so the scaling story is in
	// queries per second rather than in the default column.
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")
}

// warm opens every pooled connection before the timer starts.
func warm(pool *pgxpool.Pool, clients int) error {
	var wg sync.WaitGroup
	errs := make(chan error, clients)

	for range clients {
		wg.Go(func() {
			var answer int
			if err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&answer); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	return <-errs
}
