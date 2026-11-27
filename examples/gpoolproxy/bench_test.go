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
	name   string
	env    string
	pidEnv string
}{
	{"direct", "DATABASE_URL", ""},
	{"pgbouncer", "PGBOUNCER_URL", "PGBOUNCER_PID"},
	{"gpoolproxy", "PROXY_URL", "PROXY_PID"},
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
				benchmarkTarget(b, url, clients, processOf(target.pidEnv))
			})
		}
	}
}

func benchmarkTarget(b *testing.B, url string, clients int, proxy *process) {
	b.Helper()

	// One connection per client goroutine, all open before the timer starts, so
	// neither a queue on this side nor connection setup is counted as proxy
	// latency.
	//
	// The direct target stops being able to do this well before the pooled ones
	// do, because every client is a PostgreSQL backend. That is not a benchmark
	// failure; it is the entire reason a pooler exists, so it is reported rather
	// than treated as one.
	conns, err := warm(url, clients)
	defer func() {
		for _, conn := range conns {
			conn.close()
		}
	}()
	if err != nil {
		if sqlState(err) == "53300" {
			b.Skipf("%d clients exceed the server's max_connections: %v", clients, err)
		}
		b.Fatalf("opening the client connections = %v", err)
	}

	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var failures atomic.Int64

	before := proxy.sample()
	b.ResetTimer()

	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Go(func() {
			for remaining.Add(-1) >= 0 {
				if _, err := conn.value("SELECT $1::int", 1); err != nil {
					failures.Add(1)
					return
				}
			}
		})
	}
	wg.Wait()

	b.StopTimer()
	after := proxy.sample()

	if count := failures.Load(); count > 0 {
		b.Fatalf("%d queries failed", count)
	}
	// ns/op is per query whatever the client count, so the scaling story is in
	// queries per second rather than in the default column.
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")

	// The proxy's own cost, read from /proc while every client is still
	// connected. Only available when the proxy runs on this host and its pid is
	// given; a proxy in another container is measured with its own tools.
	if proxy != nil && after.ok && before.ok {
		b.ReportMetric(float64((after.cpu-before.cpu).Nanoseconds())/1e3/float64(b.N), "proxy-cpu-µs/query")
		b.ReportMetric(float64(after.rss)/(1<<20), "proxy-rss-MiB")
	}
}

// warm opens every client connection and runs one query on each, which leaves
// the benchmark's statement prepared. The clients cache their statements, as a
// client library does by default, so what is timed is a Bind and an Execute
// against a named statement.
//
// The connections opened so far are returned even on failure, for the caller to
// close.
func warm(url string, clients int) ([]*wireClient, error) {
	conns := make([]*wireClient, clients)
	errs := make(chan error, clients)

	var wg sync.WaitGroup
	for i := range conns {
		wg.Go(func() {
			conn, err := dialClient(context.Background(), url)
			if err != nil {
				errs <- err
				return
			}
			conns[i] = conn.caching()
			if _, err := conn.value("SELECT $1::int", 1); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)

	opened := conns[:0]
	for _, conn := range conns {
		if conn != nil {
			opened = append(opened, conn)
		}
	}
	return opened, <-errs
}
