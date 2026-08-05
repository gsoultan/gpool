// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package benchmarks

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

// targetClients is the client-side concurrency a pooler is expected to absorb.
// The point of a pooler is that this number is decoupled from the number of
// backend connections, so these benchmarks hold the pool small and vary the callers.
const targetClients = 5000

// parallelismFor converts a target goroutine count into the multiplier RunParallel
// wants, which is per-P rather than absolute.
func parallelismFor(clients int) int {
	return max(1, clients/runtime.GOMAXPROCS(0))
}

// BenchmarkScaleAcquireRelease isolates the pool's own contention: no query, no
// network, just the semaphore and the shard probe under heavy client concurrency.
// This is where a single global lock in the acquire path would show up.
func BenchmarkScaleAcquireRelease(b *testing.B) {
	for _, maxConns := range []int32{16, 64, 256} {
		b.Run(fmt.Sprintf("maxconns=%d", maxConns), func(b *testing.B) {
			pool := newGpool(b, postgrespool.Config{MaxConns: maxConns, MinConns: maxConns})
			warmPool(b, pool, int(maxConns))

			b.SetParallelism(parallelismFor(targetClients))
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
		})
	}
}

// BenchmarkScaleQuery is the end-to-end shape: many more callers than connections,
// every caller doing real work. Throughput here is bounded by the backend, so what
// this measures is whether the pool adds latency or allocations on the way.
func BenchmarkScaleQuery(b *testing.B) {
	for _, maxConns := range []int32{16, 64, 256} {
		b.Run(fmt.Sprintf("maxconns=%d", maxConns), func(b *testing.B) {
			pool := newGpool(b, postgrespool.Config{MaxConns: maxConns, MinConns: maxConns / 2})

			b.SetParallelism(parallelismFor(targetClients))
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
		})
	}
}

// BenchmarkScaleStatUnderLoad checks that scraping metrics does not serialise
// against the acquire path. Stat is called far more often than it looks in a
// system with a 1s Prometheus scrape and many pools.
func BenchmarkScaleStatUnderLoad(b *testing.B) {
	pool := newGpool(b, postgrespool.Config{MaxConns: 64, MinConns: 64})
	warmPool(b, pool, 64)

	stop := make(chan struct{})
	var load sync.WaitGroup
	for range 64 {
		load.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				conn, err := pool.Acquire(context.Background())
				if err != nil {
					return
				}
				conn.Release()
			}
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = pool.Stat()
	}
	b.StopTimer()

	close(stop)
	load.Wait()
}

// TestMemoryPerPooledConnection reports the steady-state heap cost of a pooled
// connection so the footprint at a target connection count can be projected
// without opening that many backends.
func TestMemoryPerPooledConnection(t *testing.T) {
	connString := requireDatabase(t)

	const conns = 64

	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: connString,
		MaxConns:   conns,
	})
	if err != nil {
		t.Fatalf("NewPool() = %v", err)
	}
	defer pool.Close()

	before := heapInUse()

	// Hold every connection open at once so all of them are established, then
	// release them so the measurement covers the idle steady state.
	held := make([]gpool.Conn, 0, conns)
	for range conns {
		conn, err := pool.Acquire(t.Context())
		if err != nil {
			t.Fatalf("Acquire() = %v", err)
		}
		held = append(held, conn)
	}
	for _, conn := range held {
		conn.Release()
	}

	if got := pool.Stat().TotalConnections(); got != conns {
		t.Fatalf("TotalConnections() = %d, want %d", got, conns)
	}

	after := heapInUse()
	perConn := (after - before) / conns

	t.Logf("heap in use: %.1f MiB -> %.1f MiB for %d pooled connections",
		float64(before)/(1<<20), float64(after)/(1<<20), conns)
	t.Logf("per pooled connection: %.1f KiB", float64(perConn)/1024)
	t.Logf("projected at %d connections: %.1f MiB", targetClients,
		float64(perConn)*targetClients/(1<<20))
}

// TestGoroutineCostAtScale confirms the pool adds no goroutine per client and no
// goroutine per connection: one background maintainer is the whole cost.
func TestGoroutineCostAtScale(t *testing.T) {
	connString := requireDatabase(t)

	baseline := stableGoroutines()

	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: connString,
		MaxConns:   64,
		MinConns:   32,
	})
	if err != nil {
		t.Fatalf("NewPool() = %v", err)
	}

	// Drive real traffic so connections are established and released.
	var wg sync.WaitGroup
	for range 512 {
		wg.Go(func() {
			var value int
			_ = pool.QueryRow(context.Background(), "SELECT 1").Scan(&value)
		})
	}
	wg.Wait()

	withPool := stableGoroutines()
	overhead := withPool - baseline

	t.Logf("goroutines: %d baseline -> %d with a warm pool (overhead %d)", baseline, withPool, overhead)

	// One maintainer, plus slack for the runtime and the driver's own bookkeeping.
	if overhead > 8 {
		t.Errorf("pool added %d goroutines, want a small constant independent of connection count", overhead)
	}

	pool.Close()

	afterClose := stableGoroutines()
	t.Logf("goroutines after Close: %d", afterClose)
	if afterClose > baseline+2 {
		t.Errorf("Close left %d goroutines behind", afterClose-baseline)
	}
}

func warmPool(tb testing.TB, pool gpool.Pool, count int) {
	tb.Helper()

	held := make([]gpool.Conn, 0, count)
	for range count {
		conn, err := pool.Acquire(context.Background())
		if err != nil {
			tb.Fatalf("warming the pool: %v", err)
		}
		held = append(held, conn)
	}
	for _, conn := range held {
		conn.Release()
	}
}

func heapInUse() uint64 {
	// Two collections: the first frees garbage, the second settles what the first
	// made unreachable, so the reading is steady state rather than peak.
	runtime.GC()
	runtime.GC()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapInuse
}

// stableGoroutines waits for the count to stop moving, so a goroutine that is
// mid-teardown is not miscounted as a leak.
func stableGoroutines() int {
	previous := -1
	for range 50 {
		runtime.GC()
		current := runtime.NumGoroutine()
		if current == previous {
			return current
		}
		previous = current
		time.Sleep(20 * time.Millisecond)
	}
	return runtime.NumGoroutine()
}
