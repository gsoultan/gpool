// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	postgrescdc "github.com/gsoultan/gpool/pkg/vendors/postgres/cdc"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

// defaultSoakDuration keeps the default run short enough to sit in a normal test
// pass. Raise it with GPOOL_SOAK_DURATION to actually stress something:
//
//	GPOOL_SOAK_DURATION=30m go test -run Soak -timeout 40m ./integration/
const defaultSoakDuration = 20 * time.Second

// soakDuration resolves how long a soak test should run.
func soakDuration(t *testing.T) time.Duration {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	raw := os.Getenv("GPOOL_SOAK_DURATION")
	if raw == "" {
		return defaultSoakDuration
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("GPOOL_SOAK_DURATION=%q is not a duration: %v", raw, err)
	}
	return parsed
}

// warmToSteadyState establishes every connection the pool is allowed, then returns
// them, so a later goroutine reading is not confused by the pool still filling up.
func warmToSteadyState(t *testing.T, pool gpool.Pool, count int) {
	t.Helper()

	held := make([]gpool.Conn, 0, count)
	for range count {
		conn, err := pool.Acquire(t.Context())
		if err != nil {
			t.Fatalf("warming the pool: %v", err)
		}
		held = append(held, conn)
	}
	for _, conn := range held {
		conn.Release()
	}
}

// sample is one periodic reading of the things a leak would move.
type sample struct {
	at         time.Time
	goroutines int
	heapMiB    float64
	conns      int32
}

func takeSample(pool gpool.Pool) sample {
	runtime.GC()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	s := sample{
		at:         time.Now(),
		goroutines: runtime.NumGoroutine(),
		heapMiB:    float64(stats.HeapInuse) / (1 << 20),
	}
	if pool != nil {
		s.conns = pool.Stat().TotalConnections()
	}
	return s
}

// takeSettledSample waits for the goroutine count to stop moving before reading it.
//
// pgx starts helper goroutines around I/O — a background reader and a context
// watcher per in-flight operation — and they unwind shortly after the call that
// spawned them returns, not synchronously with it. Sampling immediately counts
// that unwinding as a leak: measured here as a phantom +4 that settled to 0 once
// given time, and did not grow when the run was doubled. A leak keeps growing; a
// transient settles, and waiting is what tells the two apart.
func takeSettledSample(pool gpool.Pool) sample {
	previous, stable := -1, 0
	for range 100 {
		runtime.GC()

		current := runtime.NumGoroutine()
		if current == previous {
			stable++
			if stable >= 3 {
				break
			}
		} else {
			stable = 0
		}
		previous = current
		time.Sleep(20 * time.Millisecond)
	}
	return takeSample(pool)
}

// A leak does not show up in a test that finishes in milliseconds. This one holds
// sustained load and watches whether goroutines, heap, or connections drift.
func TestSoakSustainedQueryLoad(t *testing.T) {
	duration := soakDuration(t)
	pool := newPool(t, postgrespool.Config{
		MaxConns:          16,
		MinConns:          4,
		MaxConnLifetime:   2 * time.Second,
		MaxConnIdleTime:   time.Second,
		HealthCheckPeriod: 500 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()

	var queries, failures atomic.Int64
	var workers sync.WaitGroup

	for range 64 {
		workers.Go(func() {
			for ctx.Err() == nil {
				var value int
				if err := pool.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
					// Cancellation at the end of the run is expected, not a failure.
					if ctx.Err() == nil {
						failures.Add(1)
					}
					continue
				}
				queries.Add(1)
			}
		})
	}

	// Let the pool reach steady state before the baseline reading, so connection
	// establishment is not mistaken for growth.
	time.Sleep(2 * time.Second)
	baseline := takeSettledSample(pool)

	var samples []sample
	ticker := time.NewTicker(max(duration/10, time.Second))
	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			samples = append(samples, takeSample(pool))
		case <-ctx.Done():
		}
	}
	ticker.Stop()
	workers.Wait()

	final := takeSettledSample(pool)

	t.Logf("ran %s: %d queries, %d failures", duration, queries.Load(), failures.Load())
	t.Logf("baseline: %d goroutines, %.1f MiB heap, %d conns",
		baseline.goroutines, baseline.heapMiB, baseline.conns)
	for _, s := range samples {
		t.Logf("  +%-6s %d goroutines, %.1f MiB heap, %d conns",
			s.at.Sub(baseline.at).Round(time.Second), s.goroutines, s.heapMiB, s.conns)
	}
	t.Logf("final:    %d goroutines, %.1f MiB heap, %d conns",
		final.goroutines, final.heapMiB, final.conns)

	if failures.Load() > 0 {
		t.Errorf("%d queries failed while the pool was healthy", failures.Load())
	}
	if queries.Load() == 0 {
		t.Fatal("no queries completed; the soak proved nothing")
	}

	// Connection lifetime is 2s and the run is far longer, so every connection has
	// been retired and replaced several times. The count must still be bounded.
	if final.conns > 16 {
		t.Errorf("TotalConnections() = %d after churn, want at most MaxConns (16)", final.conns)
	}

	// Goroutines are the clearest leak signal: the pool's own cost is one, and
	// worker goroutines have all returned by now.
	if drift := final.goroutines - baseline.goroutines; drift > 4 {
		t.Errorf("goroutines grew by %d over the run", drift)
	}
}

// churnInterval paces pool creation.
//
// Without it this loop builds and tears down pools as fast as the machine allows —
// measured at ~130 per second — and every closed connection holds an ephemeral
// port in TIME_WAIT for tens of seconds afterwards. That exhausts the host's port
// range within one run and makes every later connection fail with
// "can't assign requested address", including ones belonging to other processes.
//
// The same applies in production: a pool is a long-lived object, and building one
// per request or per tenant-hit will exhaust ports long before it exhausts anything
// else. This paces to a rate a real host would actually produce.
const churnInterval = 50 * time.Millisecond

// Churning pools is what a multi-tenant host does. A pool that does not fully
// release on Close shows up here and nowhere else.
func TestSoakPoolChurn(t *testing.T) {
	duration := soakDuration(t)
	connString := connString(t)

	baseline := takeSettledSample(nil)
	deadline := time.Now().Add(duration)

	var cycles int
	for time.Now().Before(deadline) {
		pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
			ConnString: connString,
			MaxConns:   4,
			MinConns:   2,
		})
		if err != nil {
			t.Fatalf("cycle %d: NewPool() = %v", cycles, err)
		}

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				var value int
				_ = pool.QueryRow(context.Background(), "SELECT 1").Scan(&value)
			})
		}
		wg.Wait()
		pool.Close()
		cycles++

		time.Sleep(churnInterval)
	}

	final := takeSettledSample(nil)

	t.Logf("%d pool lifecycles in %s", cycles, duration)
	t.Logf("goroutines: %d -> %d, heap: %.1f -> %.1f MiB",
		baseline.goroutines, final.goroutines, baseline.heapMiB, final.heapMiB)

	if cycles == 0 {
		t.Fatal("no pool lifecycles completed")
	}
	// Every pool started a maintainer goroutine; every Close must have reclaimed it.
	if drift := final.goroutines - baseline.goroutines; drift > 4 {
		t.Errorf("goroutines grew by %d across %d pool lifecycles", drift, cycles)
	}
}

// The abnormal paths are the ones that leak. This holds sustained load while
// abandoning transactions, subscriptions, and unread rows.
func TestSoakAbusePaths(t *testing.T) {
	duration := soakDuration(t)
	pool := newPool(t, postgrespool.Config{MaxConns: 8, MinConns: 2})

	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()

	// The baseline is taken with the pool already at steady state, so establishing
	// connections is not counted as growth.
	warmToSteadyState(t, pool, 8)
	baseline := takeSettledSample(pool)

	var cycles atomic.Int64
	var workers sync.WaitGroup

	for worker := range 8 {
		workers.Go(func() {
			for ctx.Err() == nil {
				conn, err := pool.Acquire(ctx)
				if err != nil {
					continue
				}

				switch worker % 4 {
				case 0:
					// Abandon an open transaction.
					_, _ = conn.Begin(ctx)
				case 1:
					// Abandon a failed transaction.
					if _, err := conn.Begin(ctx); err == nil {
						_, _ = conn.Exec(ctx, "SELECT * FROM no_such_table")
					}
				case 2:
					// Abandon a subscription.
					if notifier, ok := conn.(gpool.Notifier); ok {
						_ = notifier.Listen(ctx, fmt.Sprintf("soak_%d", worker))
					}
				case 3:
					// Leave rows unread.
					if rows, err := conn.Query(ctx, "SELECT generate_series(1, 100)"); err == nil {
						rows.Next()
						rows.Close()
					}
				}

				conn.Release()
				cycles.Add(1)
			}
		})
	}
	workers.Wait()

	final := takeSettledSample(pool)

	t.Logf("%d abuse cycles in %s", cycles.Load(), duration)
	t.Logf("goroutines: %d -> %d, conns: %d -> %d",
		baseline.goroutines, final.goroutines, baseline.conns, final.conns)

	if cycles.Load() == 0 {
		t.Fatal("no cycles completed")
	}
	if final.conns > 8 {
		t.Errorf("TotalConnections() = %d, want at most MaxConns (8)", final.conns)
	}
	// Both readings are at steady state, so any growth here is gpool's own.
	if drift := final.goroutines - baseline.goroutines; drift > 2 {
		t.Errorf("goroutines grew by %d under abuse (baseline %d, final %d)",
			drift, baseline.goroutines, final.goroutines)
	}

	// The pool must still be usable after all of that.
	healthy, cancelHealthy := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelHealthy()

	var value int
	if err := pool.QueryRow(healthy, "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("the pool did not survive the abuse: %v", err)
	}
}

// A replication slot retains WAL until its consumer confirms a position. A stream
// that runs for a while must keep the slot moving rather than pinning it.
func TestSoakCDCStreamAdvancesTheSlot(t *testing.T) {
	duration := soakDuration(t)
	f := newCDCFixture(t)

	subscriber, err := gpool.NewSubscriber(gpool.Postgres, f.config())
	if err != nil {
		t.Fatalf("NewSubscriber() = %v", err)
	}
	t.Cleanup(func() { _ = subscriber.Close() })

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()

	var received atomic.Int64
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for event := range stream.All() {
			if event.LSN != 0 {
				received.Add(1)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Generate changes for the first half, then go quiet. The quiet half is the
	// point: an idle publication used to pin WAL because the confirmed position
	// only ever advanced on a row change.
	writeUntil := time.Now().Add(duration / 2)
	for time.Now().Before(writeUntil) && ctx.Err() == nil {
		_, _ = f.pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (email) VALUES ($1)", f.table), "soak@example.com")
		time.Sleep(50 * time.Millisecond)
	}

	<-ctx.Done()
	_ = stream.Close()
	<-consumed

	lag, err := slotRetainedBytes(f, t)
	if err != nil {
		t.Fatalf("reading slot lag = %v", err)
	}

	t.Logf("%d events over %s, slot retaining %d bytes of WAL at the end", received.Load(), duration, lag)

	if received.Load() == 0 {
		t.Fatal("no events received; the soak proved nothing")
	}
	if err := stream.Err(); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("stream ended with %v", err)
	}

	// 32 MiB is generous: it is the point past which the slot is clearly not
	// keeping up rather than a precise threshold.
	const maxRetained = 32 << 20
	if lag > maxRetained {
		t.Errorf("slot is retaining %d bytes of WAL, want under %d - the confirmed position is not advancing", lag, maxRetained)
	}
}

// slotRetainedBytes reports how much WAL the fixture's slot is holding back.
func slotRetainedBytes(f *cdcFixture, t *testing.T) (int64, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var lag int64
	err := f.pool.QueryRow(ctx,
		`SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
		 FROM pg_replication_slots WHERE slot_name = $1`, f.slot).Scan(&lag)
	return lag, err
}

// Compile-time reminder that the CDC soak uses the shared event type.
var _ = cdc.Event{}

// Compile-time reminder that the CDC soak uses the vendor config type.
var _ = postgrescdc.Config{}
