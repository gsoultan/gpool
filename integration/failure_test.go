// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package integration

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
	"github.com/jackc/pgx/v5"
)

// The pool's whole reason for bounding connection lifetime is that servers go
// away: a failover, a restart, a DNS change. Every other test here runs against a
// database that stays up, which means the recovery path — the one that matters
// most in production — was the least exercised code in the repository.
//
// Terminating backends server-side is what a failover looks like from the pool's
// side, and it is better than stopping a container: it is precise about which
// connections die, it needs no control over the runtime, and it runs in
// milliseconds.

// taggedPool returns a pool whose connections are identifiable, so a test can
// terminate its own and nobody else's.
func taggedPool(t *testing.T, tag string, config postgrespool.Config) gpool.Pool {
	t.Helper()

	config.BeforeConnect = func(c *pgx.ConnConfig) error {
		c.RuntimeParams["application_name"] = tag
		return nil
	}
	return newPool(t, config)
}

// terminate kills every backend a pool opened, and reports how many it killed.
func terminate(t *testing.T, tag string) int {
	t.Helper()

	conn, err := pgx.Connect(t.Context(), connString(t))
	if err != nil {
		t.Fatalf("pgx.Connect() = %v", err)
	}
	defer conn.Close(context.Background())

	var killed int
	err = conn.QueryRow(t.Context(), `
SELECT count(*)::int FROM (
    SELECT pg_terminate_backend(pid) FROM pg_stat_activity
    WHERE application_name = $1 AND pid <> pg_backend_pid()
) AS terminated`, tag).Scan(&killed)
	if err != nil {
		t.Fatalf("terminating backends = %v", err)
	}
	return killed
}

// uniqueTag keeps concurrent tests from terminating each other's connections.
func uniqueTag(name string) string {
	return fmt.Sprintf("gpool_%s_%d", name, time.Now().UnixNano()%1_000_000_000)
}

// A pool whose every connection was killed while idle must come back on its own.
//
// It does not come back instantly, and that is the honest contract: a connection
// terminated while idle is not detectably dead without touching it, because
// noticing costs a round trip on every acquire, which is a cost paid forever
// against a failure that is rare. So the first use of each dead connection fails,
// that failure retires it, and the pool refills. What must not happen is a pool
// that stays broken.
func TestPoolRecoversAfterEveryBackendIsTerminated(t *testing.T) {
	tag := uniqueTag("recover")
	pool := taggedPool(t, tag, postgrespool.Config{MaxConns: 4, MinConns: 4, HealthCheckPeriod: -1})

	warmToSteadyState(t, pool, 4)
	if killed := terminate(t, tag); killed == 0 {
		t.Fatal("terminated no backends; the pool had none open")
	}

	// Recovery is measured in attempts rather than asserted on the first one.
	var failures int
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var value int
		if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err == nil {
			break
		}
		failures++
	}
	t.Logf("recovered after %d failed queries", failures)

	// Once back, it has to stay back.
	for i := range 20 {
		var value int
		if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
			t.Fatalf("query %d after recovery = %v", i, err)
		}
	}
	if stat := pool.Stat(); stat.TotalConnections() > 4 {
		t.Errorf("TotalConnections() = %d after recovery, want at most MaxConns (4)", stat.TotalConnections())
	}
}

// With one connection there is nowhere to hide: the pool must retire the dead one
// and dial a replacement rather than handing the same corpse out forever.
func TestPoolReplacesItsOnlyConnectionWhenTerminated(t *testing.T) {
	tag := uniqueTag("single")
	pool := taggedPool(t, tag, postgrespool.Config{MaxConns: 1, HealthCheckPeriod: -1})

	var before int
	if err := pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&before); err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}
	terminate(t, tag)

	var after int
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&after); err == nil {
			break
		}
	}
	if after == 0 {
		t.Fatal("the pool never produced a working connection again")
	}
	if after == before {
		t.Fatalf("still served backend %d, which was terminated", before)
	}
	if stat := pool.Stat(); stat.TotalConnections() > 1 {
		t.Errorf("TotalConnections() = %d, want at most MaxConns (1)", stat.TotalConnections())
	}
}

// Repeated failovers while callers are working must not leak connections, exceed
// the ceiling, or wedge the pool. This is the shape of a database that is flapping
// rather than one that failed once.
func TestPoolSurvivesRepeatedTerminationUnderLoad(t *testing.T) {
	tag := uniqueTag("flapping")
	pool := taggedPool(t, tag, postgrespool.Config{MaxConns: 6, HealthCheckPeriod: -1})
	warmToSteadyState(t, pool, 6)

	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()

	var succeeded, failed atomic.Int64
	var workers sync.WaitGroup

	for range 8 {
		workers.Go(func() {
			for ctx.Err() == nil {
				var value int
				if err := pool.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
					failed.Add(1)
					continue
				}
				succeeded.Add(1)
			}
		})
	}

	// Kill everything, repeatedly, while that runs.
	var rounds int
	for ctx.Err() == nil {
		time.Sleep(750 * time.Millisecond)
		terminate(t, tag)
		rounds++
	}
	workers.Wait()

	t.Logf("%d terminations, %d queries succeeded, %d failed", rounds, succeeded.Load(), failed.Load())

	if succeeded.Load() == 0 {
		t.Fatal("no query ever succeeded; the pool did not recover between failovers")
	}
	// Every failure should be a query that met a dead connection, not a pool that
	// gave up: work has to continue after the last termination.
	stat := pool.Stat()
	if stat.TotalConnections() > 6 {
		t.Errorf("TotalConnections() = %d, want at most MaxConns (6)", stat.TotalConnections())
	}

	healthy, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	for healthy.Err() == nil {
		var value int
		if err := pool.QueryRow(healthy, "SELECT 1").Scan(&value); err == nil {
			return
		}
	}
	t.Fatal("the pool did not recover after the last termination")
}

// A CDC consumer has to survive the same event, and it has more to lose: the
// stream must report the failure rather than hanging, and reconnecting must
// replay from the slot rather than skipping whatever happened in between.
func TestCDCRecoversWhenItsWalsenderIsTerminated(t *testing.T) {
	f := newCDCFixture(t)
	subscriber := f.subscribe(t)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	// Iterated in the background and never broken out of, because leaving the
	// loop closes the stream — which would take the walsender away before there
	// was anything to terminate. Ending has to come from the failure, not from
	// the reader losing interest.
	events := make(chan cdc.Event, 16)
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		for event := range stream.All() {
			select {
			case events <- event:
			default:
			}
		}
	}()

	f.insertRows(t, "before")
	select {
	case <-events:
	case <-time.After(20 * time.Second):
		t.Fatal("no event arrived before the failure was injected")
	}

	control, err := pgx.Connect(t.Context(), connString(t))
	if err != nil {
		t.Fatalf("pgx.Connect() = %v", err)
	}
	defer control.Close(context.Background())

	var killed int
	err = control.QueryRow(t.Context(), `
SELECT count(*)::int FROM (
    SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
    WHERE slot_name = $1 AND active_pid IS NOT NULL
) AS terminated`, f.slot).Scan(&killed)
	if err != nil {
		t.Fatalf("terminating the walsender = %v", err)
	}
	if killed == 0 {
		t.Fatal("the slot had no active walsender, but a stream was open on it")
	}

	// It must end rather than hang. A consumer that never returns from its range
	// loop has no way to learn the stream is gone.
	select {
	case <-ended:
	case <-time.After(30 * time.Second):
		t.Fatal("the stream neither delivered nor ended after its walsender was terminated")
	}
	if err := stream.Err(); err == nil {
		t.Error("the stream ended without reporting why; a consumer cannot tell this from a clean close")
	} else {
		t.Logf("stream reported: %v", err)
	}
	_ = stream.Close()

	// Changes made while nothing was streaming. The slot retained them, so
	// reconnecting must replay them rather than start from the head.
	f.insertRows(t, "during-outage")

	resumed, err := gpool.NewSubscriber(gpool.Postgres, f.config())
	if err != nil {
		t.Fatalf("NewSubscriber() = %v", err)
	}
	t.Cleanup(func() { _ = resumed.Close() })

	again, err := resumed.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() after the failure = %v", err)
	}
	defer again.Close()

	var seen []string
	for _, event := range collect(t, again, 2, 30*time.Second) {
		seen = append(seen, fmt.Sprint(event.After["email"]))
	}
	t.Logf("after reconnecting: %v", seen)
	if !slices.Contains(seen, "during-outage") {
		t.Errorf("the change made during the outage was lost; got %v", seen)
	}
}

// A subscriber pointed at a slot another consumer already holds must say so.
// One slot admits one walsender, and the second consumer silently receiving
// nothing is the failure mode worth naming.
func TestCDCRefusesASecondConsumerOnOneSlot(t *testing.T) {
	f := newCDCFixture(t)

	first := f.subscribe(t)
	stream, err := first.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("first Subscribe() = %v", err)
	}
	defer stream.Close()

	second, err := gpool.NewSubscriber(gpool.Postgres, f.config())
	if err != nil {
		t.Fatalf("NewSubscriber() = %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	if _, err := second.Subscribe(t.Context()); err == nil {
		t.Fatal("a second consumer attached to a slot already in use")
	} else if !strings.Contains(strings.ToLower(err.Error()), "active") &&
		!strings.Contains(strings.ToLower(err.Error()), "in use") {
		t.Logf("second Subscribe() failed as required, with: %v", err)
	}
}

// The recovery contract above is measured in failed queries, which is what a
// caller feels. Lifecycle is the same event counted from the pool's side, and it
// has to distinguish this from a pool that is merely recycling on schedule —
// otherwise a dashboard cannot tell a failover from a Tuesday.
func TestLifecycleCountsTerminatedBackendsAsUnhealthy(t *testing.T) {
	tag := uniqueTag("lifecycle")
	pool := taggedPool(t, tag, postgrespool.Config{MaxConns: 4, MinConns: 4, HealthCheckPeriod: -1})

	warmToSteadyState(t, pool, 4)

	before, ok := pool.Stat().(gpool.Lifecycle)
	if !ok {
		t.Fatal("Stat() does not implement gpool.Lifecycle")
	}
	unhealthyBefore := before.UnhealthyConnections()

	killed := terminate(t, tag)
	if killed == 0 {
		t.Fatal("terminated no backends; the pool had none open")
	}

	// A connection terminated while idle still looks alive until something writes
	// to it, so each one is discovered by the query that meets it. The counter
	// therefore climbs as the pool is used, one per corpse — the same number the
	// recovery contract above states as failed queries, counted from the pool's
	// side instead of the caller's.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var value int
		_ = pool.QueryRow(t.Context(), "SELECT 1").Scan(&value)

		lifecycle := pool.Stat().(gpool.Lifecycle)
		counted := lifecycle.UnhealthyConnections() - unhealthyBefore
		if counted < int64(killed) {
			continue
		}

		t.Logf("%d backends terminated, %d counted unhealthy", killed, counted)
		if got := lifecycle.ExpiredConnections(); got != 0 {
			t.Errorf("ExpiredConnections() = %d, want 0; nothing here reached its lifetime", got)
		}
		if got := lifecycle.EvictedConnections(); got != 0 {
			t.Errorf("EvictedConnections() = %d, want 0; nothing asked for an eviction", got)
		}
		return
	}

	final := pool.Stat().(gpool.Lifecycle).UnhealthyConnections() - unhealthyBefore
	t.Fatalf("%d backends died and %d were counted; the signal a dashboard would alert on is incomplete",
		killed, final)
}
