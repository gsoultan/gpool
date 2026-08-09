// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// Occupancy says how full the pool is and Acquisition says how hard callers are
// competing for it. Neither says why connections are being replaced, and the two
// reasons want opposite responses: a pool that is too small should be grown, and
// one whose connections keep dying should not be. These counters separate them.

func lifecycleOf(t *testing.T, core *Core[*fakeConn]) gpool.Lifecycle {
	t.Helper()

	lifecycle, ok := core.Stat().(gpool.Lifecycle)
	if !ok {
		t.Fatal("Stat() does not implement gpool.Lifecycle")
	}
	return lifecycle
}

// A connection the driver reports dead is discarded on its way out of the pool,
// and that is the count worth alerting on.
func TestLifecycleCountsUnhealthyConnections(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{MaxConns: 1, HealthCheckPeriod: -1})

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	handle.Conn().dead.Store(true)
	handle.Release()

	lifecycle := lifecycleOf(t, core)
	if got := lifecycle.UnhealthyConnections(); got != 1 {
		t.Errorf("UnhealthyConnections() = %d, want 1", got)
	}
	if got := lifecycle.ExpiredConnections(); got != 0 {
		t.Errorf("ExpiredConnections() = %d, want 0; a dead connection is not an expired one", got)
	}
	if got := lifecycle.EvictedConnections(); got != 0 {
		t.Errorf("EvictedConnections() = %d, want 0", got)
	}
}

// A connection that fails the reset returning it to a clean state is as unusable
// as a dead one, and is counted the same way — the caller after it would have
// seen whatever the last one left behind.
func TestLifecycleCountsAFailedResetAsUnhealthy(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{MaxConns: 1, HealthCheckPeriod: -1})

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	handle.Conn().recyclable.Store(false)
	handle.Release()

	if got := lifecycleOf(t, core).UnhealthyConnections(); got != 1 {
		t.Errorf("UnhealthyConnections() = %d, want 1", got)
	}
}

// Reaching MaxConnLifetime is the pool working, not failing. Counting it apart
// from the unhealthy ones is the whole point: a pool recycling on schedule and a
// pool losing connections look identical in every other counter.
//
// The bound has to be tens of milliseconds and the reaper has to be on. Age is
// judged against the cached clock, which only advances every clockResolution, so
// a lifetime shorter than that is not merely quick to expire — it is invisible,
// because nothing ever reads a time later than the one the connection was
// stamped with.
func TestLifecycleCountsExpiredConnections(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{
		MaxConns:          2,
		MaxConnLifetime:   50 * time.Millisecond,
		HealthCheckPeriod: 20 * time.Millisecond,
	})

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	handle.Release()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		lifecycle := lifecycleOf(t, core)
		if lifecycle.ExpiredConnections() > 0 {
			if got := lifecycle.UnhealthyConnections(); got != 0 {
				t.Errorf("UnhealthyConnections() = %d, want 0; expiry is not ill health", got)
			}
			if got := lifecycle.EvictedConnections(); got != 0 {
				t.Errorf("EvictedConnections() = %d, want 0; nothing asked for this", got)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the connection outlived MaxConnLifetime and was never counted as expired")
}

// Eviction is something an operator asked for, so it must not read as either
// damage or expiry.
func TestLifecycleCountsEvictedConnections(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{MaxConns: 4, HealthCheckPeriod: -1})

	var handles []Handle[*fakeConn]
	for range 4 {
		handle, err := core.Acquire(t.Context())
		if err != nil {
			t.Fatalf("Acquire() = %v", err)
		}
		handles = append(handles, handle)
	}
	for _, handle := range handles {
		handle.Release()
	}

	if evicted := core.EvictIdle(); evicted == 0 {
		t.Fatal("EvictIdle() = 0, so nothing was evicted to count")
	}

	lifecycle := lifecycleOf(t, core)
	if got := lifecycle.EvictedConnections(); got != 4 {
		t.Errorf("EvictedConnections() = %d, want 4", got)
	}
	if got := lifecycle.UnhealthyConnections(); got != 0 {
		t.Errorf("UnhealthyConnections() = %d, want 0; eviction is not ill health", got)
	}
	if got := lifecycle.ExpiredConnections(); got != 0 {
		t.Errorf("ExpiredConnections() = %d, want 0", got)
	}
}

// Shrinking the pool closes the surplus as the connections come back, and that
// is the same deliberate act as EvictIdle rather than a connection going wrong.
func TestLifecycleCountsAShrinkAsEviction(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{MaxConns: 4, MaxConnsLimit: 4, HealthCheckPeriod: -1})

	var handles []Handle[*fakeConn]
	for range 4 {
		handle, err := core.Acquire(t.Context())
		if err != nil {
			t.Fatalf("Acquire() = %v", err)
		}
		handles = append(handles, handle)
	}
	if err := core.SetMaxConns(1); err != nil {
		t.Fatalf("SetMaxConns() = %v", err)
	}
	for _, handle := range handles {
		handle.Release()
	}

	lifecycle := lifecycleOf(t, core)
	if got := lifecycle.EvictedConnections(); got != 3 {
		t.Errorf("EvictedConnections() = %d, want 3", got)
	}
	if got := lifecycle.UnhealthyConnections(); got != 0 {
		t.Errorf("UnhealthyConnections() = %d, want 0", got)
	}
}

// Closing the pool is not churn, and counting it as any of the three would put a
// step into every consumer's graph at shutdown for no reason.
func TestLifecycleIgnoresConnectionsClosedWithThePool(t *testing.T) {
	t.Parallel()

	driver := &fakeDriver{}
	core, err := New(driver, Config{MaxConns: 2, HealthCheckPeriod: -1})
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	handle.Release()

	core.Close()

	lifecycle, ok := core.Stat().(gpool.Lifecycle)
	if !ok {
		t.Fatal("Stat() does not implement gpool.Lifecycle")
	}
	total := lifecycle.ExpiredConnections() + lifecycle.UnhealthyConnections() + lifecycle.EvictedConnections()
	if total != 0 {
		t.Errorf("counted %d discards after Close, want 0", total)
	}
	if got := driver.closes.Load(); got == 0 {
		t.Error("the pool closed no connections, so this proved nothing")
	}
}
