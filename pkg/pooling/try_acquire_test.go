// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"testing"
)

// TryAcquire is for callers that cannot park. These pin the three things such a
// caller relies on: a miss costs nothing, a miss never dials, and a hit is an
// ordinary checkout that Release returns.

func TestTryAcquireMissesOnAnEmptyPoolWithoutDialing(t *testing.T) {
	core, driver := newTestCore(t, Config{MaxConns: 2})

	// There is room to grow, and that is exactly the case that must not be taken:
	// dialing is a network round trip, and the caller asked not to wait.
	if _, ok := core.TryAcquire(); ok {
		t.Fatal("TryAcquire() on an empty pool = hit, want miss")
	}
	if got := driver.connects.Load(); got != 0 {
		t.Fatalf("TryAcquire() dialed %d connections, want 0", got)
	}

	// The permit taken while probing must have come back, or two misses would
	// leave a pool of two unable to serve anyone.
	for range 3 {
		core.TryAcquire()
	}
	first, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() after misses = %v", err)
	}
	second, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("second Acquire() after misses = %v", err)
	}
	first.Release()
	second.Release()
}

func TestTryAcquireTakesAnIdleConnection(t *testing.T) {
	core, driver := newTestCore(t, Config{MaxConns: 1})

	seeded, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	want := seeded.Conn()
	seeded.Release()

	handle, ok := core.TryAcquire()
	if !ok {
		t.Fatal("TryAcquire() with an idle connection = miss, want hit")
	}
	if handle.Conn() != want {
		t.Fatalf("TryAcquire() = connection %d, want the idle one %d", handle.Conn().id, want.id)
	}
	if got := core.Stat().ActiveConnections(); got != 1 {
		t.Fatalf("ActiveConnections() = %d, want 1", got)
	}

	// MaxConns is 1, so a second attempt proves the first is a real checkout.
	if _, ok := core.TryAcquire(); ok {
		t.Fatal("TryAcquire() while the only connection is out = hit, want miss")
	}

	handle.Release()
	again, ok := core.TryAcquire()
	if !ok {
		t.Fatal("TryAcquire() after Release = miss, want hit")
	}
	again.Release()

	if got := driver.connects.Load(); got != 1 {
		t.Fatalf("connects = %d, want 1", got)
	}
}

func TestTryAcquireDestroysADeadConnectionAndMisses(t *testing.T) {
	core, driver := newTestCore(t, Config{MaxConns: 1})

	seeded, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	conn := seeded.Conn()
	seeded.Release()
	conn.dead.Store(true)

	if _, ok := core.TryAcquire(); ok {
		t.Fatal("TryAcquire() = hit on a dead connection, want miss")
	}
	if got := driver.closes.Load(); got != 1 {
		t.Fatalf("closes = %d, want the dead connection destroyed", got)
	}
	if got := core.Stat().TotalConnections(); got != 0 {
		t.Fatalf("TotalConnections() = %d, want 0", got)
	}
}

func TestTryAcquireAfterCloseMisses(t *testing.T) {
	core, _ := newTestCore(t, Config{MaxConns: 1})
	core.Close()

	if _, ok := core.TryAcquire(); ok {
		t.Fatal("TryAcquire() after Close = hit, want miss")
	}
}
