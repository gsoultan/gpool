// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// openNow counts connections the driver has opened and not yet closed.
func openNow(d *fakeDriver) int {
	open := 0
	for _, c := range d.connections() {
		if c.closed.Load() == 0 {
			open++
		}
	}
	return open
}

func TestSetMaxConns_GrowsAndShrinksTheCeiling(t *testing.T) {
	core, _ := newTestCore(t, Config{MaxConns: 2, MaxConnsLimit: 8, HealthCheckPeriod: -1})

	if got := core.MaxConns(); got != 2 {
		t.Fatalf("initial MaxConns = %d, want 2", got)
	}

	if err := core.SetMaxConns(5); err != nil {
		t.Fatalf("SetMaxConns(5): %v", err)
	}
	if got := core.MaxConns(); got != 5 {
		t.Errorf("after growing, MaxConns = %d, want 5", got)
	}
	if got := core.Stat().MaxConnections(); got != 5 {
		t.Errorf("Stat reports %d, want 5 — Stat must follow the live ceiling", got)
	}

	if err := core.SetMaxConns(1); err != nil {
		t.Fatalf("SetMaxConns(1): %v", err)
	}
	if got := core.MaxConns(); got != 1 {
		t.Errorf("after shrinking, MaxConns = %d, want 1", got)
	}
}

// Growing must actually let more callers through, not just move a number.
func TestSetMaxConns_GrowthReleasesWaiters(t *testing.T) {
	core, _ := newTestCore(t, Config{MaxConns: 1, MaxConnsLimit: 4, HealthCheckPeriod: -1})

	first, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	defer first.Release()

	// The pool is full, so this must not succeed yet.
	tight, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	if _, err := core.Acquire(tight); err == nil {
		cancel()
		t.Fatal("acquired a second connection at MaxConns=1")
	}
	cancel()

	if err := core.SetMaxConns(3); err != nil {
		t.Fatalf("SetMaxConns(3): %v", err)
	}

	second, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire after growing: %v", err)
	}
	second.Release()
}

// Shrinking below the number currently checked out must not block and must not
// steal a connection from a caller that is using it.
func TestSetMaxConns_ShrinkIsNonBlockingAndDeferred(t *testing.T) {
	core, driver := newTestCore(t, Config{MaxConns: 4, MaxConnsLimit: 4, HealthCheckPeriod: -1})

	var handles []Handle[*fakeConn]
	for range 4 {
		h, err := core.Acquire(t.Context())
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		handles = append(handles, h)
	}

	done := make(chan error, 1)
	go func() { done <- core.SetMaxConns(1) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetMaxConns while fully checked out: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SetMaxConns blocked while connections were checked out")
	}

	for i := range handles {
		handles[i].Release()
	}

	// The deferred shrink is paid by those releases: the pool must settle at or
	// below the new ceiling rather than keeping four connections alive.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if openNow(driver) <= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := openNow(driver); got > 1 {
		t.Errorf("%d connections still open after shrinking to 1", got)
	}

	// And the ceiling must still be honoured afterwards.
	h, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire after shrink: %v", err)
	}
	defer h.Release()

	tight, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := core.Acquire(tight); err == nil {
		t.Error("acquired a second connection after shrinking to MaxConns=1")
	}
}

func TestSetMaxConns_RejectsOutOfRange(t *testing.T) {
	core, _ := newTestCore(t, Config{MaxConns: 4, MinConns: 2, MaxConnsLimit: 8, HealthCheckPeriod: -1})

	if err := core.SetMaxConns(1); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("below MinConns: got %v, want ErrInvalidConfig", err)
	}
	if err := core.SetMaxConns(9); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("above MaxConnsLimit: got %v, want ErrInvalidConfig", err)
	}
	// Capacity is fixed unless headroom was asked for deliberately.
	fixed, _ := newTestCore(t, Config{MaxConns: 4, HealthCheckPeriod: -1})
	if err := fixed.SetMaxConns(5); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("growing without MaxConnsLimit: got %v, want ErrInvalidConfig", err)
	}
}

func TestSetMaxConns_AfterCloseReportsClosed(t *testing.T) {
	core, _ := newTestCore(t, Config{MaxConns: 2, MaxConnsLimit: 4, HealthCheckPeriod: -1})
	core.Close()

	if err := core.SetMaxConns(3); !errors.Is(err, ErrClosed) {
		t.Errorf("got %v, want ErrClosed", err)
	}
}

// The ceiling must hold under concurrent load while it is being moved, which is
// the case a check-then-act implementation gets wrong.
func TestSetMaxConns_CeilingHoldsUnderConcurrentResize(t *testing.T) {
	const ceiling = 8
	core, driver := newTestCore(t, Config{MaxConns: 2, MaxConnsLimit: ceiling, HealthCheckPeriod: -1})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for range 32 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				h, err := core.Acquire(ctx)
				cancel()
				if err == nil {
					if open := openNow(driver); open > ceiling {
						t.Errorf("%d connections open, exceeds hard ceiling %d", open, ceiling)
					}
					h.Release()
				}
			}
		})
	}

	for range 50 {
		_ = core.SetMaxConns(2)
		_ = core.SetMaxConns(ceiling)
	}
	close(stop)
	wg.Wait()
}

func TestEvictIdle_ClosesIdleAndSparesCheckedOut(t *testing.T) {
	core, driver := newTestCore(t, Config{MaxConns: 4, MaxConnsLimit: 4, HealthCheckPeriod: -1})

	var handles []Handle[*fakeConn]
	for range 3 {
		h, err := core.Acquire(t.Context())
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		handles = append(handles, h)
	}
	handles[0].Release()
	handles[1].Release() // two idle, one still out

	evicted := core.EvictIdle()
	if evicted != 2 {
		t.Errorf("EvictIdle returned %d, want 2", evicted)
	}
	if got := core.Stat().IdleConnections(); got != 0 {
		t.Errorf("%d idle connections survived EvictIdle", got)
	}
	if got := openNow(driver); got != 1 {
		t.Errorf("%d connections open, want 1 (the checked-out one)", got)
	}

	// The connection that was in use must still work and still be releasable.
	handles[2].Release()
	if got := core.Stat().TotalConnections(); got > 1 {
		t.Errorf("total connections = %d after releasing the last one", got)
	}
}

func TestEvictIdle_AfterCloseIsZero(t *testing.T) {
	core, _ := newTestCore(t, Config{MaxConns: 2, HealthCheckPeriod: -1})
	core.Close()

	if got := core.EvictIdle(); got != 0 {
		t.Errorf("EvictIdle after Close returned %d, want 0", got)
	}
}

// Deriving active as total-idle samples two counters independently, so it reads
// high while the background warm-up is mid-flight.
func TestStat_ActiveIsExactAgainstWarmUp(t *testing.T) {
	core, _ := newTestCore(t, Config{MaxConns: 8, MinConns: 4, HealthCheckPeriod: -1})

	for range 200 {
		if got := core.Stat().ActiveConnections(); got != 0 {
			t.Fatalf("active = %d with nothing checked out (warm-up counted as active)", got)
		}
		time.Sleep(time.Millisecond)
	}

	h, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if got := core.Stat().ActiveConnections(); got != 1 {
		t.Errorf("active = %d with one checked out, want 1", got)
	}
	h.Release()
	if got := core.Stat().ActiveConnections(); got != 0 {
		t.Errorf("active = %d after release, want 0", got)
	}
}

func TestStat_WaitingAcquiresIsAGauge(t *testing.T) {
	core, _ := newTestCore(t, Config{MaxConns: 1, HealthCheckPeriod: -1})

	held, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if got := core.Stat().WaitingAcquires(); got != 0 {
		t.Errorf("waiting = %d with nobody blocked", got)
	}

	const waiters = 5
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range waiters {
		wg.Go(func() {
			if h, err := core.Acquire(ctx); err == nil {
				h.Release()
			}
		})
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && core.Stat().WaitingAcquires() < waiters {
		time.Sleep(5 * time.Millisecond)
	}
	if got := core.Stat().WaitingAcquires(); got != waiters {
		t.Errorf("waiting = %d, want %d", got, waiters)
	}

	cancel()
	wg.Wait()
	held.Release()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && core.Stat().WaitingAcquires() != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := core.Stat().WaitingAcquires(); got != 0 {
		t.Errorf("waiting = %d after everyone gave up, want 0 — the gauge leaked", got)
	}
}
