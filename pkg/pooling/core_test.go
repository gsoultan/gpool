// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn is a scripted driver connection.
type fakeConn struct {
	id         int
	dead       atomic.Bool
	recyclable atomic.Bool
	closed     atomic.Int32
}

// fakeDriver records what the engine asked of it.
type fakeDriver struct {
	connects   atomic.Int32
	closes     atomic.Int32
	cleanups   atomic.Int32
	connectErr error

	mu   sync.Mutex
	live []*fakeConn
}

var _ Driver[*fakeConn] = (*fakeDriver)(nil)

func (d *fakeDriver) Connect(context.Context) (*fakeConn, error) {
	if d.connectErr != nil {
		return nil, d.connectErr
	}

	c := &fakeConn{id: int(d.connects.Add(1))}
	c.recyclable.Store(true)

	d.mu.Lock()
	d.live = append(d.live, c)
	d.mu.Unlock()
	return c, nil
}

func (d *fakeDriver) Close(_ context.Context, conn *fakeConn) error {
	d.closes.Add(1)
	conn.closed.Add(1)
	return nil
}

func (d *fakeDriver) Dead(conn *fakeConn) bool {
	return conn.dead.Load()
}

func (d *fakeDriver) Recyclable(_ context.Context, conn *fakeConn) bool {
	d.cleanups.Add(1)
	return conn.recyclable.Load()
}

func (d *fakeDriver) connections() []*fakeConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*fakeConn(nil), d.live...)
}

func newTestCore(t *testing.T, config Config) (*Core[*fakeConn], *fakeDriver) {
	t.Helper()

	driver := &fakeDriver{}
	core, err := New(driver, config)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(core.Close)
	return core, driver
}

func TestCoreRejectsBadConstruction(t *testing.T) {
	t.Parallel()

	if _, err := New[*fakeConn](nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("New(nil driver) = %v, want ErrInvalidConfig", err)
	}
	if _, err := New(&fakeDriver{}, Config{MaxConns: 2, MinConns: 5}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("New(MinConns > MaxConns) = %v, want ErrInvalidConfig", err)
	}
}

func TestCoreReusesConnections(t *testing.T) {
	t.Parallel()

	core, driver := newTestCore(t, Config{MaxConns: 2})

	for range 10 {
		handle, err := core.Acquire(t.Context())
		if err != nil {
			t.Fatalf("Acquire() = %v", err)
		}
		handle.Release()
	}

	// One connection served every acquisition.
	if got := driver.connects.Load(); got != 1 {
		t.Errorf("driver dialled %d times, want 1", got)
	}
	if got := driver.cleanups.Load(); got != 10 {
		t.Errorf("driver cleaned up %d times, want 10", got)
	}
}

func TestCoreRespectsMaxConns(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{MaxConns: 2})

	first, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	second, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := core.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire() beyond MaxConns = %v, want a deadline", err)
	}

	first.Release()
	second.Release()

	if got := core.Stat().TotalConnections(); got > 2 {
		t.Errorf("TotalConnections() = %d, want at most 2", got)
	}
}

// The engine must never hand on a connection the driver refuses to vouch for.
func TestCoreDiscardsUnrecyclableConnections(t *testing.T) {
	t.Parallel()

	core, driver := newTestCore(t, Config{MaxConns: 1})

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	handle.Conn().recyclable.Store(false)
	handle.Release()

	if got := driver.closes.Load(); got != 1 {
		t.Errorf("driver closed %d connections, want 1", got)
	}
	if got := core.Stat().TotalConnections(); got != 0 {
		t.Errorf("TotalConnections() = %d, want 0", got)
	}

	// The pool recovers by dialling a fresh one.
	next, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() after a discard = %v", err)
	}
	next.Release()

	if got := driver.connects.Load(); got != 2 {
		t.Errorf("driver dialled %d times, want 2", got)
	}
}

func TestCoreDiscardsDeadConnections(t *testing.T) {
	t.Parallel()

	core, driver := newTestCore(t, Config{MaxConns: 1})

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	handle.Conn().dead.Store(true)
	handle.Release()

	// A dead connection is not worth asking the driver to clean up.
	if got := driver.cleanups.Load(); got != 0 {
		t.Errorf("driver was asked to clean up a dead connection %d times, want 0", got)
	}
	if got := core.Stat().TotalConnections(); got != 0 {
		t.Errorf("TotalConnections() = %d, want 0", got)
	}
}

func TestHandleReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	core, driver := newTestCore(t, Config{MaxConns: 1})

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	if handle.Released() {
		t.Error("a fresh handle should not report released")
	}

	handle.Release()
	handle.Release()
	handle.Release()

	if !handle.Released() {
		t.Error("Released() should report true after Release")
	}
	// A doubled release would have cleaned up twice and freed a permit it never held.
	if got := driver.cleanups.Load(); got != 1 {
		t.Errorf("driver cleaned up %d times, want 1", got)
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	again, err := core.Acquire(ctx)
	if err != nil {
		t.Fatalf("the permit was not returned: %v", err)
	}
	again.Release()

	exhausted, cancelExhausted := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelExhausted()
	if _, err := core.Acquire(exhausted); err != nil {
		t.Fatal("capacity should still be exactly one permit")
	}
}

func TestCoreAcquireAfterCloseFailsFast(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{MaxConns: 2})
	core.Close()

	if _, err := core.Acquire(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire() = %v, want ErrClosed", err)
	}
}

func TestCoreCloseIsIdempotentAndClosesIdle(t *testing.T) {
	t.Parallel()

	core, driver := newTestCore(t, Config{MaxConns: 4})

	handles := make([]*Handle[*fakeConn], 0, 3)
	for range 3 {
		handle, err := core.Acquire(t.Context())
		if err != nil {
			t.Fatalf("Acquire() = %v", err)
		}
		handles = append(handles, handle)
	}
	for _, handle := range handles {
		handle.Release()
	}

	core.Close()
	core.Close()

	if got := driver.closes.Load(); got != 3 {
		t.Errorf("driver closed %d connections, want 3", got)
	}
	if got := core.Stat().TotalConnections(); got != 0 {
		t.Errorf("TotalConnections() = %d, want 0", got)
	}
}

// A connection still checked out when Close runs is discarded by whoever
// releases it, rather than pooled into a closed pool.
func TestCoreReleaseAfterCloseDestroys(t *testing.T) {
	t.Parallel()

	core, driver := newTestCore(t, Config{MaxConns: 2})

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}

	core.Close()
	handle.Release()

	if got := driver.closes.Load(); got != 1 {
		t.Errorf("driver closed %d connections, want 1", got)
	}
	if got := core.Stat().IdleConnections(); got != 0 {
		t.Errorf("IdleConnections() = %d, want 0", got)
	}
}

func TestCoreStatReportsAcquisitions(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{MaxConns: 1})

	handle, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}

	stat := core.Stat()
	if stat.AcquireCount() != 1 {
		t.Errorf("AcquireCount() = %d, want 1", stat.AcquireCount())
	}
	if stat.MaxConnections() != 1 {
		t.Errorf("MaxConnections() = %d, want 1", stat.MaxConnections())
	}
	if stat.ActiveConnections() != 1 {
		t.Errorf("ActiveConnections() = %d, want 1", stat.ActiveConnections())
	}

	timedOut, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, _ = core.Acquire(timedOut)

	if got := core.Stat().CanceledAcquireCount(); got != 1 {
		t.Errorf("CanceledAcquireCount() = %d, want 1", got)
	}
	handle.Release()
}

func TestCoreWarmsUpToMinConns(t *testing.T) {
	t.Parallel()

	core, driver := newTestCore(t, Config{MaxConns: 8, MinConns: 4, HealthCheckPeriod: 20 * time.Millisecond})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if core.Stat().IdleConnections() >= 4 {
			if got := driver.connects.Load(); got < 4 {
				t.Fatalf("driver dialled %d times, want at least MinConns (4)", got)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pool never warmed to MinConns: idle = %d", core.Stat().IdleConnections())
}

func TestCoreReapsExpiredConnections(t *testing.T) {
	t.Parallel()

	core, _ := newTestCore(t, Config{
		MaxConns:          4,
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
		if core.Stat().TotalConnections() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expired connections were never reaped: %d still open", core.Stat().TotalConnections())
}

func TestCorePropagatesConnectError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("server unreachable")
	driver := &fakeDriver{connectErr: sentinel}
	core, err := New(driver, Config{MaxConns: 1})
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(core.Close)

	if _, err := core.Acquire(t.Context()); !errors.Is(err, sentinel) {
		t.Fatalf("Acquire() = %v, want the driver's error", err)
	}

	// The permit must have come back, or one failed dial would shrink the pool.
	if _, err := core.Acquire(t.Context()); !errors.Is(err, sentinel) {
		t.Fatalf("second Acquire() = %v; the permit leaked on the error path", err)
	}
}

// The capacity bound must hold exactly under contention, whatever the drivers do.
//
// Both halves matter and they are not the same bound. Holding a permit before
// dialling bounds concurrent checkouts but not total connections: a permit
// released by one caller orders nothing with respect to another caller's freshly
// pooled connection, so a probe can miss an idle connection that already exists
// and dial a surplus one. This test caught exactly that — MaxConns 4 with five
// connections alive and one sitting idle, unseen.
func TestCoreHoldsCapacityUnderContention(t *testing.T) {
	t.Parallel()

	const capacity = 4
	core, _ := newTestCore(t, Config{MaxConns: capacity})

	var held, peak atomic.Int32
	var wg sync.WaitGroup

	for range 64 {
		wg.Go(func() {
			for range 50 {
				handle, err := core.Acquire(context.Background())
				if err != nil {
					return
				}

				current := held.Add(1)
				for {
					high := peak.Load()
					if current <= high || peak.CompareAndSwap(high, current) {
						break
					}
				}
				held.Add(-1)
				handle.Release()
			}
		})
	}
	wg.Wait()

	if got := peak.Load(); got > capacity {
		t.Errorf("peak concurrent holders = %d, want at most %d", got, capacity)
	}
	if got := core.Stat().TotalConnections(); got > capacity {
		t.Errorf("TotalConnections() = %d, want at most %d", got, capacity)
	}
}
