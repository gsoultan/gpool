// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// readyDriver is a fakeDriver that also reports readiness, standing in for a
// protocol whose per-connection setup is performed by the caller rather than by
// Connect.
type readyDriver struct {
	fakeDriver
	// defaultReady is what a freshly connected connection reports, so a test
	// can decide whether setup has happened yet.
	defaultReady bool
}

var _ ReadinessChecker[*fakeConn] = (*readyDriver)(nil)

func (d *readyDriver) Connect(ctx context.Context) (*fakeConn, error) {
	conn, err := d.fakeDriver.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if d.defaultReady {
		markReady(conn)
	}
	return conn, nil
}

func (d *readyDriver) Ready(conn *fakeConn) bool {
	return readyFlag(conn).Load()
}

// The readiness bit lives beside the connection rather than on fakeConn, so the
// shared fixture stays untouched for drivers that do not implement the
// capability.
var readyBits sync.Map // *fakeConn -> *atomic.Bool

func readyFlag(conn *fakeConn) *atomic.Bool {
	v, _ := readyBits.LoadOrStore(conn, new(atomic.Bool))
	return v.(*atomic.Bool)
}

func markReady(conn *fakeConn) { readyFlag(conn).Store(true) }

func newReadyPool(t *testing.T, d *readyDriver, cfg Config) *Core[*fakeConn] {
	t.Helper()
	core, err := New[*fakeConn](d, cfg.WithDefaults())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { core.Close() })
	return core
}

// A connection released without ever becoming ready must be destroyed, not
// pooled. Left in the idle set it is indistinguishable from a ready one, and
// the next caller that assumes readiness — a health check running a query —
// fails on it and condemns the backend.
func TestUnreadyConnectionIsDestroyedOnRelease(t *testing.T) {
	d := &readyDriver{defaultReady: false}
	core := newReadyPool(t, d, Config{MaxConns: 4})

	handle, err := core.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	handle.Release()

	if got := d.closes.Load(); got != 1 {
		t.Fatalf("unready connection was recycled: closes = %d, want 1", got)
	}
	if got := core.Stat().IdleConnections(); got != 0 {
		t.Errorf("unready connection went back to the idle set: idle = %d", got)
	}
}

// The caller completing setup is the normal path, and it must recycle.
func TestReadyConnectionIsRecycled(t *testing.T) {
	d := &readyDriver{defaultReady: false}
	core := newReadyPool(t, d, Config{MaxConns: 4})

	handle, err := core.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	markReady(handle.Conn()) // the caller's setup exchange succeeded
	handle.Release()

	if got := d.closes.Load(); got != 0 {
		t.Fatalf("ready connection was destroyed: closes = %d, want 0", got)
	}
	if got := core.Stat().IdleConnections(); got != 1 {
		t.Errorf("ready connection was not pooled: idle = %d, want 1", got)
	}
}

// Readiness survives recycling: a connection set up once must not have to be
// set up again on every acquisition.
func TestReadinessPersistsAcrossAcquisitions(t *testing.T) {
	d := &readyDriver{defaultReady: false}
	core := newReadyPool(t, d, Config{MaxConns: 4})

	first, err := core.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	markReady(first.Conn())
	first.Release()

	second, err := core.Acquire(context.Background())
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if !d.Ready(second.Conn()) {
		t.Error("a connection that completed setup came back unready")
	}
	second.Release()

	if got := d.connects.Load(); got != 1 {
		t.Errorf("connection was not reused: connects = %d, want 1", got)
	}
}

// A driver that does not implement ReadinessChecker must behave exactly as
// before — the capability is opt-in.
func TestDriverWithoutReadinessIsUnaffected(t *testing.T) {
	d := &fakeDriver{}
	core, err := New[*fakeConn](d, Config{MaxConns: 4}.WithDefaults())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer core.Close()

	handle, err := core.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	handle.Release()

	if got := d.closes.Load(); got != 0 {
		t.Fatalf("a driver without readiness had its connection destroyed: closes = %d", got)
	}
	if got := core.Stat().IdleConnections(); got != 1 {
		t.Errorf("idle = %d, want 1", got)
	}
}

// Readiness is checked alongside the other release-time rules, not instead of
// them: a dead connection is still destroyed even if it reported ready.
func TestDeadReadyConnectionIsStillDestroyed(t *testing.T) {
	d := &readyDriver{defaultReady: true}
	core := newReadyPool(t, d, Config{MaxConns: 4})

	handle, err := core.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	handle.Conn().dead.Store(true)
	handle.Release()

	if got := core.Stat().IdleConnections(); got != 0 {
		t.Errorf("dead connection was pooled: idle = %d", got)
	}
}
