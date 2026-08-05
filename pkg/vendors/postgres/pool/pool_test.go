// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestPool(t *testing.T, config Config) *Postgres {
	t.Helper()

	if config.ConnString == "" {
		config.ConnString = testConnString
	}

	p, err := New(config)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	if _, err := New(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() = %v, want ErrInvalidConfig", err)
	}
}

func TestAcquireAfterCloseFailsFast(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 2})
	p.Close()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if _, err := p.Acquire(ctx); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Acquire() = %v, want ErrPoolClosed", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 2})

	// A second and third Close must not double-drain or panic.
	p.Close()
	p.Close()
	p.Close()
}

// Releasing the same connection twice used to return a live connection to the pool
// while another goroutine still held it, and over-released the permit set.
func TestConnReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 1})

	if err := p.permits.acquire(t.Context()); err != nil {
		t.Fatalf("priming the permit set: %v", err)
	}
	p.totalConns.Add(1)

	now := time.Now()
	conn := newConnWrapper(p, &idleConn{createdAt: now, idleSince: now}, 0)
	conn.Release()
	conn.Release()
	conn.Release()

	// Exactly one permit must have come back.
	first, cancelFirst := context.WithTimeout(t.Context(), time.Second)
	defer cancelFirst()
	if err := p.permits.acquire(first); err != nil {
		t.Fatalf("permit was not returned: %v", err)
	}

	second, cancelSecond := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelSecond()
	if err := p.permits.acquire(second); err == nil {
		t.Fatal("the permit set handed out more permits than were held")
	}

	p.permits.release()
}

func TestConnUseAfterReleaseIsRefused(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 1})

	if err := p.permits.acquire(t.Context()); err != nil {
		t.Fatalf("priming the permit set: %v", err)
	}

	now := time.Now()
	conn := newConnWrapper(p, &idleConn{createdAt: now, idleSince: now}, 0)
	conn.Release()

	if _, err := conn.Exec(t.Context(), "SELECT 1"); !errors.Is(err, ErrConnReleased) {
		t.Errorf("Exec() = %v, want ErrConnReleased", err)
	}
	if _, err := conn.Query(t.Context(), "SELECT 1"); !errors.Is(err, ErrConnReleased) {
		t.Errorf("Query() = %v, want ErrConnReleased", err)
	}
	if _, err := conn.Begin(t.Context()); !errors.Is(err, ErrConnReleased) {
		t.Errorf("Begin() = %v, want ErrConnReleased", err)
	}
	if err := conn.Ping(t.Context()); !errors.Is(err, ErrConnReleased) {
		t.Errorf("Ping() = %v, want ErrConnReleased", err)
	}
	if err := conn.QueryRow(t.Context(), "SELECT 1").Scan(); !errors.Is(err, ErrConnReleased) {
		t.Errorf("QueryRow().Scan() = %v, want ErrConnReleased", err)
	}
}

func TestStatReportsShardOccupancy(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 8})

	if got := p.Stat(); got.TotalConnections() != 0 || got.IdleConnections() != 0 || got.ActiveConnections() != 0 {
		t.Fatalf("empty pool stat = %+v, want all zero", got)
	}

	now := time.Now()
	p.totalConns.Add(3)
	p.shards[0].push(&idleConn{createdAt: now, idleSince: now})
	p.shards[5].push(&idleConn{createdAt: now, idleSince: now})

	stat := p.Stat()
	if stat.TotalConnections() != 3 {
		t.Errorf("TotalConnections() = %d, want 3", stat.TotalConnections())
	}
	if stat.IdleConnections() != 2 {
		t.Errorf("IdleConnections() = %d, want 2", stat.IdleConnections())
	}
	if stat.ActiveConnections() != 1 {
		t.Errorf("ActiveConnections() = %d, want 1", stat.ActiveConnections())
	}
}

// Stat samples the total and the shard counters independently, so a race between
// them must clamp rather than report a negative active count to a metrics backend.
func TestStatClampsInconsistentSample(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 8})

	now := time.Now()
	p.shards[1].push(&idleConn{createdAt: now, idleSince: now})
	p.shards[2].push(&idleConn{createdAt: now, idleSince: now})

	stat := p.Stat()
	if stat.ActiveConnections() < 0 {
		t.Fatalf("ActiveConnections() = %d, want non-negative", stat.ActiveConnections())
	}
	if stat.IdleConnections() > stat.TotalConnections() {
		t.Fatalf("idle %d exceeds total %d", stat.IdleConnections(), stat.TotalConnections())
	}
}

// Occupancy alone cannot distinguish a pool that is busy from one that is too
// small. These counters are what makes MaxConns tunable from production data.
func TestStatTracksAcquisitions(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 1})

	if got := p.Stat().MaxConnections(); got != 1 {
		t.Fatalf("MaxConnections() = %d, want 1", got)
	}

	// Served immediately: counted, but contributes no wait.
	if err := p.acquirePermit(t.Context()); err != nil {
		t.Fatalf("acquirePermit() = %v", err)
	}

	stat := p.Stat()
	if stat.AcquireCount() != 1 {
		t.Errorf("AcquireCount() = %d, want 1", stat.AcquireCount())
	}
	if stat.EmptyAcquireCount() != 0 {
		t.Errorf("EmptyAcquireCount() = %d, want 0 for an uncontended acquire", stat.EmptyAcquireCount())
	}
	if stat.AcquireDuration() != 0 {
		t.Errorf("AcquireDuration() = %v, want 0 for an uncontended acquire", stat.AcquireDuration())
	}

	// The pool is now exhausted, so this one gives up with its context.
	timedOut, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := p.acquirePermit(timedOut); err == nil {
		t.Fatal("acquirePermit() on an exhausted pool should fail")
	}

	stat = p.Stat()
	if stat.CanceledAcquireCount() != 1 {
		t.Errorf("CanceledAcquireCount() = %d, want 1", stat.CanceledAcquireCount())
	}
	if stat.AcquireCount() != 1 {
		t.Errorf("AcquireCount() = %d, a cancelled acquisition should not count as one", stat.AcquireCount())
	}

	// A caller that waits and then succeeds is counted as an empty acquire and
	// contributes its wait to the total.
	waited := make(chan error, 1)
	go func() { waited <- p.acquirePermit(context.Background()) }()

	time.Sleep(20 * time.Millisecond)
	p.permits.release()

	if err := <-waited; err != nil {
		t.Fatalf("the waiting acquirer failed: %v", err)
	}

	stat = p.Stat()
	if stat.EmptyAcquireCount() != 1 {
		t.Errorf("EmptyAcquireCount() = %d, want 1", stat.EmptyAcquireCount())
	}
	if stat.AcquireCount() != 2 {
		t.Errorf("AcquireCount() = %d, want 2", stat.AcquireCount())
	}
	if stat.AcquireDuration() <= 0 {
		t.Errorf("AcquireDuration() = %v, want a positive wait", stat.AcquireDuration())
	}

	p.permits.release()
}

func TestShardPushPopIsLIFO(t *testing.T) {
	t.Parallel()

	var s shard
	first := &idleConn{}
	second := &idleConn{}

	if got := s.pop(); got != nil {
		t.Fatal("pop() on an empty shard should return nil")
	}

	s.push(first)
	s.push(second)

	if got := s.count.Load(); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
	// The hottest connection is reused so the rest go cold and the reaper can retire them.
	if got := s.pop(); got != second {
		t.Error("pop() should return the most recently pushed connection")
	}
	if got := s.pop(); got != first {
		t.Error("pop() should then return the older connection")
	}
	if got := s.count.Load(); got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
}

func TestShardTakeIfAndDrain(t *testing.T) {
	t.Parallel()

	var s shard
	keep := &idleConn{createdAt: time.Now()}
	drop := &idleConn{createdAt: time.Now().Add(-time.Hour)}

	s.push(keep)
	s.push(drop)

	taken := s.takeIf(func(ic *idleConn) bool { return ic == drop })
	if len(taken) != 1 || taken[0] != drop {
		t.Fatalf("takeIf() = %v, want exactly the matching connection", taken)
	}
	if got := s.count.Load(); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}

	drained := s.drain()
	if len(drained) != 1 || drained[0] != keep {
		t.Fatalf("drain() = %v, want the remaining connection", drained)
	}
	if got := s.count.Load(); got != 0 {
		t.Fatalf("count after drain = %d, want 0", got)
	}
}

func TestIdleConnExpired(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name        string
		conn        idleConn
		maxLifetime time.Duration
		maxIdle     time.Duration
		want        bool
	}{
		{
			name:        "fresh connection",
			conn:        idleConn{createdAt: now, idleSince: now},
			maxLifetime: time.Hour, maxIdle: time.Minute,
			want: false,
		},
		{
			name:        "lifetime exceeded",
			conn:        idleConn{createdAt: now.Add(-2 * time.Hour), idleSince: now},
			maxLifetime: time.Hour, maxIdle: time.Minute,
			want: true,
		},
		{
			name:        "idle exceeded",
			conn:        idleConn{createdAt: now, idleSince: now.Add(-2 * time.Minute)},
			maxLifetime: time.Hour, maxIdle: time.Minute,
			want: true,
		},
		{
			name:        "bounds disabled",
			conn:        idleConn{createdAt: now.Add(-100 * time.Hour), idleSince: now.Add(-100 * time.Hour)},
			maxLifetime: -1, maxIdle: -1,
			want: false,
		},
		{
			name:        "idle bound ignored when zero",
			conn:        idleConn{createdAt: now, idleSince: now.Add(-100 * time.Hour)},
			maxLifetime: time.Hour, maxIdle: 0,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.conn.expired(now, tt.maxLifetime, tt.maxIdle); got != tt.want {
				t.Fatalf("expired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIdleConnNilIsDead(t *testing.T) {
	t.Parallel()

	ic := &idleConn{}
	if !ic.dead() {
		t.Fatal("a connection with no driver handle should count as dead")
	}
	if ic.usable(time.Now(), time.Hour, time.Minute) {
		t.Fatal("a dead connection should never be usable")
	}

	// The state probes reach into the driver, so they must short-circuit on a dead
	// connection rather than dereference it.
	if ic.busy() {
		t.Error("busy() on a dead connection should be false, not a panic")
	}
	if ic.inTransaction() {
		t.Error("inTransaction() on a dead connection should be false, not a panic")
	}
}

// A dead connection is never recycled, whatever else is configured.
func TestRecyclableRejectsDeadConnection(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 1})

	if p.recyclable(&idleConn{createdAt: time.Now(), idleSince: time.Now()}) {
		t.Fatal("recyclable() accepted a connection with no driver handle")
	}
}

// popIdle must discard expired connections instead of handing them out, otherwise a
// pool that idled through a failover keeps serving connections to a server that is gone.
func TestPopIdleDiscardsExpired(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 4, MaxConnLifetime: time.Minute})

	stale := &idleConn{createdAt: time.Now().Add(-time.Hour), idleSince: time.Now()}
	p.totalConns.Add(1)
	p.shards[3].push(stale)

	if _, _, ok := p.popIdle(); ok {
		t.Fatal("popIdle() handed out an expired connection")
	}
	if got := p.totalConns.Load(); got != 0 {
		t.Fatalf("totalConns = %d, want 0 after the expired connection was destroyed", got)
	}
}

func TestReapExpiredDrainsStaleConnections(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 4, MaxConnLifetime: time.Minute})

	p.totalConns.Add(2)
	p.shards[0].push(&idleConn{createdAt: time.Now().Add(-time.Hour), idleSince: time.Now()})
	p.shards[1].push(&idleConn{createdAt: time.Now().Add(-time.Hour), idleSince: time.Now()})

	p.reapExpired()

	if got := p.Stat().IdleConnections(); got != 0 {
		t.Fatalf("IdleConnections() = %d, want 0", got)
	}
	if got := p.totalConns.Load(); got != 0 {
		t.Fatalf("totalConns = %d, want 0", got)
	}
}

func TestReleaseDestroysWhenPoolIsClosed(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 1})

	if err := p.permits.acquire(t.Context()); err != nil {
		t.Fatalf("priming the permit set: %v", err)
	}
	p.totalConns.Add(1)

	now := time.Now()
	conn := newConnWrapper(p, &idleConn{createdAt: now, idleSince: now}, 0)

	p.closed.Store(true)
	conn.Release()

	if got := p.Stat().IdleConnections(); got != 0 {
		t.Fatalf("a connection released into a closed pool was pooled anyway: idle = %d", got)
	}
}
