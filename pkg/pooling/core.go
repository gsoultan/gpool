// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
)

const (
	// connCloseTimeout bounds the graceful close of a single connection.
	connCloseTimeout = 5 * time.Second
	// closeDrainTimeout bounds how long Close waits for checked-out connections.
	closeDrainTimeout = 30 * time.Second
)

// ErrClosed is returned by Acquire once Close has been called.
var ErrClosed = errors.New("gpool/pooling: pool is closed")

// Core is the vendor-agnostic pooling engine.
//
// Capacity is enforced twice, because the two bounds are not the same thing:
//
//   - A permit set of MaxConns tokens bounds how many callers hold a connection
//     at once. A permit is held for as long as a connection is checked out.
//   - A reservation on the total count bounds how many connections exist at all.
//
// The permit alone is not sufficient for the second bound. A permit released by
// one caller creates no ordering with respect to a different caller's freshly
// pooled connection, so a caller can hold a permit, fail to see an idle
// connection that already exists, and dial a surplus one. See take.
//
// All methods are safe for concurrent use, and Close is idempotent.
type Core[C any] struct {
	driver  Driver[C]
	config  Config
	permits permits
	shards  [shardCount]shard[C]

	// clock is read on every acquire and release, so it is cached rather than
	// asked of the system each time.
	clock *coarseClock

	totalConns atomic.Int32

	// Cumulative acquisition counters. Occupancy alone cannot distinguish a pool
	// that is merely busy from one that is too small; these can.
	acquireCount         atomic.Int64
	acquireWaitNanos     atomic.Int64
	emptyAcquireCount    atomic.Int64
	canceledAcquireCount atomic.Int64

	closed    atomic.Bool
	closeOnce sync.Once
	bgCtx     context.Context
	bgCancel  context.CancelFunc
	bgDone    chan struct{}
}

// New creates a pooling engine for the given driver. It validates the
// configuration but does not dial: connections are established lazily on Acquire,
// and in the background up to MinConns.
func New[C any](driver Driver[C], config Config) (*Core[C], error) {
	if driver == nil {
		return nil, fmt.Errorf("%w: driver is required", ErrInvalidConfig)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	config = config.WithDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	c := &Core[C]{
		driver:   driver,
		config:   config,
		permits:  newPermits(config.MaxConns),
		clock:    newCoarseClock(),
		bgCtx:    ctx,
		bgCancel: cancel,
		bgDone:   make(chan struct{}),
	}

	go c.maintain()
	return c, nil
}

// Config returns the effective configuration, with defaults applied.
func (c *Core[C]) Config() Config {
	return c.config
}

// Acquire checks out a connection, blocking until one is available, the context
// is cancelled, or the pool is closed. The caller must Release the handle.
func (c *Core[C]) Acquire(ctx context.Context) (Handle[C], error) {
	if c.closed.Load() {
		return Handle[C]{}, ErrClosed
	}
	if err := c.acquirePermit(ctx); err != nil {
		return Handle[C]{}, err
	}
	// Close may have run while this caller was queued for a permit.
	if c.closed.Load() {
		c.permits.release()
		return Handle[C]{}, ErrClosed
	}

	ic, idx, err := c.take(ctx)
	if err != nil {
		c.permits.release()
		return Handle[C]{}, err
	}
	return Handle[C]{core: c, idle: ic, conn: ic.conn, shardIdx: idx}, nil
}

// take returns a connection for a caller that already holds a permit, either by
// reusing an idle one or by establishing a new one.
//
// Holding a permit is not on its own enough to bound the total number of
// connections. A permit released by one caller establishes no ordering with
// respect to a *different* caller's freshly pooled connection, so a probe can
// legitimately miss an idle connection that already exists and dial a surplus
// one. Reserving a slot in the total count is what makes MaxConns an actual
// ceiling on connections to the server rather than only on concurrent checkouts.
func (c *Core[C]) take(ctx context.Context) (*idleConn[C], int, error) {
	for {
		if ic, idx, ok := c.popIdle(); ok {
			return ic, idx, nil
		}

		if c.reserveSlot() {
			ic, err := c.connect(ctx)
			if err != nil {
				c.releaseSlot()
				return nil, 0, err
			}
			return ic, int(rand.UintN(shardCount)), nil
		}

		// At the ceiling with nothing visible to probe. Because this caller holds
		// a permit, fewer than MaxConns others can hold one, so at least one of
		// the existing connections is idle or on its way there — it just has not
		// become visible yet. Yield and look again rather than exceed the ceiling.
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		runtime.Gosched()
	}
}

// reserveSlot claims room for one more connection, reporting false at the ceiling.
func (c *Core[C]) reserveSlot() bool {
	for {
		current := c.totalConns.Load()
		if current >= c.config.MaxConns {
			return false
		}
		if c.totalConns.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

// releaseSlot gives back a reservation that was not turned into a connection.
func (c *Core[C]) releaseSlot() {
	c.totalConns.Add(-1)
}

// acquirePermit takes a permit and records what it cost.
//
// Only the blocking path is timed. An acquisition served immediately has no wait
// to measure, and reading the clock there would cost more than the metric is
// worth on a path that is otherwise a couple of hundred nanoseconds.
func (c *Core[C]) acquirePermit(ctx context.Context) error {
	if c.permits.tryAcquire() {
		c.acquireCount.Add(1)
		return nil
	}

	start := time.Now()
	if err := c.permits.wait(ctx); err != nil {
		c.canceledAcquireCount.Add(1)
		return err
	}

	c.acquireWaitNanos.Add(int64(time.Since(start)))
	c.emptyAcquireCount.Add(1)
	c.acquireCount.Add(1)
	return nil
}

// popIdle finds a usable idle connection, destroying any dead or expired ones it
// passes. Probing starts at a random shard: a shared round-robin counter would
// put every Acquire in the process on one contended cache line, which is exactly
// the contention the striping exists to remove.
func (c *Core[C]) popIdle() (*idleConn[C], int, bool) {
	start := rand.UintN(shardCount)
	now := c.clock.now()

	for i := range uint(shardCount) {
		idx := int((start + i) % shardCount)
		s := &c.shards[idx]
		for {
			ic := s.pop()
			if ic == nil {
				break
			}
			if !c.driver.Dead(ic.conn) && !ic.expired(now, c.config.MaxConnLifetime, c.config.MaxConnIdleTime) {
				return ic, idx, true
			}
			c.destroy(ic)
		}
	}
	return nil, 0, false
}

// release returns a connection to the pool, or destroys it if it is no longer fit
// to reuse. The permit is released last so a waiter never wakes up before the
// connection it is waiting for is visible.
func (c *Core[C]) release(ic *idleConn[C], shardIdx int) {
	defer c.permits.release()

	if c.closed.Load() || !c.recyclable(ic) {
		c.destroy(ic)
		return
	}

	ic.idleSince = c.clock.now()
	c.shards[shardIdx].push(ic)
}

// recyclable reports whether a returned connection is fit for the next caller.
func (c *Core[C]) recyclable(ic *idleConn[C]) bool {
	if c.driver.Dead(ic.conn) {
		return false
	}

	// Building a deadline context costs four allocations and a runtime timer, so
	// a connection returned with nothing left on it pays none of it.
	if c.driver.NeedsCleanup(ic.conn) {
		ctx, cancel := context.WithTimeout(c.bgCtx, c.config.CleanupTimeout)
		clean := c.driver.Recyclable(ctx, ic.conn)
		cancel()

		if !clean {
			return false
		}
	}

	// Idle expiry is deliberately not checked here: the connection was in use
	// until this instant, so only its total lifetime can have run out.
	return !ic.expired(c.clock.now(), c.config.MaxConnLifetime, 0)
}

// connect establishes one new connection.
func (c *Core[C]) connect(ctx context.Context) (*idleConn[C], error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && c.config.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.ConnectTimeout)
		defer cancel()
	}

	conn, err := c.driver.Connect(ctx)
	if err != nil {
		return nil, err
	}

	// The slot was reserved by take before dialling, so the count is already
	// correct. Establishing a connection is not a hot path, so this one reading
	// of the clock is exact rather than cached.
	now := time.Now()
	return &idleConn[C]{conn: conn, createdAt: now, idleSince: now}, nil
}

// destroy closes a connection and drops it from the total count.
func (c *Core[C]) destroy(ic *idleConn[C]) {
	ctx, cancel := context.WithTimeout(context.Background(), connCloseTimeout)
	_ = c.driver.Close(ctx, ic.conn)
	cancel()

	c.releaseSlot()
}

// maintain runs the background reaper and keeps the cached clock fresh.
func (c *Core[C]) maintain() {
	defer close(c.bgDone)

	c.warmUp()

	// The clock ticks regardless of whether health checking is enabled: the
	// acquire path reads it, so it must stay fresh even when nothing is reaped.
	clock := time.NewTicker(clockResolution)
	defer clock.Stop()

	var health <-chan time.Time
	if c.config.HealthCheckPeriod > 0 {
		ticker := time.NewTicker(c.config.HealthCheckPeriod)
		defer ticker.Stop()
		health = ticker.C
	}

	for {
		select {
		case <-c.bgCtx.Done():
			return
		case <-clock.C:
			c.clock.update()
		case <-health:
			c.reapExpired()
			c.warmUp()
		}
	}
}

// reapExpired closes idle connections that have outlived their bounds. Without it
// a pool that goes quiet after a failover keeps handing out connections to a
// server that no longer exists.
func (c *Core[C]) reapExpired() {
	now := c.clock.now()
	stale := func(ic *idleConn[C]) bool {
		return c.driver.Dead(ic.conn) || ic.expired(now, c.config.MaxConnLifetime, c.config.MaxConnIdleTime)
	}

	for i := range c.shards {
		for _, ic := range c.shards[i].takeIf(stale) {
			c.destroy(ic)
		}
	}
}

// warmUp tops the pool up to MinConns. Each connection is created while holding a
// permit so the MaxConns invariant still holds, and a dial failure ends the round
// rather than spinning against an unreachable server.
func (c *Core[C]) warmUp() {
	for c.config.MinConns > 0 && c.totalConns.Load() < c.config.MinConns {
		if c.closed.Load() {
			return
		}
		if err := c.permits.acquire(c.bgCtx); err != nil {
			return
		}
		if !c.reserveSlot() {
			c.permits.release()
			return
		}

		ic, err := c.connect(c.bgCtx)
		if err != nil {
			c.releaseSlot()
			c.permits.release()
			return
		}

		c.shards[rand.UintN(shardCount)].push(ic)
		c.permits.release()
	}
}

// Close shuts the pool down. It stops the reaper, waits briefly for checked-out
// connections to come back, and closes everything idle. It is idempotent, and it
// never blocks indefinitely: a connection still out when the drain window expires
// is closed by whoever eventually releases it.
func (c *Core[C]) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.bgCancel()
		<-c.bgDone

		ctx, cancel := context.WithTimeout(context.Background(), closeDrainTimeout)
		c.permits.drain(ctx, c.config.MaxConns)
		cancel()

		for i := range c.shards {
			for _, ic := range c.shards[i].drain() {
				c.destroy(ic)
			}
		}
	})
}

// Stat reports current occupancy and cumulative acquisition counters. It is lock-free.
func (c *Core[C]) Stat() gpool.Stat {
	total := c.totalConns.Load()

	var idle int32
	for i := range c.shards {
		idle += c.shards[i].count.Load()
	}

	// total and the shard counters are sampled independently, so clamp rather
	// than report a nonsensical pair to a metrics backend.
	idle = min(idle, total)

	return Stat{
		total:     total,
		idle:      idle,
		active:    max(total-idle, 0),
		maxConns:  c.config.MaxConns,
		acquires:  c.acquireCount.Load(),
		waitNanos: c.acquireWaitNanos.Load(),
		empties:   c.emptyAcquireCount.Load(),
		canceled:  c.canceledAcquireCount.Load(),
	}
}
