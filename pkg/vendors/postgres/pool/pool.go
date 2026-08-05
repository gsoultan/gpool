// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

// shardCount is the number of lock-striped idle buckets. A power of two keeps the
// modulo cheap, and 16 is deep enough to spread contention across realistic core
// counts without leaving every shard empty on a small pool.
const shardCount = 16

const (
	// connCloseTimeout bounds the graceful close of a single connection.
	connCloseTimeout = 5 * time.Second
	// closeDrainTimeout bounds how long Close waits for checked-out connections.
	closeDrainTimeout = 30 * time.Second
)

// Postgres is a connection pool for PostgreSQL implementing gpool.Pool.
//
// The zero value is not usable; construct it with New. All methods are safe for
// concurrent use, and Close is idempotent.
//
// Capacity is enforced by a permit set holding MaxConns tokens. A permit is held
// for as long as a connection is checked out, so total connections can never
// exceed MaxConns: a connection is only ever created while its acquirer holds a
// permit, and is only ever returned to a shard when one is being released.
type Postgres struct {
	config     Config
	connConfig *pgx.ConnConfig
	permits    permits
	shards     [shardCount]shard

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

// Compile-time proof that Postgres satisfies the public pool contract.
var _ gpool.Pool = (*Postgres)(nil)

// New creates a PostgreSQL pool. It validates the configuration and resolves the
// connection string up front, but does not dial the database: connections are
// established lazily on Acquire, and in the background up to MinConns.
func New(config Config) (*Postgres, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	config = config.withDefaults()

	connConfig, err := config.parse()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Postgres{
		config:     config,
		connConfig: connConfig,
		permits:    newPermits(config.MaxConns),
		bgCtx:      ctx,
		bgCancel:   cancel,
		bgDone:     make(chan struct{}),
	}

	go p.maintain()
	return p, nil
}

// Acquire checks out a connection, blocking until one is available, the context is
// cancelled, or the pool is closed. The caller must Release it.
func (p *Postgres) Acquire(ctx context.Context) (gpool.Conn, error) {
	return p.acquire(ctx)
}

// acquire is the typed form of Acquire used internally, so the pool-level helpers
// do not have to type-assert their own return value.
func (p *Postgres) acquire(ctx context.Context) (*connWrapper, error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}
	if err := p.acquirePermit(ctx); err != nil {
		return nil, err
	}
	// Close may have run while this caller was queued for a permit.
	if p.closed.Load() {
		p.permits.release()
		return nil, ErrPoolClosed
	}

	if ic, idx, ok := p.popIdle(); ok {
		return newConnWrapper(p, ic, idx), nil
	}

	ic, err := p.connect(ctx)
	if err != nil {
		p.permits.release()
		return nil, err
	}
	return newConnWrapper(p, ic, int(rand.UintN(shardCount))), nil
}

// acquirePermit takes a permit and records what it cost.
//
// Only the blocking path is timed. An acquisition served immediately has no wait
// to measure, and reading the clock there would cost more than the metric is worth
// on a path that is otherwise ~250ns.
func (p *Postgres) acquirePermit(ctx context.Context) error {
	if p.permits.tryAcquire() {
		p.acquireCount.Add(1)
		return nil
	}

	start := time.Now()
	if err := p.permits.wait(ctx); err != nil {
		p.canceledAcquireCount.Add(1)
		return err
	}

	p.acquireWaitNanos.Add(int64(time.Since(start)))
	p.emptyAcquireCount.Add(1)
	p.acquireCount.Add(1)
	return nil
}

// popIdle finds a usable idle connection, destroying any dead or expired ones it
// passes. Probing starts at a random shard: a shared round-robin counter would put
// every Acquire in the process on one contended cache line, which is exactly the
// contention the striping exists to remove.
func (p *Postgres) popIdle() (*idleConn, int, bool) {
	start := rand.UintN(shardCount)
	now := time.Now()

	for i := range uint(shardCount) {
		idx := int((start + i) % shardCount)
		s := &p.shards[idx]
		for {
			ic := s.pop()
			if ic == nil {
				break
			}
			if ic.usable(now, p.config.MaxConnLifetime, p.config.MaxConnIdleTime) {
				return ic, idx, true
			}
			p.destroy(ic)
		}
	}
	return nil, 0, false
}

// release returns a connection to the pool, or destroys it if it is no longer fit
// to reuse. The permit is released last so a waiter never wakes up before the
// connection it is waiting for is visible.
func (p *Postgres) release(ic *idleConn, shardIdx int) {
	defer p.permits.release()

	if p.closed.Load() || !p.recyclable(ic) {
		p.destroy(ic)
		return
	}

	ic.idleSince = time.Now()
	p.shards[shardIdx].push(ic)
}

// recyclable reports whether a returned connection is fit to hand to the next
// caller, cleaning it up where that is cheaper than replacing it.
//
// This is the boundary that makes pooling safe: whatever the previous caller left
// behind must not be observable by the next one.
func (p *Postgres) recyclable(ic *idleConn) bool {
	if ic.dead() {
		return false
	}

	// An unread result leaves the connection mid-protocol. Nothing can be sent on
	// it, not even a cleanup statement, so it cannot be salvaged.
	if ic.busy() {
		return false
	}

	// A caller that returns a connection without settling its transaction must not
	// leak it onward. Unwinding costs one round trip; replacing the connection
	// costs a full reconnect, so unwinding is tried first.
	if ic.inTransaction() && !p.rollback(ic) {
		return false
	}

	// A LISTEN outlives the caller that registered it, so the subscription is
	// cleared before the connection can serve anyone else. Only connections that
	// actually listened pay for this.
	if ic.listening && !p.unlisten(ic) {
		return false
	}

	if p.config.ResetQuery != "" && !p.reset(ic) {
		return false
	}

	// Idle expiry is deliberately not checked here: the connection was in use until
	// this instant, so only its total lifetime can have run out.
	return !ic.expired(time.Now(), p.config.MaxConnLifetime, 0)
}

// rollback unwinds a transaction left open by the previous caller, reporting
// whether the connection came back to a clean idle state.
func (p *Postgres) rollback(ic *idleConn) bool {
	ctx, cancel := context.WithTimeout(p.bgCtx, p.config.ResetQueryTimeout)
	defer cancel()

	if _, err := ic.conn.Exec(ctx, "ROLLBACK"); err != nil {
		return false
	}
	return !ic.inTransaction()
}

// unlisten clears every subscription on a connection, reporting whether it came
// back clean enough to reuse.
func (p *Postgres) unlisten(ic *idleConn) bool {
	ctx, cancel := context.WithTimeout(p.bgCtx, p.config.ResetQueryTimeout)
	defer cancel()

	if _, err := ic.conn.Exec(ctx, unlistenAll); err != nil {
		return false
	}
	ic.listening = false
	return true
}

// reset runs ResetQuery, reporting whether the connection is clean enough to reuse.
// A failed reset means the session state is unknown, so the caller destroys it
// rather than leaking one caller's state into the next.
func (p *Postgres) reset(ic *idleConn) bool {
	ctx, cancel := context.WithTimeout(p.bgCtx, p.config.ResetQueryTimeout)
	defer cancel()

	_, err := ic.conn.Exec(ctx, p.config.ResetQuery)
	return err == nil
}

// connect establishes one new connection from the pre-parsed template.
func (p *Postgres) connect(ctx context.Context) (*idleConn, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && p.config.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.ConnectTimeout)
		defer cancel()
	}

	connConfig := p.connConfig.Copy()
	if p.config.BeforeConnect != nil {
		if err := p.config.BeforeConnect(connConfig); err != nil {
			return nil, fmt.Errorf("gpool/postgres: BeforeConnect: %w", err)
		}
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("gpool/postgres: connect: %w", err)
	}

	if p.config.AfterConnect != nil {
		if err := p.config.AfterConnect(conn); err != nil {
			closeConn(conn)
			return nil, fmt.Errorf("gpool/postgres: AfterConnect: %w", err)
		}
	}

	p.totalConns.Add(1)
	now := time.Now()
	return &idleConn{conn: conn, createdAt: now, idleSince: now}, nil
}

// destroy closes a connection and drops it from the total count.
func (p *Postgres) destroy(ic *idleConn) {
	closeConn(ic.conn)
	p.totalConns.Add(-1)
}

// closeConn closes a pgx connection with a bounded, cancellation-immune context so
// shutdown still gets a chance to send a graceful Terminate.
func closeConn(conn *pgx.Conn) {
	if conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), connCloseTimeout)
	defer cancel()
	_ = conn.Close(ctx)
}

// maintain runs the background reaper: it retires expired connections and keeps
// MinConns warm. It exits when the pool is closed.
func (p *Postgres) maintain() {
	defer close(p.bgDone)

	p.warmUp()

	if p.config.HealthCheckPeriod <= 0 {
		<-p.bgCtx.Done()
		return
	}

	ticker := time.NewTicker(p.config.HealthCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-p.bgCtx.Done():
			return
		case <-ticker.C:
			p.reapExpired()
			p.warmUp()
		}
	}
}

// reapExpired closes idle connections that have outlived their bounds. Without it
// a pool that goes quiet after a failover keeps handing out connections to a server
// that no longer exists.
func (p *Postgres) reapExpired() {
	now := time.Now()
	stale := func(ic *idleConn) bool {
		return ic.dead() || ic.expired(now, p.config.MaxConnLifetime, p.config.MaxConnIdleTime)
	}

	for i := range p.shards {
		for _, ic := range p.shards[i].takeIf(stale) {
			p.destroy(ic)
		}
	}
}

// warmUp tops the pool up to MinConns. Each connection is created while holding a
// permit so the MaxConns invariant still holds, and a dial failure ends the round
// rather than spinning against an unreachable server.
func (p *Postgres) warmUp() {
	for p.config.MinConns > 0 && p.totalConns.Load() < p.config.MinConns {
		if p.closed.Load() {
			return
		}
		if err := p.permits.acquire(p.bgCtx); err != nil {
			return
		}

		ic, err := p.connect(p.bgCtx)
		if err != nil {
			p.permits.release()
			return
		}

		p.shards[rand.UintN(shardCount)].push(ic)
		p.permits.release()
	}
}

// Close shuts the pool down. It stops the reaper, waits briefly for checked-out
// connections to come back, and closes everything idle. It is idempotent, and it
// never blocks indefinitely: a connection that is still out when the drain window
// expires is closed by whoever eventually releases it.
func (p *Postgres) Close() {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		p.bgCancel()
		<-p.bgDone

		ctx, cancel := context.WithTimeout(context.Background(), closeDrainTimeout)
		p.permits.drain(ctx, p.config.MaxConns)
		cancel()

		for i := range p.shards {
			for _, ic := range p.shards[i].drain() {
				p.destroy(ic)
			}
		}
	})
}

// Stat reports current pool occupancy. It is lock-free.
func (p *Postgres) Stat() gpool.Stat {
	total := p.totalConns.Load()

	var idle int32
	for i := range p.shards {
		idle += p.shards[i].count.Load()
	}

	// total and the shard counters are sampled independently, so clamp rather than
	// report a nonsensical pair to a metrics backend.
	idle = min(idle, total)

	return poolStat{
		total:     total,
		idle:      idle,
		active:    max(total-idle, 0),
		maxConns:  p.config.MaxConns,
		acquires:  p.acquireCount.Load(),
		waitNanos: p.acquireWaitNanos.Load(),
		empties:   p.emptyAcquireCount.Load(),
		canceled:  p.canceledAcquireCount.Load(),
	}
}

// Exec acquires a connection, runs the statement, and releases the connection.
func (p *Postgres) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	conn, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return conn.Exec(ctx, sql, args...)
}

// Query acquires a connection and runs the query. The connection is held until the
// returned Rows are closed, which Rows.All does automatically when iteration ends.
func (p *Postgres) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	conn, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := conn.conn.Query(ctx, sql, args...)
	if err != nil {
		conn.Release()
		return nil, err
	}
	return newRows(rows, conn), nil
}

// QueryRow acquires a connection and runs the query. The connection is released by
// the first call to Scan or Release on the returned Row, whichever happens first;
// calling neither leaks the connection for the lifetime of the pool.
func (p *Postgres) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	conn, err := p.acquire(ctx)
	if err != nil {
		return errorRow{err: err}
	}

	rows, err := conn.conn.Query(ctx, sql, args...)
	if err != nil {
		closeRows(rows)
		conn.Release()
		return errorRow{err: err}
	}
	return newRow(rows, conn)
}
