// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

// connWrapper is a checked-out connection implementing gpool.Conn.
//
// One wrapper is allocated per acquisition and is never recycled. That is
// deliberate: the wrapper's lifetime is controlled by user code, so pooling it
// would let a second Release from one goroutine hand a live connection back to the
// pool while another goroutine is still using it. A three-word allocation is
// immaterial next to the network round trip it fronts.
type connWrapper struct {
	pool     *Postgres
	idle     *idleConn
	conn     *pgx.Conn
	shardIdx int
	released atomic.Bool
}

var _ gpool.Conn = (*connWrapper)(nil)

func newConnWrapper(p *Postgres, ic *idleConn, shardIdx int) *connWrapper {
	return &connWrapper{pool: p, idle: ic, conn: ic.conn, shardIdx: shardIdx}
}

// Release returns the connection to the pool. It is idempotent; releasing twice is
// a no-op rather than a double return that would corrupt pool accounting.
func (c *connWrapper) Release() {
	if !c.released.CompareAndSwap(false, true) {
		return
	}
	c.pool.release(c.idle, c.shardIdx)
}

// live reports whether the connection may still be used.
func (c *connWrapper) live() error {
	if c.released.Load() {
		return ErrConnReleased
	}
	return nil
}

// Exec executes a statement that returns no rows.
func (c *connWrapper) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	if err := c.live(); err != nil {
		return nil, err
	}

	tag, err := c.conn.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgResult{tag: tag}, nil
}

// Query executes a query. The returned Rows do not own the connection: the caller
// still has it and is responsible for releasing it.
func (c *connWrapper) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	if err := c.live(); err != nil {
		return nil, err
	}

	rows, err := c.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return newRows(rows, nil), nil
}

// QueryRow executes a query expected to return at most one row.
func (c *connWrapper) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	if err := c.live(); err != nil {
		return errorRow{err: err}
	}

	rows, err := c.conn.Query(ctx, sql, args...)
	if err != nil {
		closeRows(rows)
		return errorRow{err: err}
	}
	return newRow(rows, nil)
}

// closeRows closes a result set that is being discarded. pgx returns a non-nil Rows
// alongside a query error, and leaving it open would keep the connection busy.
func closeRows(rows pgx.Rows) {
	if rows != nil {
		rows.Close()
	}
}

// Begin starts a transaction on this connection. The transaction does not own the
// connection; release it yourself once the transaction has finished.
func (c *connWrapper) Begin(ctx context.Context) (gpool.Tx, error) {
	if err := c.live(); err != nil {
		return nil, err
	}

	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return newTx(tx), nil
}

// Ping verifies the connection is still usable.
func (c *connWrapper) Ping(ctx context.Context) error {
	if err := c.live(); err != nil {
		return err
	}
	return c.conn.Ping(ctx)
}
