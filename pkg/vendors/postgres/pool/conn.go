// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/pooling"
)

// connWrapper is a checked-out connection implementing gpool.Conn.
//
// One wrapper is allocated per acquisition and is never recycled. That is
// deliberate: the wrapper's lifetime is controlled by user code, so pooling it
// would let a second Release from one goroutine hand a live connection back to the
// pool while another goroutine is still using it.
type connWrapper struct {
	handle pooling.Handle[*pgConn]
}

var _ gpool.Conn = (*connWrapper)(nil)

func newConnWrapper(handle pooling.Handle[*pgConn]) *connWrapper {
	return &connWrapper{handle: handle}
}

// conn returns the underlying pooled connection and its vendor state.
func (c *connWrapper) conn() *pgConn {
	return c.handle.Conn()
}

// Release returns the connection to the pool. It is idempotent; releasing twice is
// a no-op rather than a double return that would corrupt pool accounting.
func (c *connWrapper) Release() {
	c.handle.Release()
}

// live reports whether the connection may still be used.
func (c *connWrapper) live() error {
	if c.handle.Released() {
		return ErrConnReleased
	}
	return nil
}

// Exec executes a statement that returns no rows.
func (c *connWrapper) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.conn().exec(ctx, sql, args)
}

// Query executes a query. The returned Rows do not own the connection: the caller
// still has it and is responsible for releasing it.
func (c *connWrapper) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.conn().query(ctx, sql, args, nil)
}

// QueryRow executes a query expected to return at most one row.
func (c *connWrapper) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	if err := c.live(); err != nil {
		return errorRow{err: err}
	}
	return c.conn().queryRow(ctx, sql, args, nil)
}

// Begin starts a transaction on this connection. The transaction does not own the
// connection; release it yourself once the transaction has finished.
func (c *connWrapper) Begin(ctx context.Context) (gpool.Tx, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.conn().begin(ctx)
}

// Ping verifies the connection is still usable.
func (c *connWrapper) Ping(ctx context.Context) error {
	if err := c.live(); err != nil {
		return err
	}
	return c.conn().ping(ctx)
}
