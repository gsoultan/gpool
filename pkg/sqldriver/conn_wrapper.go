// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"context"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/pooling"
)

// connWrapper is a checked-out connection implementing gpool.Conn.
//
// One is allocated per acquisition and never recycled: its lifetime is controlled
// by user code, so pooling it would let a second Release hand a live connection
// back while another goroutine is still using it.
type connWrapper struct {
	handle pooling.Handle[*conn]
}

var _ gpool.Conn = (*connWrapper)(nil)

func newConnWrapper(handle pooling.Handle[*conn]) *connWrapper {
	return &connWrapper{handle: handle}
}

// conn returns the underlying pooled connection.
func (c *connWrapper) conn() *conn {
	return c.handle.Conn()
}

// live reports whether the connection may still be used.
func (c *connWrapper) live() error {
	if c.handle.Released() {
		return ErrConnReleased
	}
	return nil
}

// Release returns the connection to the pool. It is idempotent.
func (c *connWrapper) Release() {
	c.handle.Release()
}

// Exec executes a statement that returns no rows.
func (c *connWrapper) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	if err := c.live(); err != nil {
		return nil, err
	}

	result, err := c.conn().exec(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	return pgResult{result: result}, nil
}

// Query executes a query. The returned Rows do not own the connection: the caller
// still has it and is responsible for releasing it.
func (c *connWrapper) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.queryOwned(ctx, nil, sql, args)
}

// queryOwned runs a query, optionally handing ownership of the connection to the
// returned rows so that closing them releases it.
func (c *connWrapper) queryOwned(ctx context.Context, owner *connWrapper, sql string, args []any) (gpool.Rows, error) {
	rows, release, err := c.conn().query(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	return newRows(rows, owner, release), nil
}

// QueryRow executes a query expected to return at most one row.
func (c *connWrapper) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	if err := c.live(); err != nil {
		return errorRow{err: err}
	}

	rows, err := c.queryOwned(ctx, nil, sql, args)
	if err != nil {
		return errorRow{err: err}
	}
	return newRow(rows.(*pgRows))
}

// Begin starts a transaction on this connection. The transaction does not own the
// connection; release it yourself once the transaction has finished.
func (c *connWrapper) Begin(ctx context.Context) (gpool.Tx, error) {
	if err := c.live(); err != nil {
		return nil, err
	}

	tx, err := c.conn().begin(ctx)
	if err != nil {
		return nil, err
	}
	return newTx(tx, c), nil
}

// Ping verifies the connection is still usable. Drivers that do not implement
// Pinger fall back to a trivial round trip.
func (c *connWrapper) Ping(ctx context.Context) error {
	if err := c.live(); err != nil {
		return err
	}
	return c.conn().ping(ctx)
}
