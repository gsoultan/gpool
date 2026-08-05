// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"context"
	"database/sql/driver"
)

// conn is the pooled connection together with the per-connection state the
// adapter needs. The engine is generic over this type, so whatever a vendor has
// to remember about a connection lives here rather than in a side table.
type conn struct {
	driver driver.Conn

	// tx is the open transaction, if any. The driver does not tell us whether one
	// is in flight, and a connection returned mid-transaction would carry the
	// previous caller's locks and snapshot into the next one — so the handle is
	// kept here, where release can reach it after the caller has walked away.
	tx driver.Tx
}

// exec runs a statement that returns no rows.
//
// A driver may implement ExecerContext directly, or only the older Stmt path.
// Preparing and closing a statement costs an extra round trip, so it is the
// fallback rather than the default.
func (c *conn) exec(ctx context.Context, query string, args []any) (driver.Result, error) {
	values, err := convertArgs(c.driver, args)
	if err != nil {
		return nil, err
	}

	if execer, ok := c.driver.(driver.ExecerContext); ok {
		result, err := execer.ExecContext(ctx, query, values)
		if err != driver.ErrSkip {
			return result, err
		}
	}

	stmt, err := c.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	execer, ok := stmt.(driver.StmtExecContext)
	if !ok {
		return nil, ErrUnsupported
	}
	return execer.ExecContext(ctx, values)
}

// query runs a statement that returns rows. When it prepares a statement, the
// statement's lifetime is tied to the returned rows.
func (c *conn) query(ctx context.Context, query string, args []any) (driver.Rows, func(), error) {
	values, err := convertArgs(c.driver, args)
	if err != nil {
		return nil, nil, err
	}

	if queryer, ok := c.driver.(driver.QueryerContext); ok {
		rows, err := queryer.QueryContext(ctx, query, values)
		if err != driver.ErrSkip {
			return rows, nil, err
		}
	}

	stmt, err := c.prepare(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	queryer, ok := stmt.(driver.StmtQueryContext)
	if !ok {
		stmt.Close()
		return nil, nil, ErrUnsupported
	}

	rows, err := queryer.QueryContext(ctx, values)
	if err != nil {
		stmt.Close()
		return nil, nil, err
	}
	// The statement must outlive the rows it produced.
	return rows, func() { stmt.Close() }, nil
}

func (c *conn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := c.driver.(driver.ConnPrepareContext); ok {
		return preparer.PrepareContext(ctx, query)
	}
	return c.driver.Prepare(query)
}

// begin starts a transaction and records that one is open.
func (c *conn) begin(ctx context.Context) (driver.Tx, error) {
	beginner, ok := c.driver.(driver.ConnBeginTx)
	if !ok {
		return nil, ErrUnsupported
	}

	tx, err := beginner.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return tx, nil
}

// settled records that the caller finished with the transaction, so release has
// nothing left to unwind.
func (c *conn) settled() {
	c.tx = nil
}

// ping verifies the connection is usable, falling back to a trivial statement for
// a driver that does not implement Pinger.
func (c *conn) ping(ctx context.Context) error {
	if pinger, ok := c.driver.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}

	_, err := c.exec(ctx, "SELECT 1", nil)
	return err
}

// dead reports whether the connection can no longer carry traffic. Drivers that
// implement Validator know; the rest are assumed live until they fail.
func (c *conn) dead() bool {
	if validator, ok := c.driver.(driver.Validator); ok {
		return !validator.IsValid()
	}
	return false
}

// reset returns the connection to a clean state, reporting whether it succeeded.
//
// ResetSession is the hook database/sql itself calls before reusing a connection,
// so a driver that implements it already knows what has to be cleared.
func (c *conn) reset(ctx context.Context) bool {
	// A caller that returned a connection without settling its transaction must
	// not leak it onward. Unwinding costs one round trip; replacing the connection
	// costs a full reconnect.
	if c.tx != nil {
		if err := c.tx.Rollback(); err != nil {
			return false
		}
		c.tx = nil
	}

	if resetter, ok := c.driver.(driver.SessionResetter); ok {
		if err := resetter.ResetSession(ctx); err != nil {
			return false
		}
	}
	return true
}
