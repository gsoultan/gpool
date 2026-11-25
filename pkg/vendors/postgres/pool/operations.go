// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// The operations every gpool.Conn shares, on the pooled connection itself, so
// that a checked-out connection and the one AfterConnect is given behave alike.

func (c *pgConn) exec(ctx context.Context, sql string, args []any) (gpool.Result, error) {
	tag, err := c.session.exec(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	return pgResult{tag: tag}, nil
}

// query returns rows that release owner when closed, if it is set.
func (c *pgConn) query(ctx context.Context, sql string, args []any, owner releaser) (gpool.Rows, error) {
	cursor, err := c.session.query(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	return newRows(cursor, owner), nil
}

// queryRow returns a row that releases owner when finished, if it is set. An
// error is deferred to Scan, so the caller always gets a Row to finish.
//
// The row and its cursor are allocated together: a single-row query is the
// commonest call there is, and this is one allocation fewer on every one.
func (c *pgConn) queryRow(ctx context.Context, sql string, args []any, owner releaser) gpool.Row {
	row := new(cursorRow)
	if err := c.session.queryInto(ctx, sql, args, &row.cursor); err != nil {
		if owner != nil {
			owner.Release()
		}
		return errorRow{err: err}
	}
	row.pgRow.rows, row.pgRow.owner = &row.cursor, owner
	return &row.pgRow
}

// cursorRow is a pgRow and the cursor it reads, in one allocation.
type cursorRow struct {
	pgRow  pgRow
	cursor cursor
}

// begin starts a transaction for the caller holding the connection under held,
// or for the pool itself with none.
func (c *pgConn) begin(ctx context.Context, held lease) (gpool.Tx, error) {
	// No round trip: caduceus sends BEGIN with the transaction's first
	// statement.
	tx, err := c.session.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return newTx(sessionTx{tx: tx}, held), nil
}

// ping sends a comment, the cheapest statement there is.
func (c *pgConn) ping(ctx context.Context) error {
	_, err := c.session.exec(ctx, "-- ping", nil)
	return err
}

// detachedConn is the gpool.Conn given to AfterConnect: the connection before
// the pool owns it, so there is nothing to release.
type detachedConn struct {
	c *pgConn
}

var _ gpool.Conn = detachedConn{}

func (d detachedConn) Release() {}

func (d detachedConn) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	return d.c.exec(ctx, sql, args)
}

func (d detachedConn) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	return d.c.query(ctx, sql, args, nil)
}

func (d detachedConn) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	return d.c.queryRow(ctx, sql, args, nil)
}

func (d detachedConn) Begin(ctx context.Context) (gpool.Tx, error) { return d.c.begin(ctx, nil) }

func (d detachedConn) Ping(ctx context.Context) error { return d.c.ping(ctx) }
