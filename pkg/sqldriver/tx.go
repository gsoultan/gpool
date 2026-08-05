// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"context"
	"database/sql/driver"
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// pgTx is a transaction implementing gpool.Tx.
//
// Commit and Rollback both settle it exactly once. The canonical Go idiom pairs a
// deferred Rollback with a Commit on the happy path, so the second call is
// expected: it returns ErrTxClosed and touches nothing.
//
// Statements inside the transaction go out on the connection, because that is
// what a database/sql driver's transaction is — the connection is in a
// transactional state, not a separate object to route through.
type pgTx struct {
	tx    driver.Tx
	owner *connWrapper
	done  atomic.Bool
}

var _ gpool.Tx = (*pgTx)(nil)

func newTx(tx driver.Tx, owner *connWrapper) *pgTx {
	return &pgTx{tx: tx, owner: owner}
}

// Commit commits the transaction.
func (t *pgTx) Commit(context.Context) error {
	if !t.done.CompareAndSwap(false, true) {
		return ErrTxClosed
	}
	defer t.owner.conn().settled()

	return t.tx.Commit()
}

// Rollback rolls the transaction back. Called after a successful Commit it
// returns ErrTxClosed, which a deferred rollback can safely ignore.
func (t *pgTx) Rollback(context.Context) error {
	if !t.done.CompareAndSwap(false, true) {
		return ErrTxClosed
	}
	defer t.owner.conn().settled()

	return t.tx.Rollback()
}

// Exec executes a statement inside the transaction.
func (t *pgTx) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	if t.done.Load() {
		return nil, ErrTxClosed
	}
	return t.owner.Exec(ctx, sql, args...)
}

// Query executes a query inside the transaction.
func (t *pgTx) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	if t.done.Load() {
		return nil, ErrTxClosed
	}
	return t.owner.Query(ctx, sql, args...)
}

// QueryRow executes a query inside the transaction expected to return at most one row.
func (t *pgTx) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	if t.done.Load() {
		return errorRow{err: ErrTxClosed}
	}
	return t.owner.QueryRow(ctx, sql, args...)
}
