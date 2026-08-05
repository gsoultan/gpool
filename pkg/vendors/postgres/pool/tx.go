// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

// pgTx is a database transaction implementing gpool.Tx.
//
// Commit and Rollback both settle the transaction exactly once. The canonical Go
// idiom pairs a deferred Rollback with a Commit on the happy path, so the second
// call is expected: it returns ErrTxClosed and touches nothing.
type pgTx struct {
	tx   pgx.Tx
	done atomic.Bool
}

var _ gpool.Tx = (*pgTx)(nil)

func newTx(tx pgx.Tx) *pgTx {
	return &pgTx{tx: tx}
}

// Commit commits the transaction.
func (t *pgTx) Commit(ctx context.Context) error {
	if !t.done.CompareAndSwap(false, true) {
		return ErrTxClosed
	}
	return t.tx.Commit(ctx)
}

// Rollback rolls the transaction back. Called after a successful Commit it returns
// ErrTxClosed, which a deferred rollback can safely ignore.
func (t *pgTx) Rollback(ctx context.Context) error {
	if !t.done.CompareAndSwap(false, true) {
		return ErrTxClosed
	}
	return t.tx.Rollback(ctx)
}

// Exec executes a statement inside the transaction.
func (t *pgTx) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	if t.done.Load() {
		return nil, ErrTxClosed
	}

	tag, err := t.tx.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgResult{tag: tag}, nil
}

// Query executes a query inside the transaction. The returned Rows never own a
// connection: the transaction's connection belongs to whoever acquired it.
func (t *pgTx) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	if t.done.Load() {
		return nil, ErrTxClosed
	}

	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return newRows(rows, nil), nil
}

// QueryRow executes a query inside the transaction expected to return at most one row.
func (t *pgTx) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	if t.done.Load() {
		return errorRow{err: ErrTxClosed}
	}

	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		closeRows(rows)
		return errorRow{err: err}
	}
	return newRow(rows, nil)
}
