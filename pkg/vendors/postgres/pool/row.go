// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

// pgRow is a single-row result implementing gpool.Row.
//
// It holds the full result set rather than a pgx.Row, because pgx.Row exposes only
// Scan and closes the underlying result as a side effect of scanning. A caller who
// decides not to read the row would then have no way to close it, and returning
// that connection to the pool hands the next caller a connection stuck mid-query.
//
// owner is set only when the row was produced by a pool-level QueryRow, in which
// case finishing the row is what returns the connection to the pool. Both Scan and
// Release finish it, so either call is sufficient and both together are safe.
type pgRow struct {
	rows  pgx.Rows
	owner *connWrapper
	done  atomic.Bool
}

var _ gpool.Row = (*pgRow)(nil)

func newRow(rows pgx.Rows, owner *connWrapper) *pgRow {
	return &pgRow{rows: rows, owner: owner}
}

// Scan reads the single row into dest and finishes the row. It returns pgx.ErrNoRows
// when the query produced nothing, matching pgx's own QueryRow semantics.
func (r *pgRow) Scan(dest ...any) error {
	if !r.done.CompareAndSwap(false, true) {
		return ErrRowsClosed
	}
	defer r.finish()

	if err := r.rows.Err(); err != nil {
		return err
	}
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}

	r.rows.Close()
	return r.rows.Err()
}

// Release finishes the row without reading it. Use it on the paths where you decide
// not to consume the result, so the query is closed and the connection freed.
func (r *pgRow) Release() {
	if !r.done.CompareAndSwap(false, true) {
		return
	}
	r.finish()
}

// finish closes the result set and hands the connection back, in that order: the
// connection is not reusable until its in-flight query is closed.
func (r *pgRow) finish() {
	r.rows.Close()
	if r.owner != nil {
		r.owner.Release()
	}
}
