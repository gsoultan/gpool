// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"sync/atomic"

	"github.com/gsoultan/nextar-engine/pkg/gpool"
)

// pgRow is a single-row result implementing gpool.Row.
//
// It holds the full result set rather than a single row, so a caller who decides
// not to read the row can still close it. Returning that connection to the pool
// unclosed would hand the next caller a connection stuck mid-query.
//
// owner is set only when the row was produced by a pool-level QueryRow, in which
// case finishing the row is what returns the connection to the pool. Both Scan and
// Release finish it, so either call is sufficient and both together are safe.
type pgRow struct {
	rows  resultSet
	owner releaser
	done  atomic.Bool
}

var _ gpool.Row = (*pgRow)(nil)

func newRow(rows resultSet, owner releaser) *pgRow {
	return &pgRow{rows: rows, owner: owner}
}

// Scan reads the single row into dest and finishes the row. It returns ErrNoRows,
// which is sql.ErrNoRows, when the query produced nothing.
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
		return ErrNoRows
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
