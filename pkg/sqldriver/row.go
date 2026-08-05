// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"fmt"
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// pgRow is a single-row result implementing gpool.Row.
//
// It holds the whole result set rather than one row's values, because a caller
// who decides not to read the row still has to be able to close the query. Both
// Scan and Release finish it, so either call is sufficient and both are safe.
type pgRow struct {
	rows *pgRows
	done atomic.Bool
}

var _ gpool.Row = (*pgRow)(nil)

func newRow(rows *pgRows) *pgRow {
	return &pgRow{rows: rows}
}

// Scan reads the single row into dest and finishes the row. It returns ErrNoRows
// when the query produced nothing.
func (r *pgRow) Scan(dest ...any) error {
	if !r.done.CompareAndSwap(false, true) {
		return ErrRowsClosed
	}
	defer r.rows.Close()

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// Release finishes the row without reading it, so the query is closed and the
// connection freed.
func (r *pgRow) Release() {
	if !r.done.CompareAndSwap(false, true) {
		return
	}
	r.rows.Close()
}

// rowCursor is a view over the row a result set is currently positioned on,
// yielded by Rows.All. It is a value type holding no ownership.
type rowCursor struct {
	rows *pgRows
}

var _ gpool.Row = rowCursor{}

// Scan copies the current row into dest.
func (r rowCursor) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Release is a no-op: the cursor owns nothing, and the enclosing iterator closes
// the result set when it ends.
func (r rowCursor) Release() {}

// errorRow defers a failure to Scan, so QueryRow can keep its single-return
// signature without ever handing back a nil Row.
type errorRow struct {
	err error
}

var _ gpool.Row = errorRow{}

func (r errorRow) Scan(...any) error { return r.err }
func (r errorRow) Release()          {}

func errArity(columns, destinations int) error {
	return fmt.Errorf("%w: %d destination(s) for %d column(s)", ErrScan, destinations, columns)
}
