// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"database/sql/driver"
	"errors"
	"io"
	"iter"
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// pgRows is a result set implementing gpool.Rows.
//
// owner is set only when the result set was produced by a pool-level Query, in
// which case closing the rows is what returns the connection to the pool.
type pgRows struct {
	rows    driver.Rows
	owner   *connWrapper
	release func() // closes a prepared statement the rows depend on, if any

	values []driver.Value
	err    error
	closed atomic.Bool
}

var _ gpool.Rows = (*pgRows)(nil)

func newRows(rows driver.Rows, owner *connWrapper, release func()) *pgRows {
	return &pgRows{
		rows:    rows,
		owner:   owner,
		release: release,
		values:  make([]driver.Value, len(rows.Columns())),
	}
}

// Close releases the result set and, when the rows own it, the connection.
// It is idempotent, so deferring Close alongside a range over All is safe.
func (r *pgRows) Close() {
	if !r.closed.CompareAndSwap(false, true) {
		return
	}

	if err := r.rows.Close(); err != nil && r.err == nil {
		r.err = err
	}
	if r.release != nil {
		r.release()
	}
	if r.owner != nil {
		r.owner.Release()
	}
}

// Err returns the error that ended iteration, if any.
func (r *pgRows) Err() error {
	return r.err
}

// Next advances to the next row, reporting false once the set is exhausted,
// closed, or has failed.
func (r *pgRows) Next() bool {
	if r.closed.Load() || r.err != nil {
		return false
	}

	// A driver signals exhaustion with io.EOF, which is not a failure.
	switch err := r.rows.Next(r.values); {
	case errors.Is(err, io.EOF):
		return false
	case err != nil:
		r.err = err
		return false
	default:
		return true
	}
}

// Scan copies the current row into dest.
func (r *pgRows) Scan(dest ...any) error {
	if r.closed.Load() {
		return ErrRowsClosed
	}
	return scanInto(r.values, dest)
}

// FieldDescriptions returns column metadata. A database/sql driver exposes only
// column names, so the type fields are left zero rather than invented.
func (r *pgRows) FieldDescriptions() []gpool.Field {
	names := r.rows.Columns()

	fields := make([]gpool.Field, len(names))
	for i, name := range names {
		fields[i] = gpool.Field{Name: name}
	}
	return fields
}

// RawValues returns the current row rendered as bytes. Unlike a native driver
// there is no untouched wire buffer to hand back, so these are converted values.
func (r *pgRows) RawValues() [][]byte {
	raw := make([][]byte, len(r.values))
	for i, value := range r.values {
		if value == nil {
			continue
		}
		raw[i] = []byte(stringify(value))
	}
	return raw
}

// CommandTag is empty: a database/sql driver reports no tag for a query.
func (r *pgRows) CommandTag() string {
	return ""
}

// All iterates the result set, closing it when iteration ends by any path.
//
// The yielded Row is a cursor over the current row, reused for every iteration
// and invalid once the loop advances. Scan what you need inside the loop body
// rather than retaining the Row.
func (r *pgRows) All() iter.Seq[gpool.Row] {
	return func(yield func(gpool.Row) bool) {
		defer r.Close()

		cursor := rowCursor{rows: r}
		for r.Next() {
			if !yield(cursor) {
				return
			}
		}
	}
}

// scanInto assigns a row's values to the caller's destinations.
func scanInto(values []driver.Value, dest []any) error {
	if len(dest) != len(values) {
		return errArity(len(values), len(dest))
	}
	for i, d := range dest {
		if err := assign(d, values[i]); err != nil {
			return err
		}
	}
	return nil
}
