// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"iter"
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

// pgRows is a query result set implementing gpool.Rows.
//
// owner is set only when the result set was produced by a pool-level Query, in
// which case closing the rows is what returns the connection to the pool.
type pgRows struct {
	rows   pgx.Rows
	owner  *connWrapper
	closed atomic.Bool
}

var _ gpool.Rows = (*pgRows)(nil)

func newRows(rows pgx.Rows, owner *connWrapper) *pgRows {
	return &pgRows{rows: rows, owner: owner}
}

// Close releases the result set and, when the rows own it, the connection.
// It is idempotent, so deferring Close alongside a range over All is safe.
func (r *pgRows) Close() {
	if !r.closed.CompareAndSwap(false, true) {
		return
	}

	r.rows.Close()
	if r.owner != nil {
		r.owner.Release()
	}
}

// Err returns the error that ended iteration, if any. It stays valid after Close,
// which is where a failed query surfaces.
func (r *pgRows) Err() error {
	return r.rows.Err()
}

// Next advances to the next row, reporting false once the set is exhausted or closed.
func (r *pgRows) Next() bool {
	if r.closed.Load() {
		return false
	}
	return r.rows.Next()
}

// Scan copies the current row into dest.
func (r *pgRows) Scan(dest ...any) error {
	if r.closed.Load() {
		return ErrRowsClosed
	}
	return r.rows.Scan(dest...)
}

// FieldDescriptions returns column metadata for the result set.
func (r *pgRows) FieldDescriptions() []gpool.Field {
	pgFields := r.rows.FieldDescriptions()

	fields := make([]gpool.Field, len(pgFields))
	for i, f := range pgFields {
		fields[i] = gpool.Field{
			Name:                 f.Name,
			TableOID:             f.TableOID,
			TableAttributeNumber: f.TableAttributeNumber,
			DataTypeOID:          f.DataTypeOID,
			DataTypeSize:         f.DataTypeSize,
			TypeModifier:         f.TypeModifier,
			Format:               f.Format,
		}
	}
	return fields
}

// RawValues returns the undecoded bytes of the current row. The slices alias the
// connection's read buffer and are only valid until the next call to Next.
func (r *pgRows) RawValues() [][]byte {
	return r.rows.RawValues()
}

// CommandTag returns the command tag, which is only populated once the rows are closed.
func (r *pgRows) CommandTag() string {
	return r.rows.CommandTag().String()
}

// All iterates the result set, closing it when iteration ends by any path.
//
// The yielded Row is a cursor over the current row, reused for every iteration and
// invalid once the loop advances. Scan what you need inside the loop body rather
// than retaining the Row.
func (r *pgRows) All() iter.Seq[gpool.Row] {
	return func(yield func(gpool.Row) bool) {
		defer r.Close()

		cursor := rowCursor{rows: r.rows}
		for r.Next() {
			if !yield(cursor) {
				return
			}
		}
	}
}
