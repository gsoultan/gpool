// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// CopyRows is a forward-only source of rows for a bulk copy.
//
// It is a cursor rather than a slice so a caller can stream a dataset larger than
// memory: rows are pulled one at a time and written straight to the wire.
type CopyRows interface {
	// Next advances to the next row, reporting false when the source is exhausted.
	Next() bool
	// Values returns the current row's column values, in the order given by
	// CopyRequest.Columns.
	Values() ([]any, error)
	// Err returns the error that ended iteration, if any.
	Err() error
}

// CopyFromSlice adapts an indexed callback into CopyRows, for the common case of
// copying from something already in memory.
//
//	gpool.CopyFromSlice(len(users), func(i int) ([]any, error) {
//	    return []any{users[i].ID, users[i].Email}, nil
//	})
func CopyFromSlice(length int, next func(index int) ([]any, error)) CopyRows {
	return &sliceRows{length: length, next: next, index: -1}
}

// sliceRows is the CopyRows returned by CopyFromSlice.
type sliceRows struct {
	length int
	next   func(int) ([]any, error)
	index  int
	err    error
}

var _ CopyRows = (*sliceRows)(nil)

func (r *sliceRows) Next() bool {
	if r.err != nil {
		return false
	}
	r.index++
	return r.index < r.length
}

func (r *sliceRows) Values() ([]any, error) {
	values, err := r.next(r.index)
	if err != nil {
		r.err = err
		return nil, err
	}
	return values, nil
}

func (r *sliceRows) Err() error {
	return r.err
}
