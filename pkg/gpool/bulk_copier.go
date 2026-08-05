// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"context"
)

// CopyRequest describes one bulk copy. The fields are grouped into a struct rather
// than passed separately so CopyFrom keeps a readable signature as it grows.
type CopyRequest struct {
	// Table is the destination, given as its parts: {"public", "users"} or
	// {"users"}. Splitting it here rather than parsing a dotted string means a
	// table whose name legitimately contains a dot is still addressable.
	Table []string
	// Columns is the destination column list, in the order Values returns them.
	Columns []string
	// Rows is the source.
	Rows CopyRows
}

// BulkCopier loads many rows in one pass using the PostgreSQL COPY protocol,
// which is far faster than the equivalent INSERTs.
//
// It is an optional capability, kept out of Conn and Pool so neither grows past
// what every caller needs. Reach it with a type assertion:
//
//	if copier, ok := pool.(gpool.BulkCopier); ok {
//	    n, err := copier.CopyFrom(ctx, req)
//	}
type BulkCopier interface {
	// CopyFrom streams rows into the destination table, returning the number
	// copied. A failure part-way rolls the whole copy back: COPY is atomic.
	CopyFrom(ctx context.Context, request CopyRequest) (int64, error)
}
