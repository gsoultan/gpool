// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// Result defines the interface for the result of an Exec operation.
type Result interface {
	// RowsAffected returns the number of rows affected by the query.
	RowsAffected() int64
	// Release returns the result object to the pool.
	Release()
}
