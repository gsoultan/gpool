// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// Row defines the interface for a single row.
type Row interface {
	// Scan copies the columns from the row into the values pointed at by dest.
	Scan(dest ...any) error
	// Release returns the row object to the pool.
	Release()
}
