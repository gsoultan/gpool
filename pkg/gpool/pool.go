// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"context"
)

// Pool defines the interface for a connection pool.
type Pool interface {
	// Acquire returns a connection from the pool.
	Acquire(ctx context.Context) (Conn, error)
	// Close closes the pool and all its connections.
	Close()
	// Stat returns the current statistics of the pool.
	Stat() Stat
	// Exec executes a query without returning any rows.
	Exec(ctx context.Context, sql string, args ...any) (Result, error)
	// Query executes a query that returns rows.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	// QueryRow executes a query that is expected to return at most one row.
	QueryRow(ctx context.Context, sql string, args ...any) Row
}
