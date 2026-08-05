// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"context"
)

// Conn defines the interface for a single database connection.
type Conn interface {
	// Release returns the connection to the pool.
	Release()
	// Exec executes a query without returning any rows.
	Exec(ctx context.Context, sql string, args ...any) (Result, error)
	// Query executes a query that returns rows.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	// QueryRow executes a query that is expected to return at most one row.
	QueryRow(ctx context.Context, sql string, args ...any) Row
	// Begin starts a transaction.
	Begin(ctx context.Context) (Tx, error)
	// Ping checks if the connection is still alive.
	Ping(ctx context.Context) error
}
