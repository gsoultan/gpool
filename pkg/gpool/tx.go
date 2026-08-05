// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"context"
)

// Tx defines the interface for a database transaction.
type Tx interface {
	// Commit commits the transaction.
	Commit(ctx context.Context) error
	// Rollback rolls back the transaction.
	Rollback(ctx context.Context) error
	// Exec executes a query within the transaction.
	Exec(ctx context.Context, sql string, args ...any) (Result, error)
	// Query executes a query within the transaction.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	// QueryRow executes a query within the transaction that is expected to return at most one row.
	QueryRow(ctx context.Context, sql string, args ...any) Row
}
