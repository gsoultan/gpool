// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
)

// TableManager controls which tables a subscription tracks.
//
// Table names may be schema-qualified ("public.users"). An unqualified name is
// resolved by the server against its search_path.
type TableManager interface {
	// AddTables adds tables to the subscription. Tables already tracked are ignored.
	AddTables(ctx context.Context, tables ...string) error
	// RemoveTables removes tables from the subscription. Tables not tracked are ignored.
	RemoveTables(ctx context.Context, tables ...string) error
	// SyncTables reconciles the subscription to exactly the given list, adding
	// missing tables and removing extra ones.
	SyncTables(ctx context.Context, tables ...string) error
	// IsTracking reports whether the table is in the subscriber's local tracking list.
	IsTracking(table string) bool
	// GetTables returns a copy of the subscriber's local tracking list.
	GetTables() []string
	// VerifyTable reports whether the table is tracked by the publication in the database.
	VerifyTable(ctx context.Context, table string) (bool, error)
}
