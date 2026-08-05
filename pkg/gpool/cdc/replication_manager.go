// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
)

// ReplicationManager administers the server-side objects a subscription depends on.
//
// These are destructive, privileged operations. Dropping a replication slot discards
// the WAL position it was holding, so a subscriber that later reconnects to that slot
// name resumes from wherever the new slot is created, not from where the old one left off.
type ReplicationManager interface {
	// CreateSlot creates a logical replication slot. Creating an existing slot is a no-op.
	CreateSlot(ctx context.Context, name string) error
	// DropSlot drops a logical replication slot. Dropping a missing slot is a no-op.
	DropSlot(ctx context.Context, name string) error
	// CreatePublication creates a publication for the given tables. Creating an
	// existing publication is a no-op and does not reconcile its table list.
	CreatePublication(ctx context.Context, name string, tables ...string) error
	// DropPublication drops a publication. Dropping a missing publication is a no-op.
	DropPublication(ctx context.Context, name string) error
}
