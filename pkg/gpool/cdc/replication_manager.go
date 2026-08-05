// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
)

// ReplicationManager administers the server-side objects a subscription depends on.
//
// This is an optional capability, not part of Subscriber: slots and publications
// are PostgreSQL's model, and an engine that streams its log without any
// per-consumer server object — MySQL's binlog — has nothing to administer. Reach
// it by type assertion, and treat a failed assertion as "this vendor has no such
// objects" rather than as an error:
//
//	if slots, ok := subscriber.(cdc.ReplicationManager); ok {
//		err := slots.CreateSlot(ctx, "orders")
//	}
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
