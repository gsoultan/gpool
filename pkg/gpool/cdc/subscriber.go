// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

// Subscriber is the change data capture surface every vendor can provide.
//
// It composes the two narrower interfaces so callers can depend only on what
// they use: Stream to read changes, TableManager to adjust the tracked table set.
//
// Administering the source's own subscription objects is deliberately not here.
// Replication slots and publications are PostgreSQL's, and a vendor whose engine
// has no equivalent — MySQL has none at all — would have to implement those
// methods to return an error, which turns a compile-time mismatch into a runtime
// one. It is an optional capability instead, reached the same way BulkCopier and
// Notifier are:
//
//	if slots, ok := subscriber.(cdc.ReplicationManager); ok {
//		err := slots.CreateSlot(ctx, "orders")
//	}
type Subscriber interface {
	Stream
	TableManager

	// Close releases every resource the subscriber holds, including any open stream.
	// It is idempotent.
	Close() error
}
