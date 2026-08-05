// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

// Subscriber is the full change data capture surface for one vendor.
//
// It composes three narrower interfaces so callers can depend only on what they
// use: Stream to read changes, TableManager to adjust the tracked table set, and
// ReplicationManager to administer slots and publications.
type Subscriber interface {
	Stream
	TableManager
	ReplicationManager

	// Close releases every resource the subscriber holds, including any open stream.
	// It is idempotent.
	Close() error
}
