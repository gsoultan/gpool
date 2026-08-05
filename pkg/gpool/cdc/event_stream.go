// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"iter"
)

// EventStream is a live stream of CDC events.
//
// Delivery is at-least-once. A position is treated as processed only after the
// iterator body for that event has returned, so a crash mid-processing replays
// the event rather than losing it. The corollary is that a consumer which hands
// work to another goroutine and returns immediately has confirmed work it has not
// done: either finish processing before returning from the loop body, or record
// Event.Position yourself and resume from it with Stream.SubscribeFrom.
//
// What falling behind costs depends on the source, and the two failure modes are
// opposites. Where the server tracks each consumer's position — a PostgreSQL
// replication slot — the log is retained until the position advances, so a
// consumer that stops draining grows the primary's disk. Where it does not — a
// MySQL binlog expires on age and size regardless of who is reading — falling far
// enough behind means the changes are gone. Each vendor's package documents which
// of the two it is.
type EventStream interface {
	// All returns an iterator over the stream's events. It blocks waiting for
	// changes and ends when the stream is closed or fails. Only one call to All
	// may be in flight at a time; a second concurrent call yields nothing.
	All() iter.Seq[Event]
	// Close stops the stream and releases its connection. It is idempotent, so it
	// is safe to defer even though All closes the stream when iteration ends.
	Close() error
	// Err returns the error that terminated the stream, or nil if it ended cleanly.
	// It is safe to call from another goroutine while All is running.
	Err() error
}
