// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"iter"
)

// EventStream is a live stream of CDC events.
//
// Delivery is at-least-once. The stream confirms a WAL position to the server only
// after the iterator body for that event has returned, so a crash mid-processing
// replays the event rather than losing it. The corollary is that a consumer which
// hands work to another goroutine and returns immediately has confirmed work it has
// not done: either finish processing before returning from the loop body, or persist
// Event.LSN yourself and resume from it.
//
// Until a position is confirmed the server retains the WAL behind it, so a consumer
// that stops draining will grow the primary's disk usage.
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
