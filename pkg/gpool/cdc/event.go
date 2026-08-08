// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import "time"

// Op represents the type of database operation in a CDC event.
type Op int8

const (
	OpInsert Op = iota
	OpUpdate
	OpDelete
)

// String returns the SQL name of the operation.
func (o Op) String() string {
	switch o {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Event represents a single Change Data Capture event.
//
// Before and After are owned by the receiver: they are freshly allocated per event
// and are never reused by the stream, so they are safe to retain and to hand to
// another goroutine.
//
// A column that is present in the table but absent from the map was not transmitted
// by the server. For Update and Delete this means the table's REPLICA IDENTITY does
// not cover it; for a TOASTed column it means the value did not change. An absent key
// is therefore distinct from a key present with a nil value, which is a real SQL NULL.
type Event struct {
	// Op is the type of operation (Insert, Update, Delete).
	Op Op
	// Schema is the name of the database schema.
	Schema string
	// Table is the name of the database table.
	Table string
	// Position marks this change's place in the source's change log. Record it
	// and hand it to Stream.SubscribeFrom to resume from here.
	//
	// Resuming starts at or before the change the position came from, never after
	// it: a resumed stream may repeat changes it already delivered, but does not
	// skip any. That is what carries at-least-once delivery across a restart.
	// Exactly where the boundary falls is the vendor's business — PostgreSQL
	// replays from the transaction containing the change, MySQL from the start of
	// it — so a consumer that must not process a change twice needs its own
	// idempotency, not a tighter position.
	Position Position
	// Transaction identifies the transaction this change belongs to. Changes with
	// equal values were committed together; changes with different values were
	// not. That is the whole contract — it is a Position, so it is opaque, and
	// nothing else about it is meaningful.
	//
	// This is what a consumer needs to replay a batch atomically rather than one
	// row at a time. Note that a transaction can span more than one delivery: a
	// stream may end mid-transaction and resume, and both halves carry the same
	// value, so a consumer that must apply whole transactions needs to see the
	// value change before it commits its own.
	//
	// It is the zero Position where the source does not report transaction
	// boundaries.
	Transaction Position

	// Timestamp is when the transaction containing this change committed, as the
	// source recorded it. Every change from one transaction carries the same value.
	//
	// This is the source's clock, not the consumer's, and the two are not the same
	// clock: use it to reason about the order and age of changes at the origin, not
	// to measure how long anything took to arrive. Resolution differs by vendor —
	// PostgreSQL records microseconds, MySQL whole seconds — so equal timestamps do
	// not mean simultaneous. It is the zero Time if the source did not report one.
	Timestamp time.Time

	// Before contains the row data before the change (Update and Delete only, and
	// only for columns covered by the table's REPLICA IDENTITY).
	Before map[string]any
	// After contains the row data after the change (Insert and Update only).
	After map[string]any
}
