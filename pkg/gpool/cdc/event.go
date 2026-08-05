// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

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
	// LSN is the WAL position immediately after the record that produced this event.
	// Persist it if you need to resume from a known position.
	LSN uint64
	// Before contains the row data before the change (Update and Delete only, and
	// only for columns covered by the table's REPLICA IDENTITY).
	Before map[string]any
	// After contains the row data after the change (Insert and Update only).
	After map[string]any
}
