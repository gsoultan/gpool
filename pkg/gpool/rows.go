// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"iter"
)

// Field defines the metadata for a single column.
type Field struct {
	Name                 string
	TableOID             uint32
	TableAttributeNumber uint16
	DataTypeOID          uint32
	DataTypeSize         int16
	TypeModifier         int32
	Format               int16
}

// Rows defines the interface for multiple rows returned by a query.
type Rows interface {
	// Close closes the rows iterator.
	Close()
	// Err returns the error, if any, that occurred during iteration.
	Err() error
	// Next prepares the next row for reading with Scan.
	Next() bool
	// Scan copies the columns from the current row into the values pointed at by dest.
	Scan(dest ...any) error
	// FieldDescriptions returns the metadata for the columns.
	FieldDescriptions() []Field
	// RawValues returns the raw bytes for the current row's columns.
	RawValues() [][]byte
	// CommandTag returns the command tag from the query.
	CommandTag() string
	// All returns an iterator over all rows. (Go 1.26+)
	All() iter.Seq[Row]
}
