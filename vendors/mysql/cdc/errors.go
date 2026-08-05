// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import "errors"

var (
	// ErrClosed is returned when a subscriber is used after Close.
	ErrClosed = errors.New("gpool/mysql/cdc: subscriber is closed")

	// ErrAlreadySubscribed is returned by Subscribe when a stream is already open.
	// One binlog connection carries one stream; close the current one first.
	ErrAlreadySubscribed = errors.New("gpool/mysql/cdc: already subscribed")

	// ErrNoTables is returned when an operation needs at least one table and got none.
	ErrNoTables = errors.New("gpool/mysql/cdc: no tables specified")

	// ErrInvalidConfig is returned by New when the configuration cannot be used.
	ErrInvalidConfig = errors.New("gpool/mysql/cdc: invalid config")

	// ErrInvalidPosition is returned when a position did not come from this vendor.
	ErrInvalidPosition = errors.New("gpool/mysql/cdc: invalid position")

	// ErrSchemaMismatch is returned when the live table definition has a different
	// number of columns than the binlog row being decoded.
	//
	// It means the table was altered after the row was written, so the column
	// names on record no longer describe it. Guessing would hand the consumer
	// values under the wrong names, which is worse than stopping.
	ErrSchemaMismatch = errors.New("gpool/mysql/cdc: table definition does not match the binlog row")
)
