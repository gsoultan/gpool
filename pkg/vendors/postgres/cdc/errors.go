// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import "errors"

var (
	// ErrClosed is returned when a subscriber is used after Close.
	ErrClosed = errors.New("gpool/postgres/cdc: subscriber is closed")

	// ErrAlreadySubscribed is returned by Subscribe when a stream is already open.
	// One walsender connection carries one stream; close the current one first.
	ErrAlreadySubscribed = errors.New("gpool/postgres/cdc: already subscribed")

	// ErrNoTables is returned when an operation needs at least one table and got none.
	ErrNoTables = errors.New("gpool/postgres/cdc: no tables specified")

	// ErrInvalidConfig is returned by New when the configuration cannot be used.
	ErrInvalidConfig = errors.New("gpool/postgres/cdc: invalid config")
)
