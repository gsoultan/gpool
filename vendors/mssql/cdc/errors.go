// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import "errors"

var (
	// ErrClosed is returned when a subscriber is used after Close.
	ErrClosed = errors.New("gpool/mssql/cdc: subscriber is closed")

	// ErrAlreadySubscribed is returned by Subscribe when a stream is already open.
	ErrAlreadySubscribed = errors.New("gpool/mssql/cdc: already subscribed")

	// ErrNoTables is returned when an operation needs at least one table and got none.
	ErrNoTables = errors.New("gpool/mssql/cdc: no tables specified")

	// ErrInvalidConfig is returned by New when the configuration cannot be used.
	ErrInvalidConfig = errors.New("gpool/mssql/cdc: invalid config")

	// ErrInvalidPosition is returned when a position did not come from this vendor.
	ErrInvalidPosition = errors.New("gpool/mssql/cdc: invalid position")

	// ErrPositionExpired is returned by SubscribeFrom for a position the cleanup
	// job has already discarded.
	//
	// SQL Server retains changes for a fixed window — three days by default —
	// rather than until a consumer confirms them. Reading from before the window
	// would silently begin at whatever is left, which is a stream with a hole in
	// it that looks complete.
	ErrPositionExpired = errors.New("gpool/mssql/cdc: position is older than the retained change history")

	// ErrCDCNotEnabled is returned when the database has no change data capture.
	ErrCDCNotEnabled = errors.New("gpool/mssql/cdc: change data capture is not enabled on this database")
)
