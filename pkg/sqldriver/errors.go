// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import "errors"

var (
	// ErrPoolClosed is returned by Acquire once Close has been called.
	ErrPoolClosed = errors.New("gpool/sqldriver: pool is closed")

	// ErrConnReleased is returned when a Conn is used after Release.
	ErrConnReleased = errors.New("gpool/sqldriver: connection already released")

	// ErrTxClosed is returned when a Tx is used, committed, or rolled back after it
	// has finished. A deferred Rollback after a successful Commit returns this and
	// can be ignored.
	ErrTxClosed = errors.New("gpool/sqldriver: transaction already closed")

	// ErrRowsClosed is returned when Rows are used after Close.
	ErrRowsClosed = errors.New("gpool/sqldriver: rows already closed")

	// ErrNoRows is returned by Row.Scan when the query produced no rows.
	ErrNoRows = errors.New("gpool/sqldriver: no rows in result set")

	// ErrInvalidConfig is returned by New when the configuration cannot be used.
	ErrInvalidConfig = errors.New("gpool/sqldriver: invalid config")

	// ErrScan is returned when a value cannot be assigned to a scan destination.
	ErrScan = errors.New("gpool/sqldriver: cannot scan value")

	// ErrConvertArgument is returned when a query argument cannot be converted
	// into something the driver accepts.
	ErrConvertArgument = errors.New("gpool/sqldriver: cannot convert argument")

	// ErrUnsupported is returned when the driver does not implement an optional
	// capability the call requires. Not every driver supports every operation.
	ErrUnsupported = errors.New("gpool/sqldriver: driver does not support this operation")
)
