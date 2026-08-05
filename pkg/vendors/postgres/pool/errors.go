// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import "errors"

var (
	// ErrPoolClosed is returned by Acquire and the pool-level query helpers once Close has been called.
	ErrPoolClosed = errors.New("gpool/postgres: pool is closed")

	// ErrConnReleased is returned when a Conn is used after Release.
	ErrConnReleased = errors.New("gpool/postgres: connection already released")

	// ErrTxClosed is returned when a Tx is used, committed, or rolled back after it has finished.
	// A deferred Rollback after a successful Commit returns this and can be ignored.
	ErrTxClosed = errors.New("gpool/postgres: transaction already closed")

	// ErrRowsClosed is returned when Rows are used after Close.
	ErrRowsClosed = errors.New("gpool/postgres: rows already closed")

	// ErrInvalidConfig is returned by New when the configuration cannot be used,
	// and by the optional capabilities when a request is malformed.
	ErrInvalidConfig = errors.New("gpool/postgres: invalid config")

	// ErrEmptyBatch is returned by SendBatch when the batch has no statements.
	ErrEmptyBatch = errors.New("gpool/postgres: batch is empty")

	// ErrBatchClosed is returned when batch results are read after Close.
	ErrBatchClosed = errors.New("gpool/postgres: batch results already closed")
)
