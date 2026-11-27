// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"database/sql"
	"errors"

	"github.com/gsoultan/caduceus"
)

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

	// ErrNoRows is returned by Row.Scan when the query returned no rows. It is
	// sql.ErrNoRows itself, so code written against database/sql matches it.
	ErrNoRows = sql.ErrNoRows

	// ErrConnBusy is returned when a connection is used while a result it
	// returned is still open. Close the Rows first; the protocol answers one
	// request at a time.
	ErrConnBusy = caduceus.ErrConnBusy

	// ErrTxCommitRollback is returned by Commit when the transaction had already
	// failed, so the server rolled it back instead of committing.
	ErrTxCommitRollback = errors.New("gpool/postgres: commit unexpectedly resulted in rollback")

	// ErrTxNotBegun is returned by a transaction whose BEGIN the server
	// refused. BEGIN travels with the first statement, so that statement
	// reports the server's own error and does not run. From then until the
	// transaction is committed or rolled back, the Tx and the connection it is
	// on return this for everything, so that nothing runs outside the
	// transaction the caller believes is open. Begin sends a plain BEGIN,
	// which a server has little reason to refuse.
	ErrTxNotBegun = caduceus.ErrTxNotBegun

	// ErrInTransaction is returned by Begin on a connection already in a
	// transaction. PostgreSQL itself only warns and carries on in the one
	// already open, which would leave the caller's Rollback undoing work it
	// believes was committed earlier, so it is refused instead.
	ErrInTransaction = caduceus.ErrInTransaction

	// ErrResultChanged is returned for a column that cannot be read because
	// the statement's result changed type since it was described: a table
	// altered under a statement remembered on a connection that keeps nothing
	// on the server. The statement ran, and a write it made stands; the next
	// execution asks the server about it afresh.
	ErrResultChanged = caduceus.ErrResultChanged
)
