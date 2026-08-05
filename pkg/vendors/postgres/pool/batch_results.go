// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

// batchResults reads the replies to a pipelined batch.
type batchResults struct {
	results pgx.BatchResults
	closed  atomic.Bool
}

var _ gpool.BatchResults = (*batchResults)(nil)

// Exec reads the next reply as a statement returning no rows.
func (r *batchResults) Exec() (gpool.Result, error) {
	if r.closed.Load() {
		return nil, ErrBatchClosed
	}

	tag, err := r.results.Exec()
	if err != nil {
		return nil, err
	}
	return pgResult{tag: tag}, nil
}

// Query reads the next reply as a row set. The rows own nothing: the batch's
// connection belongs to whoever acquired it.
func (r *batchResults) Query() (gpool.Rows, error) {
	if r.closed.Load() {
		return nil, ErrBatchClosed
	}

	rows, err := r.results.Query()
	if err != nil {
		closeRows(rows)
		return nil, err
	}
	return newRows(rows, nil), nil
}

// QueryRow reads the next reply as a single row.
func (r *batchResults) QueryRow() gpool.Row {
	if r.closed.Load() {
		return errorRow{err: ErrBatchClosed}
	}

	rows, err := r.results.Query()
	if err != nil {
		closeRows(rows)
		return errorRow{err: err}
	}
	return newRow(rows, nil)
}

// Close drains any unread replies and returns the batch's first error. It is
// idempotent. Skipping it would leave replies on the wire and desynchronise the
// connection for whoever gets it next.
func (r *batchResults) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	return r.results.Close()
}

// failedBatchResults reports a failure that happened before anything was sent,
// so SendBatch can keep its single-return signature without handing back nil.
type failedBatchResults struct {
	err error
}

var _ gpool.BatchResults = failedBatchResults{}

func (r failedBatchResults) Exec() (gpool.Result, error) { return nil, r.err }
func (r failedBatchResults) Query() (gpool.Rows, error)  { return nil, r.err }
func (r failedBatchResults) QueryRow() gpool.Row         { return errorRow{err: r.err} }
func (r failedBatchResults) Close() error                { return r.err }
