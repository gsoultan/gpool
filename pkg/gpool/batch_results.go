// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// BatchResults reads the replies to a batch, in the order the statements were queued.
//
// Call one of Exec, Query, or QueryRow per queued statement, matching what that
// statement returns, then Close. Close reports the first error the batch hit and
// must be called even if you stop reading early — the remaining replies are still
// on the wire, and leaving them there desynchronises the connection.
type BatchResults interface {
	// Exec reads the next reply as a statement returning no rows.
	Exec() (Result, error)
	// Query reads the next reply as a row set. Close it before reading the reply
	// after it.
	Query() (Rows, error)
	// QueryRow reads the next reply as a single row.
	QueryRow() Row
	// Close drains any unread replies and returns the batch's first error.
	Close() error
}
