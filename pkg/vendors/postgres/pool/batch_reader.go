// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"errors"

	"github.com/gsoultan/gpool/pkg/vendors/postgres/wire"
)

// errBatchExhausted is returned when more results are read than were queued.
var errBatchExhausted = errors.New("gpool/postgres: no more results in the batch")

// batchReader reads a pipelined batch's replies off the connection, one
// statement at a time, in the order they were queued.
type batchReader struct {
	session *session
	stmts   []*statement
	next    int
	current *cursor
	failure error
	settled bool
}

// statement returns a cursor over the next statement's result, closing the
// previous one first so the connection is positioned at its start.
func (b *batchReader) statement() (*cursor, error) {
	b.finishCurrent()
	if b.settled {
		if b.failure != nil {
			return nil, b.failure
		}
		return nil, errBatchExhausted
	}
	if b.next >= len(b.stmts) {
		return nil, errBatchExhausted
	}
	b.current = newCursor(b.session, b.stmts[b.next], true)
	b.next++
	return b.current, nil
}

// finishCurrent reads what is left of the current statement.
func (b *batchReader) finishCurrent() {
	if b.current == nil {
		return
	}
	b.current.Close()
	b.note(b.current)
	b.current = nil
}

func (b *batchReader) note(c *cursor) {
	if c.err != nil && b.failure == nil {
		b.failure = c.err
	}
	if c.settled {
		b.settled = true
	}
}

// close reads every remaining reply up to the batch's ReadyForQuery and
// returns the batch's first error.
func (b *batchReader) close() error {
	b.finishCurrent()
	if b.settled {
		return b.failure
	}
	for {
		kind, body, err := b.session.conn.Receive()
		if err != nil {
			b.settled = true
			return b.remember(b.session.finish(err))
		}
		switch kind {
		case wire.MsgErrorResponse:
			b.remember(wire.ParseError(body))
		case wire.MsgReadyForQuery:
			b.settled = true
			return b.remember(b.session.finish(nil))
		}
	}
}

func (b *batchReader) remember(err error) error {
	if err != nil && b.failure == nil {
		b.failure = err
	}
	return b.failure
}
