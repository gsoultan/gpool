// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"strconv"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/vendors/postgres/wire"
)

// batch pipelines queries in at most two round trips: one to parse and
// describe the statements the cache does not know yet, if there are any, and
// one for every Bind and Execute behind a single Sync. The exchange is left
// open for the returned reader.
//
// A single Sync makes the batch one implicit transaction, as it was with pgx:
// a statement that fails makes the server skip the rest.
func (s *session) batch(ctx context.Context, queries []gpool.BatchQuery) (*batchReader, error) {
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	stmts, temporary, err := s.prepareBatch(queries)
	if err != nil {
		return nil, s.finish(err)
	}
	buf := s.conn.Buffer()
	for i, query := range queries {
		if buf, err = appendBind(buf, stmts[i], query.Arguments); err != nil {
			return nil, s.finish(s.discard(temporary, err))
		}
		buf = wire.AppendExecute(buf, "", 0)
	}
	for _, name := range temporary {
		buf = wire.AppendClose(buf, 'S', name)
	}
	if err := s.conn.Send(wire.AppendSync(buf)); err != nil {
		return nil, s.finish(err)
	}
	return &batchReader{session: s, stmts: stmts}, nil
}

// prepareBatch finds or prepares a statement for every query. Without a cache
// the statements are given names of their own for the length of the batch,
// since one unnamed statement cannot serve several queries at once, and are
// returned for closing.
func (s *session) prepareBatch(queries []gpool.BatchQuery) ([]*statement, []string, error) {
	stmts := make([]*statement, len(queries))
	var missing []*statement
	bySQL := map[string]*statement{}
	for i, query := range queries {
		if s.cache != nil {
			stmts[i] = s.cache.get(query.SQL)
		}
		if stmts[i] == nil {
			stmts[i] = bySQL[query.SQL]
		}
		if stmts[i] == nil {
			s.named++
			stmt := &statement{name: "gpool_" + strconv.FormatUint(s.named, 36), sql: query.SQL}
			bySQL[query.SQL] = stmt
			missing = append(missing, stmt)
			stmts[i] = stmt
		}
	}
	if len(missing) == 0 {
		return stmts, nil, nil
	}
	if err := s.describeAll(missing); err != nil {
		return nil, nil, err
	}

	var temporary []string
	for _, stmt := range missing {
		if s.cache == nil {
			temporary = append(temporary, stmt.name)
			continue
		}
		// An eviction is closed ahead of the next exchange's Parse, after this
		// batch, so a statement this batch is about to bind is never closed
		// under it.
		if evicted := s.cache.put(stmt); evicted != nil {
			s.closing = append(s.closing, evicted.name)
		}
	}
	return stmts, temporary, nil
}

// describeAll parses and describes statements behind one Sync, filling in what
// the server reports for each, in order.
func (s *session) describeAll(stmts []*statement) error {
	buf := s.closeEvicted(s.conn.Buffer())
	for _, stmt := range stmts {
		buf = wire.AppendParse(buf, stmt.name, stmt.sql, nil)
		buf = wire.AppendDescribe(buf, 'S', stmt.name)
	}
	if err := s.conn.Send(wire.AppendSync(buf)); err != nil {
		return err
	}

	var failure error
	next := 0
	var params []uint32
	for {
		kind, body, err := s.conn.Receive()
		if err != nil {
			return err
		}
		switch kind {
		case wire.MsgParameterDescription:
			params, err = wire.ParseParameterDescription(nil, body)
		case wire.MsgRowDescription, wire.MsgNoData:
			var fields []wire.Field
			if kind == wire.MsgRowDescription {
				fields, err = wire.ParseRowDescription(nil, body)
			}
			if next < len(stmts) {
				*stmts[next] = *newStatement(stmts[next].name, stmts[next].sql, params, fields)
				next++
			}
		case wire.MsgErrorResponse:
			if failure == nil {
				failure = wire.ParseError(body)
			}
		case wire.MsgReadyForQuery:
			return failure
		}
		if err != nil {
			return err
		}
	}
}

// discard closes temporary statements after a batch failed before it was sent,
// and returns the failure.
func (s *session) discard(temporary []string, failure error) error {
	if len(temporary) == 0 {
		return failure
	}
	buf := s.conn.Buffer()
	for _, name := range temporary {
		buf = wire.AppendClose(buf, 'S', name)
	}
	if err := s.conn.Send(wire.AppendSync(buf)); err != nil {
		return err
	}
	if _, err := s.drain(nil); err != nil {
		return err
	}
	return failure
}
