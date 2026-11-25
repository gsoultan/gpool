// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"errors"
	"strconv"

	"github.com/gsoultan/gpool/pkg/vendors/postgres/codec"
	"github.com/gsoultan/gpool/pkg/vendors/postgres/wire"
)

// session runs queries on one connection over the extended protocol.
//
// Every exchange — a query and its result, a batch, a copy — starts with begin
// and ends with finish, and at most one is open at a time. One left open is
// what makes a connection busy, and a busy connection is never pooled.
type session struct {
	conn *wire.Conn

	// cache is nil when statements are not to be kept on the server, in which
	// case every query is parsed into the unnamed statement first.
	cache *statementCache
	// closing are evicted statements still to be closed on the server. They are
	// sent ahead of the next Parse, so eviction costs no round trip of its own.
	closing []string
	named   uint64

	open  bool
	guard wire.Guard

	// lastTag is the previous command tag, reused when the next one has the
	// same text, as it does for every run of the same statement.
	lastTag string
}

func newSession(conn *wire.Conn, statements int) *session {
	s := &session{conn: conn}
	if statements > 0 {
		s.cache = newStatementCache(statements)
	}
	return s
}

// begin opens an exchange guarded by ctx.
func (s *session) begin(ctx context.Context) error {
	if s.open {
		return ErrConnBusy
	}
	if s.conn.IsClosed() {
		return wire.ErrClosed
	}
	s.guard = s.conn.Watch(ctx)
	s.open = true
	return nil
}

// finish closes the exchange, given how it went, and returns how it went once
// the context has had its say.
func (s *session) finish(err error) error {
	if !s.open {
		return err
	}
	s.open = false
	return s.guard.Done(err)
}

// busy reports whether an exchange is open: a result not read to the end.
func (s *session) busy() bool { return s.open }

// exec runs a statement and discards its rows. Without arguments it goes over
// the simple protocol, which is one round trip and allows several statements
// separated by semicolons, as a migration script has.
func (s *session) exec(ctx context.Context, sql string, args []any) (string, error) {
	if err := s.begin(ctx); err != nil {
		return "", err
	}
	if len(args) == 0 {
		tag, err := s.simple(sql)
		return tag, s.finish(err)
	}
	stmt, err := s.prepare(sql)
	if err == nil {
		err = s.bind(stmt, args)
	}
	if err != nil {
		return "", s.finish(err)
	}
	tag, err := s.drain(stmt)
	return tag, s.finish(err)
}

// query runs a statement and returns its rows. The exchange stays open until
// the cursor has been read to the end or closed.
func (s *session) query(ctx context.Context, sql string, args []any) (*cursor, error) {
	c := new(cursor)
	if err := s.queryInto(ctx, sql, args, c); err != nil {
		return nil, err
	}
	return c, nil
}

// queryInto is query with the cursor supplied by the caller, so one that
// embeds it saves an allocation.
func (s *session) queryInto(ctx context.Context, sql string, args []any, c *cursor) error {
	if err := s.begin(ctx); err != nil {
		return err
	}
	stmt, err := s.prepare(sql)
	if err == nil {
		err = s.bind(stmt, args)
	}
	if err != nil {
		return s.finish(err)
	}
	c.init(s, stmt, false)
	return nil
}

// simple sends sql over the simple protocol and reads to ReadyForQuery,
// returning the last command tag.
func (s *session) simple(sql string) (string, error) {
	if err := s.conn.Send(wire.AppendQuery(s.conn.Buffer(), sql)); err != nil {
		return "", err
	}
	var tag string
	var failure error
	for {
		kind, body, err := s.conn.Receive()
		if err != nil {
			return "", err
		}
		switch kind {
		case wire.MsgCommandComplete:
			tag = s.tag(body)
		case wire.MsgErrorResponse:
			if failure == nil {
				failure = wire.ParseError(body)
			}
		case wire.MsgCopyInResponse:
			if err := s.conn.Send(wire.AppendCopyFail(s.conn.Buffer(), "COPY FROM STDIN needs CopyFrom")); err != nil {
				return "", err
			}
		case wire.MsgCopyOutResponse:
			failure = errors.Join(failure, errCopyOut)
		case wire.MsgReadyForQuery:
			return tag, failure
		}
	}
}

var errCopyOut = errors.New("gpool/postgres: COPY TO STDOUT is not supported through Exec")

// prepare returns the statement for sql, parsing it on the server and asking
// what it takes and returns unless the cache already knows. It is one round
// trip on a miss and none on a hit.
func (s *session) prepare(sql string) (*statement, error) {
	if s.cache != nil {
		if stmt := s.cache.get(sql); stmt != nil {
			return stmt, nil
		}
	}
	name := ""
	if s.cache != nil {
		s.named++
		name = "gpool_" + strconv.FormatUint(s.named, 36)
	}
	buf := s.closeEvicted(s.conn.Buffer())
	buf = wire.AppendParse(buf, name, sql, nil)
	buf = wire.AppendDescribe(buf, 'S', name)
	buf = wire.AppendSync(buf)
	if err := s.conn.Send(buf); err != nil {
		return nil, err
	}
	stmt, err := s.readDescription(name, sql)
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		if evicted := s.cache.put(stmt); evicted != nil {
			s.closing = append(s.closing, evicted.name)
		}
	}
	return stmt, nil
}

// closeEvicted appends a Close for every evicted statement.
func (s *session) closeEvicted(buf []byte) []byte {
	for _, name := range s.closing {
		buf = wire.AppendClose(buf, 'S', name)
	}
	s.closing = s.closing[:0]
	return buf
}

// readDescription reads the replies to Parse and Describe up to ReadyForQuery.
func (s *session) readDescription(name, sql string) (*statement, error) {
	var failure error
	var params []uint32
	var fields []wire.Field
	for {
		kind, body, err := s.conn.Receive()
		if err != nil {
			return nil, err
		}
		switch kind {
		case wire.MsgParameterDescription:
			params, err = wire.ParseParameterDescription(nil, body)
		case wire.MsgRowDescription:
			fields, err = wire.ParseRowDescription(nil, body)
		case wire.MsgErrorResponse:
			if failure == nil {
				failure = wire.ParseError(body)
			}
		case wire.MsgReadyForQuery:
			if failure != nil {
				return nil, failure
			}
			return newStatement(name, sql, params, fields), nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// bind sends Bind, Execute and Sync for a prepared statement.
func (s *session) bind(stmt *statement, args []any) error {
	buf, err := appendBind(s.conn.Buffer(), stmt, args)
	if err != nil {
		return err
	}
	buf = wire.AppendExecute(buf, "", 0)
	return s.conn.Send(wire.AppendSync(buf))
}

// appendBind encodes a Bind of the unnamed portal to stmt. Each argument is
// encoded for the type the server resolved for its parameter, in binary where
// the codec can, and the format codes are written once the encoding has chosen
// them.
func appendBind(buf []byte, stmt *statement, args []any) ([]byte, error) {
	if len(args) != len(stmt.params) {
		return buf, &ArgumentCountError{Want: len(stmt.params), Got: len(args)}
	}
	buf, mark := wire.StartMessage(buf, 'B')
	buf = wire.AppendString(wire.AppendString(buf, ""), stmt.name)
	buf = wire.AppendInt16(buf, int16(len(args)))
	formats := len(buf)
	buf = append(buf, make([]byte, 2*len(args))...)
	buf = wire.AppendInt16(buf, int16(len(args)))
	for i, arg := range args {
		length := len(buf)
		buf = append(buf, 0, 0, 0, 0)
		encoded, format, null, err := codec.Encode(buf, stmt.params[i], arg)
		if err != nil {
			return buf, &ArgumentError{Index: i, Err: err}
		}
		buf = encoded
		buf[formats+2*i], buf[formats+2*i+1] = 0, byte(format)
		size := uint32(len(buf) - length - 4)
		if null {
			size = 0xFFFFFFFF
		}
		buf[length], buf[length+1], buf[length+2], buf[length+3] = byte(size>>24), byte(size>>16), byte(size>>8), byte(size)
	}
	buf = wire.AppendInt16(buf, int16(len(stmt.formats)))
	for _, format := range stmt.formats {
		buf = wire.AppendInt16(buf, format)
	}
	return wire.FinishMessage(buf, mark), nil
}

// drain reads a statement's result to ReadyForQuery, discarding rows, and
// returns its command tag.
func (s *session) drain(stmt *statement) (string, error) {
	var tag string
	var failure error
	for {
		kind, body, err := s.conn.Receive()
		if err != nil {
			return "", err
		}
		switch kind {
		case wire.MsgCommandComplete:
			tag = s.tag(body)
		case wire.MsgErrorResponse:
			if failure == nil {
				failure = s.serverError(stmt, body)
			}
		case wire.MsgReadyForQuery:
			return tag, failure
		}
	}
}

// serverError parses an ErrorResponse and forgets a cached statement the error
// says is stale: SQLSTATE 0A000 is a plan whose result type changed under it,
// 26000 a statement the server no longer has. The next use prepares afresh.
func (s *session) serverError(stmt *statement, body []byte) error {
	failure := wire.ParseError(body)
	if stmt != nil && s.cache != nil && (failure.Code == "0A000" || failure.Code == "26000") {
		s.cache.remove(stmt)
		s.closing = append(s.closing, stmt.name)
	}
	return failure
}

// forgetStatements drops every cached statement, for after a reset that
// deallocated them on the server.
func (s *session) forgetStatements() {
	if s.cache != nil {
		s.cache.clear()
	}
	s.closing = s.closing[:0]
}

// tag reads a CommandComplete body, reusing the previous tag's string when the
// text is the same, which saves the allocation on every repeated statement.
func (s *session) tag(body []byte) string {
	if len(body) > 0 && body[len(body)-1] == 0 {
		body = body[:len(body)-1]
	}
	if string(body) != s.lastTag {
		s.lastTag = string(body)
	}
	return s.lastTag
}
