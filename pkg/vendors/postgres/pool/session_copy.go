// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/vendors/postgres/codec"
	"github.com/gsoultan/gpool/pkg/vendors/postgres/wire"
)

// copyChunk is how much row data is gathered before it goes out as CopyData.
const copyChunk = 64 << 10

// errCopyNotStarted is a server that answered COPY with something other than
// CopyInResponse.
var errCopyNotStarted = errors.New("gpool/postgres: server did not start the COPY")

// copyFrom streams rows into a table in COPY's text format.
//
// The column types are learned and the COPY started in the same round trip: a
// Parse and Describe of a SELECT over the columns, then the COPY behind it. The
// rows are then encoded for those types as they are pulled from the source.
func (s *session) copyFrom(ctx context.Context, request gpool.CopyRequest) (int64, error) {
	if err := s.begin(ctx); err != nil {
		return 0, err
	}
	copied, err := s.copyRows(request)
	return copied, s.finish(err)
}

func (s *session) copyRows(request gpool.CopyRequest) (int64, error) {
	table := quoteQualifiedName(request.Table)
	columns := make([]string, len(request.Columns))
	for i, column := range request.Columns {
		columns[i] = quoteIdentifier(column)
	}
	list := strings.Join(columns, ", ")

	buf := s.closeEvicted(s.conn.Buffer())
	buf = wire.AppendParse(buf, "", "SELECT "+list+" FROM "+table, nil)
	buf = wire.AppendDescribe(buf, 'S', "")
	buf = wire.AppendSync(buf)
	buf = wire.AppendQuery(buf, "COPY "+table+" ("+list+") FROM STDIN")
	if err := s.conn.Send(buf); err != nil {
		return 0, err
	}

	// A failed describe is reported by the COPY as well, which still has to be
	// read to its ReadyForQuery, so the COPY's error is the one returned.
	described, describeErr := s.readDescription("", "")
	if err := s.awaitCopyIn(); err != nil {
		return 0, err
	}
	if describeErr != nil {
		return 0, s.abandonCopy(describeErr)
	}

	oids := make([]uint32, len(described.fields))
	for i, field := range described.fields {
		oids[i] = field.DataTypeOID
	}
	if err := s.sendRows(request.Rows, oids); err != nil {
		return 0, s.abandonCopy(err)
	}
	if err := s.conn.Send(wire.AppendCopyDone(s.conn.Buffer())); err != nil {
		return 0, err
	}
	tag, err := s.drain(nil)
	if err != nil {
		return 0, err
	}
	return copyCount(tag), nil
}

// awaitCopyIn reads up to the server's CopyInResponse. A server error before it
// is read through to ReadyForQuery and returned.
func (s *session) awaitCopyIn() error {
	for {
		kind, body, err := s.conn.Receive()
		if err != nil {
			return err
		}
		switch kind {
		case wire.MsgCopyInResponse:
			return nil
		case wire.MsgErrorResponse:
			failure := wire.ParseError(body)
			if _, err := s.drain(nil); err != nil {
				return err
			}
			return failure
		case wire.MsgReadyForQuery:
			return errCopyNotStarted
		}
	}
}

// abandonCopy ends a started COPY with CopyFail, reads the server's reply
// through to ReadyForQuery, and returns why the copy was abandoned.
func (s *session) abandonCopy(reason error) error {
	if err := s.conn.Send(wire.AppendCopyFail(s.conn.Buffer(), "gpool: copy abandoned")); err != nil {
		return err
	}
	if _, err := s.drain(nil); err != nil && !isServerError(err) {
		return err
	}
	return reason
}

// sendRows encodes every row from the source and sends them in chunks.
func (s *session) sendRows(rows gpool.CopyRows, oids []uint32) error {
	buf, mark := wire.StartMessage(s.conn.Buffer(), 'd')
	var scratch []byte
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		if len(values) != len(oids) {
			return &ArgumentCountError{Want: len(oids), Got: len(values)}
		}
		if buf, scratch, err = appendCopyLine(buf, scratch, oids, values); err != nil {
			return err
		}
		if len(buf)-mark < copyChunk {
			continue
		}
		if err := s.conn.Send(wire.FinishMessage(buf, mark)); err != nil {
			return err
		}
		buf, mark = wire.StartMessage(s.conn.Buffer(), 'd')
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(buf)-mark == 4 {
		return nil
	}
	return s.conn.Send(wire.FinishMessage(buf, mark))
}

// appendCopyLine appends one row of COPY text format: tab-separated values,
// \N for NULL, with backslash, tab, newline and carriage return escaped.
func appendCopyLine(buf, scratch []byte, oids []uint32, values []any) ([]byte, []byte, error) {
	for i, value := range values {
		if i > 0 {
			buf = append(buf, '\t')
		}
		text, null, err := codec.EncodeText(scratch[:0], oids[i], value)
		if err != nil {
			return buf, scratch, &ArgumentError{Index: i, Err: err}
		}
		scratch = text
		if null {
			buf = append(buf, `\N`...)
			continue
		}
		buf = appendCopyEscaped(buf, text)
	}
	return append(buf, '\n'), scratch, nil
}

func appendCopyEscaped(buf, text []byte) []byte {
	for _, b := range text {
		switch b {
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\t':
			buf = append(buf, '\\', 't')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		default:
			buf = append(buf, b)
		}
	}
	return buf
}

// copyCount reads the row count from a "COPY n" tag.
func copyCount(tag string) int64 {
	n, _ := strconv.ParseInt(strings.TrimPrefix(tag, "COPY "), 10, 64)
	return n
}

func isServerError(err error) bool {
	var failure *wire.Error
	return errors.As(err, &failure)
}

// quoteQualifiedName quotes each part of a possibly schema-qualified name.
func quoteQualifiedName(parts []string) string {
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = quoteIdentifier(part)
	}
	return strings.Join(quoted, ".")
}
