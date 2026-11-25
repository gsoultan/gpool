// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"context"
)

// Result is a small, fully read result: what Exec and QueryText return. Values
// are copies, safe to keep; a NULL is a nil slice.
type Result struct {
	Fields []Field
	Rows   [][][]byte
	Tag    string
}

// Exec runs one or more statements over the simple protocol and returns the
// last statement's result. It is for commands — DDL, SET, the replication
// protocol's own commands — not for user data: nothing is bound, so sql must
// not contain a value from outside the program.
func (c *Conn) Exec(ctx context.Context, sql string) (*Result, error) {
	guard := c.Watch(ctx)
	result, err := c.exec(sql)
	return result, guard.Done(err)
}

func (c *Conn) exec(sql string) (*Result, error) {
	if err := c.Send(AppendQuery(c.Buffer(), sql)); err != nil {
		return nil, err
	}
	return c.collect()
}

// QueryText runs one statement over the extended protocol with its parameters
// bound as text, letting the server infer their types, and returns every row in
// text format. It is for catalog queries with small results.
func (c *Conn) QueryText(ctx context.Context, sql string, params ...string) (*Result, error) {
	guard := c.Watch(ctx)
	result, err := c.queryText(sql, params)
	return result, guard.Done(err)
}

func (c *Conn) queryText(sql string, params []string) (*Result, error) {
	buf := AppendParse(c.Buffer(), "", sql, nil)
	buf, mark := StartMessage(buf, 'B')
	buf = AppendString(AppendString(buf, ""), "")
	buf = AppendInt16(buf, 0)
	buf = AppendInt16(buf, int16(len(params)))
	for _, param := range params {
		buf = AppendInt32(buf, int32(len(param)))
		buf = append(buf, param...)
	}
	buf = AppendInt16(buf, 0)
	buf = FinishMessage(buf, mark)
	buf = AppendDescribe(buf, 'P', "")
	buf = AppendExecute(buf, "", 0)
	buf = AppendSync(buf)
	if err := c.Send(buf); err != nil {
		return nil, err
	}
	return c.collect()
}

// collect reads every message up to ReadyForQuery. A server error is returned
// once the connection is back at a boundary, so it stays usable.
func (c *Conn) collect() (*Result, error) {
	result := &Result{}
	var failure *Error
	for {
		kind, body, err := c.Receive()
		if err != nil {
			return nil, err
		}
		switch kind {
		case MsgRowDescription:
			result.Fields, err = ParseRowDescription(result.Fields[:0], body)
			result.Rows = result.Rows[:0]
		case MsgDataRow:
			err = result.addRow(body)
		case MsgCommandComplete:
			result.Tag, _, _ = cutString(body)
		case MsgErrorResponse:
			if failure == nil {
				failure = parseError(body)
			}
		case MsgCopyInResponse:
			// A COPY FROM STDIN sent through Exec has no data to send.
			err = c.Send(AppendCopyFail(c.Buffer(), "COPY FROM STDIN is not supported by Exec"))
		case MsgReadyForQuery:
			if failure != nil {
				return nil, failure
			}
			return result, nil
		}
		if err != nil {
			return nil, c.fail(err)
		}
	}
}

func (r *Result) addRow(body []byte) error {
	values, err := SplitDataRow(nil, body)
	if err != nil {
		return err
	}
	for i, value := range values {
		if value != nil {
			values[i] = append([]byte{}, value...)
		}
	}
	r.Rows = append(r.Rows, values)
	return nil
}
