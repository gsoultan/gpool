// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"strconv"
)

// Error is an ErrorResponse from PostgreSQL.
//
// Classify it by Code, the SQLSTATE, never by Message: servers localise their
// messages, so text that matches in English does not match in German.
type Error struct {
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         int32
	InternalPosition int32
	InternalQuery    string
	Where            string
	SchemaName       string
	TableName        string
	ColumnName       string
	DataTypeName     string
	ConstraintName   string
	File             string
	Line             int32
	Routine          string
}

// Error renders the error the way psql does, with the SQLSTATE appended.
func (e *Error) Error() string {
	return e.Severity + ": " + e.Message + " (SQLSTATE " + e.Code + ")"
}

// SQLState returns the error's SQLSTATE code, the stable way to classify it.
func (e *Error) SQLState() string {
	return e.Code
}

// parseError reads the fields of an ErrorResponse or NoticeResponse body.
func parseError(body []byte) *Error {
	failure := &Error{}
	for len(body) > 1 {
		kind := body[0]
		value, rest, ok := cutString(body[1:])
		if !ok {
			break
		}
		failure.set(kind, value)
		body = rest
	}
	return failure
}

func (e *Error) set(kind byte, value string) {
	switch kind {
	case 'S':
		e.Severity = value
	case 'V':
		// The non-localised severity, present since 9.6, wins over the other.
		e.Severity = value
	case 'C':
		e.Code = value
	case 'M':
		e.Message = value
	case 'D':
		e.Detail = value
	case 'H':
		e.Hint = value
	case 'P':
		e.Position = atoi32(value)
	case 'p':
		e.InternalPosition = atoi32(value)
	case 'q':
		e.InternalQuery = value
	case 'W':
		e.Where = value
	case 's':
		e.SchemaName = value
	case 't':
		e.TableName = value
	case 'c':
		e.ColumnName = value
	case 'd':
		e.DataTypeName = value
	case 'n':
		e.ConstraintName = value
	case 'F':
		e.File = value
	case 'L':
		e.Line = atoi32(value)
	case 'R':
		e.Routine = value
	}
}

func atoi32(text string) int32 {
	n, _ := strconv.ParseInt(text, 10, 32)
	return int32(n)
}
