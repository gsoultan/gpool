// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"github.com/gsoultan/gpool/pkg/vendors/postgres/codec"
	"github.com/gsoultan/gpool/pkg/vendors/postgres/wire"
)

// statement is a parsed statement and what the server said about it: the types
// of its parameters, which decide how each argument is encoded, and the columns
// of its result, which decide the format each is asked for in.
type statement struct {
	name   string
	sql    string
	params []uint32
	fields []wire.Field
	// formats is the result format code for each column, already in the form a
	// Bind wants it: empty when every column is text, one code when they all
	// agree, one per column otherwise.
	formats []int16

	// prev and next link the statement into its cache's recency list.
	prev, next *statement
}

// newStatement builds a statement from a ParameterDescription and an optional
// RowDescription. The fields are given the format they will arrive in.
func newStatement(name, sql string, params []uint32, fields []wire.Field) *statement {
	s := &statement{name: name, sql: sql, params: params, fields: fields}
	allText, allBinary := true, true
	for i := range s.fields {
		format := codec.ResultFormat(s.fields[i].DataTypeOID)
		s.fields[i].Format = format
		allText = allText && format == wire.TextFormat
		allBinary = allBinary && format == wire.BinaryFormat
	}
	switch {
	case allText:
	case allBinary:
		s.formats = []int16{wire.BinaryFormat}
	default:
		s.formats = make([]int16, len(s.fields))
		for i, field := range s.fields {
			s.formats[i] = field.Format
		}
	}
	return s
}
