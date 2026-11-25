// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Package codec converts between Go values and PostgreSQL's wire formats: Go
// arguments into Bind parameters, and result columns into the variables a
// caller scans into.
//
// It decodes the binary format for the types that carry the most data — the
// integers, floats, booleans, bytea, uuid, the date and time types and arrays
// of all of them — and asks for text for everything else, where the text a
// server sends is already what a caller wants.
package codec

// Type OIDs from pg_type.
const (
	Bool        uint32 = 16
	Bytea       uint32 = 17
	Char        uint32 = 18
	Name        uint32 = 19
	Int8        uint32 = 20
	Int2        uint32 = 21
	Int4        uint32 = 23
	Text        uint32 = 25
	OID         uint32 = 26
	JSON        uint32 = 114
	Float4      uint32 = 700
	Float8      uint32 = 701
	Unknown     uint32 = 705
	Bpchar      uint32 = 1042
	Varchar     uint32 = 1043
	Date        uint32 = 1082
	Time        uint32 = 1083
	Timestamp   uint32 = 1114
	Timestamptz uint32 = 1184
	Interval    uint32 = 1186
	Numeric     uint32 = 1700
	UUID        uint32 = 2950
	JSONB       uint32 = 3802
)

// arrayElements maps an array type to its element type, for the arrays decoded
// and encoded in binary.
var arrayElements = map[uint32]uint32{
	1000: Bool, 1001: Bytea, 1005: Int2, 1007: Int4, 1016: Int8, 1028: OID,
	1021: Float4, 1022: Float8, 1182: Date, 1183: Time, 1115: Timestamp,
	1185: Timestamptz, 1187: Interval, 2951: UUID,
	1009: Text, 1015: Varchar, 1014: Bpchar, 1003: Name, 199: JSON, 3807: JSONB,
}

// ArrayElement returns the element type of an array type the codec handles.
func ArrayElement(oid uint32) (uint32, bool) {
	element, ok := arrayElements[oid]
	return element, ok
}

// binaryScalars are decoded from binary. Text types are the same bytes in
// either format, so they are asked for in text, which needs no translation.
var binaryScalars = map[uint32]bool{
	Bool: true, Bytea: true, Int2: true, Int4: true, Int8: true, OID: true,
	Float4: true, Float8: true, Date: true, Time: true, Timestamp: true,
	Timestamptz: true, Interval: true, UUID: true,
}

// ResultFormat returns the format to ask the server for a column of type oid.
func ResultFormat(oid uint32) int16 {
	if binaryScalars[oid] {
		return 1
	}
	if _, ok := arrayElements[oid]; ok {
		return 1
	}
	return 0
}

// textual reports whether a type's values are text in both formats.
func textual(oid uint32) bool {
	switch oid {
	case Text, Varchar, Bpchar, Name, Char, JSON, Unknown:
		return true
	}
	return false
}
