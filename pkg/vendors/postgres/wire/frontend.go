// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import "encoding/binary"

// The Append functions encode one frontend message onto dst and return the
// extended slice. They never allocate beyond growing dst, so a caller that keeps
// its buffer between queries encodes every message without garbage.

// StartMessage begins a message of the given type and returns the offset its
// length is to be written at. FinishMessage writes it once the body is complete.
// Together they let a caller encode a body in place — a Bind's parameters
// straight from Go values — rather than building it and copying it.
func StartMessage(dst []byte, kind byte) ([]byte, int) {
	dst = append(dst, kind, 0, 0, 0, 0)
	return dst, len(dst) - 4
}

// FinishMessage writes the length of the message started at mark.
func FinishMessage(dst []byte, mark int) []byte {
	binary.BigEndian.PutUint32(dst[mark:], uint32(len(dst)-mark))
	return dst
}

// AppendString appends s with the NUL terminator the protocol uses.
func AppendString(dst []byte, s string) []byte {
	return append(append(dst, s...), 0)
}

// AppendInt16 appends a big-endian 16-bit integer.
func AppendInt16(dst []byte, n int16) []byte {
	return binary.BigEndian.AppendUint16(dst, uint16(n))
}

// AppendInt32 appends a big-endian 32-bit integer.
func AppendInt32(dst []byte, n int32) []byte {
	return binary.BigEndian.AppendUint32(dst, uint32(n))
}

// AppendQuery encodes a simple-protocol Query.
func AppendQuery(dst []byte, sql string) []byte {
	dst, mark := StartMessage(dst, 'Q')
	return FinishMessage(AppendString(dst, sql), mark)
}

// AppendParse encodes a Parse. A zero type lets the server infer the
// parameter's type from context.
func AppendParse(dst []byte, name, sql string, types []uint32) []byte {
	dst, mark := StartMessage(dst, 'P')
	dst = AppendString(AppendString(dst, name), sql)
	dst = AppendInt16(dst, int16(len(types)))
	for _, oid := range types {
		dst = binary.BigEndian.AppendUint32(dst, oid)
	}
	return FinishMessage(dst, mark)
}

// AppendDescribe encodes a Describe of a statement ('S') or a portal ('P').
func AppendDescribe(dst []byte, kind byte, name string) []byte {
	dst, mark := StartMessage(dst, 'D')
	dst = AppendString(append(dst, kind), name)
	return FinishMessage(dst, mark)
}

// AppendExecute encodes an Execute of a portal. Zero rows means no limit.
func AppendExecute(dst []byte, portal string, rows int32) []byte {
	dst, mark := StartMessage(dst, 'E')
	dst = AppendInt32(AppendString(dst, portal), rows)
	return FinishMessage(dst, mark)
}

// AppendClose encodes a Close of a statement ('S') or a portal ('P').
func AppendClose(dst []byte, kind byte, name string) []byte {
	dst, mark := StartMessage(dst, 'C')
	dst = AppendString(append(dst, kind), name)
	return FinishMessage(dst, mark)
}

// AppendSync encodes a Sync, the end of an extended-protocol unit.
func AppendSync(dst []byte) []byte {
	return append(dst, 'S', 0, 0, 0, 4)
}

// AppendFlush encodes a Flush, which asks the server to send what it has
// without ending the unit.
func AppendFlush(dst []byte) []byte {
	return append(dst, 'H', 0, 0, 0, 4)
}

// AppendTerminate encodes a Terminate.
func AppendTerminate(dst []byte) []byte {
	return append(dst, 'X', 0, 0, 0, 4)
}

// AppendCopyData encodes a CopyData carrying data.
func AppendCopyData(dst []byte, data []byte) []byte {
	dst, mark := StartMessage(dst, 'd')
	return FinishMessage(append(dst, data...), mark)
}

// AppendCopyDone encodes a CopyDone.
func AppendCopyDone(dst []byte) []byte {
	return append(dst, 'c', 0, 0, 0, 4)
}

// AppendCopyFail encodes a CopyFail, which aborts a COPY FROM STDIN.
func AppendCopyFail(dst []byte, reason string) []byte {
	dst, mark := StartMessage(dst, 'f')
	return FinishMessage(AppendString(dst, reason), mark)
}
