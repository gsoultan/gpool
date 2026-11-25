// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"bytes"
	"encoding/binary"
)

func be16(b []byte) uint16 { return binary.BigEndian.Uint16(b) }
func be32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }

// cutString splits a NUL-terminated string off the front of b.
func cutString(b []byte) (string, []byte, bool) {
	end := bytes.IndexByte(b, 0)
	if end < 0 {
		return "", nil, false
	}
	return string(b[:end]), b[end+1:], true
}
