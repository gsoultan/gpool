// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// position renders a WAL offset as a cdc.Position, in PostgreSQL's own "X/Y"
// notation — the same text psql, pg_stat_replication and pg_lsn all use, so a
// position recorded by a consumer can be compared against the server by hand.
//
// The numeric offset stays internal. Confirming a position to the server is
// arithmetic on a uint64 and happens on the reader's hot path; only the value
// handed to the consumer is text.
func position(lsn uint64) cdc.Position {
	return cdc.Position(formatLSN(lsn))
}

// formatLSN writes an LSN as two hexadecimal halves, upper case, unpadded, as
// the server does.
func formatLSN(lsn uint64) string {
	var buf [17]byte
	out := appendHex(buf[:0], lsn>>32)
	out = append(out, '/')
	return string(appendHex(out, lsn&0xFFFFFFFF))
}

func appendHex(dst []byte, v uint64) []byte {
	const digits = "0123456789ABCDEF"
	if v == 0 {
		return append(dst, '0')
	}
	var tmp [8]byte
	i := len(tmp)
	for ; v > 0; v >>= 4 {
		i--
		tmp[i] = digits[v&0xF]
	}
	return append(dst, tmp[i:]...)
}

// parsePosition reads a position this vendor produced back into a WAL offset.
//
// A position from another vendor is rejected rather than coerced. Resuming a
// replication stream from a misparsed offset would silently skip or replay an
// arbitrary span of WAL, which is worse than refusing to start.
func parsePosition(p cdc.Position) (uint64, error) {
	if p == cdc.NoPosition {
		return 0, nil
	}
	lsn, err := parseLSN(string(p))
	if err != nil {
		return 0, fmt.Errorf("%w: position %q is not a PostgreSQL LSN: %w", ErrInvalidConfig, p, err)
	}
	return lsn, nil
}

func parseLSN(s string) (uint64, error) {
	high, low, ok := strings.Cut(s, "/")
	if !ok {
		return 0, errLSNFormat
	}
	hi, err := strconv.ParseUint(high, 16, 32)
	if err != nil {
		return 0, err
	}
	lo, err := strconv.ParseUint(low, 16, 32)
	if err != nil {
		return 0, err
	}
	return hi<<32 | lo, nil
}
