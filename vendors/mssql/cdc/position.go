// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// lsnLen is the width of a SQL Server log sequence number, which is binary(10).
const lsnLen = 10

// position renders an LSN the way SQL Server itself prints one, so a recorded
// position can be pasted into a query against sys.fn_cdc_map_lsn_to_time and
// compared by hand.
func position(lsn []byte) cdc.Position {
	return cdc.Position("0x" + strings.ToUpper(hex.EncodeToString(lsn)))
}

// parsePosition reads a position this vendor produced back into an LSN.
//
// A position from another vendor is refused rather than coerced. The capture
// functions take a starting LSN and return everything after it, so a misread
// value does not fail — it silently starts somewhere else.
func parsePosition(p cdc.Position) ([]byte, error) {
	text, ok := strings.CutPrefix(strings.ToLower(string(p)), "0x")
	if !ok {
		return nil, fmt.Errorf("%w: %q has no 0x prefix, so it did not come from this vendor", ErrInvalidPosition, p)
	}

	lsn, err := hex.DecodeString(text)
	if err != nil {
		return nil, fmt.Errorf("%w: %q is not hexadecimal: %w", ErrInvalidPosition, p, err)
	}
	if len(lsn) != lsnLen {
		return nil, fmt.Errorf("%w: %q decodes to %d bytes, want %d", ErrInvalidPosition, p, len(lsn), lsnLen)
	}
	return lsn, nil
}

// compareLSN orders two log sequence numbers. They are fixed-width big-endian
// byte strings, so ordering them is comparing the bytes.
func compareLSN(a, b []byte) int {
	return strings.Compare(string(a), string(b))
}
