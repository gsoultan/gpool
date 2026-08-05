// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	"github.com/jackc/pglogrepl"
)

// position renders a WAL offset as a cdc.Position, in PostgreSQL's own "X/Y"
// notation — the same text psql, pg_stat_replication and pg_lsn all use, so a
// position recorded by a consumer can be compared against the server by hand.
//
// The numeric offset stays internal. Confirming a position to the server is
// arithmetic on a uint64 and happens on the reader's hot path; only the value
// handed to the consumer is text.
func position(lsn uint64) cdc.Position {
	return cdc.Position(pglogrepl.LSN(lsn).String())
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

	lsn, err := pglogrepl.ParseLSN(string(p))
	if err != nil {
		return 0, fmt.Errorf("%w: position %q is not a PostgreSQL LSN: %w", ErrInvalidConfig, p, err)
	}
	return uint64(lsn), nil
}
