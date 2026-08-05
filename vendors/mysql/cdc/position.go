// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// MySQL names a place in the binary log two different ways, and a position has
// to say which it is.
//
// A GTID set and a file-and-offset pair are not interchangeable — the first
// survives a failover to a replica, the second does not — and they are not
// reliably distinguishable by shape: "mysql-bin.000042:1234" and a GTID set both
// contain a colon. Tagging removes the guess.
const (
	gtidScheme = "gtid:"
	fileScheme = "file:"
)

// position is a place in the binary log, in one of MySQL's two notations.
type position struct {
	gtid mysql.GTIDSet
	file mysql.Position
}

// isGTID reports which notation this position uses.
func (p position) isGTID() bool {
	return p.gtid != nil
}

// encode renders a position for a consumer to store.
func (p position) encode() cdc.Position {
	if p.isGTID() {
		return cdc.Position(gtidScheme + p.gtid.String())
	}
	return cdc.Position(fmt.Sprintf("%s%s:%d", fileScheme, p.file.Name, p.file.Pos))
}

// parsePosition reads a position this vendor produced.
//
// A position from another vendor, or in the wrong flavour's GTID syntax, is
// refused rather than coerced. Starting a binlog dump from a misread offset
// yields a stream that silently begins somewhere else entirely.
func parsePosition(p cdc.Position, flavor string) (position, error) {
	text := string(p)

	if set, ok := strings.CutPrefix(text, gtidScheme); ok {
		gtid, err := mysql.ParseGTIDSet(flavor, set)
		if err != nil {
			return position{}, fmt.Errorf("%w: %q is not a %s GTID set: %w", ErrInvalidPosition, set, flavor, err)
		}
		return position{gtid: gtid}, nil
	}

	if rest, ok := strings.CutPrefix(text, fileScheme); ok {
		name, offset, found := strings.Cut(rest, ":")
		if !found || name == "" {
			return position{}, fmt.Errorf("%w: %q is not a binlog file and offset", ErrInvalidPosition, rest)
		}
		parsed, err := strconv.ParseUint(offset, 10, 32)
		if err != nil {
			return position{}, fmt.Errorf("%w: offset %q in %q is not a number", ErrInvalidPosition, offset, rest)
		}
		return position{file: mysql.Position{Name: name, Pos: uint32(parsed)}}, nil
	}

	return position{}, fmt.Errorf("%w: %q has no %q or %q prefix, so it did not come from this vendor",
		ErrInvalidPosition, text, gtidScheme, fileScheme)
}
