// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"errors"
	"math"
	"testing"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// A position is the only thing a consumer persists in order to resume, so the
// round trip has to be exact. An offset that changed by one on the way through
// would silently replay or skip a WAL record.
func TestPositionRoundTrips(t *testing.T) {
	tests := []struct {
		name string
		lsn  uint64
		text cdc.Position
	}{
		{"zero", 0, "0/0"},
		{"low", 42, "0/2A"},
		{"segment boundary", 1 << 24, "0/1000000"},
		{"high half set", 1 << 32, "1/0"},
		{"maximum", math.MaxUint64, "FFFFFFFF/FFFFFFFF"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := position(test.lsn)
			if got != test.text {
				t.Errorf("position(%d) = %q, want %q", test.lsn, got, test.text)
			}

			back, err := parsePosition(got)
			if err != nil {
				t.Fatalf("parsePosition(%q) = %v", got, err)
			}
			if back != test.lsn {
				t.Errorf("round trip = %d, want %d", back, test.lsn)
			}
		})
	}
}

// NoPosition means "the slot decides", which is offset zero to START_REPLICATION.
func TestParsePositionTreatsNoPositionAsTheSlotsChoice(t *testing.T) {
	got, err := parsePosition(cdc.NoPosition)
	if err != nil {
		t.Fatalf("parsePosition(NoPosition) = %v", err)
	}
	if got != 0 {
		t.Errorf("parsePosition(NoPosition) = %d, want 0", got)
	}
}

// A position from another vendor must be refused rather than coerced. Starting
// replication from a misparsed offset skips or replays an arbitrary span of WAL,
// and does it silently.
func TestParsePositionRejectsAForeignPosition(t *testing.T) {
	tests := []struct {
		name     string
		position cdc.Position
	}{
		{"MySQL GTID set", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"},
		{"MySQL binlog file and offset", "mysql-bin.000042:1234"},
		{"plain number", "12345"},
		{"not hexadecimal", "0/ZZZZ"},
		{"missing separator", "1A2B3C4D"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parsePosition(test.position); !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("parsePosition(%q) error = %v, want ErrInvalidConfig", test.position, err)
			}
		})
	}
}
