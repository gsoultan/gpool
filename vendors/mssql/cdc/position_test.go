// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"errors"
	"testing"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// A position is the only durable record a SQL Server consumer has of its
// progress, so the round trip has to be exact.
func TestPositionRoundTrips(t *testing.T) {
	tests := []struct {
		name string
		lsn  []byte
		text cdc.Position
	}{
		{"zero", make([]byte, lsnLen), "0x00000000000000000000"},
		{"observed", []byte{0, 0, 0, 0x2B, 0, 0, 5, 0x82, 0, 0x1C}, "0x0000002B00000582001C"},
		{"maximum", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, "0xFFFFFFFFFFFFFFFFFFFF"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := position(test.lsn); got != test.text {
				t.Errorf("position() = %q, want %q", got, test.text)
			}

			back, err := parsePosition(test.text)
			if err != nil {
				t.Fatalf("parsePosition(%q) = %v", test.text, err)
			}
			if string(back) != string(test.lsn) {
				t.Errorf("round trip = %x, want %x", back, test.lsn)
			}
		})
	}
}

// A position from another vendor must be refused rather than coerced. The
// capture functions take a starting LSN and return everything after it, so a
// misread value does not fail — it silently starts somewhere else.
func TestParsePositionRejectsAForeignPosition(t *testing.T) {
	tests := []struct {
		name     string
		position cdc.Position
	}{
		{"PostgreSQL LSN", "0/1A2B3C4D"},
		{"MySQL GTID", "gtid:3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"},
		{"MySQL binlog offset", "file:mysql-bin.000042:1234"},
		{"empty", ""},
		{"no prefix", "0000002B00000582001C"},
		{"not hexadecimal", "0xZZZZZZZZZZZZZZZZZZZZ"},
		{"too short", "0x0000002B"},
		{"too long", "0x0000002B00000582001C00"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parsePosition(test.position); !errors.Is(err, ErrInvalidPosition) {
				t.Errorf("parsePosition(%q) error = %v, want ErrInvalidPosition", test.position, err)
			}
		})
	}
}

// LSNs are fixed-width big-endian, so ordering them is comparing their bytes —
// and the ordering is what merges several capture instances into commit order.
func TestCompareLSN(t *testing.T) {
	low := []byte{0, 0, 0, 0x2B, 0, 0, 5, 0x82, 0, 0x1B}
	high := []byte{0, 0, 0, 0x2B, 0, 0, 5, 0x82, 0, 0x1C}

	if compareLSN(low, high) >= 0 {
		t.Error("compareLSN did not order a lower LSN first")
	}
	if compareLSN(high, low) <= 0 {
		t.Error("compareLSN did not order a higher LSN last")
	}
	if compareLSN(low, low) != 0 {
		t.Error("compareLSN did not report equality")
	}
}

// An update arrives as two rows under "all update old", and they are one change.
func TestPairJoinsUpdateImages(t *testing.T) {
	changes := []change{
		{operation: opInsert, schema: "dbo", table: "t", startLSN: make([]byte, lsnLen), values: map[string]any{"v": "new"}},
		{operation: opUpdateOld, schema: "dbo", table: "t", startLSN: make([]byte, lsnLen), values: map[string]any{"v": "before"}},
		{operation: opUpdateNew, schema: "dbo", table: "t", startLSN: make([]byte, lsnLen), values: map[string]any{"v": "after"}},
		{operation: opDelete, schema: "dbo", table: "t", startLSN: make([]byte, lsnLen), values: map[string]any{"v": "gone"}},
	}

	events := pair(changes)
	if len(events) != 3 {
		t.Fatalf("got %d events from 4 rows, want 3", len(events))
	}

	if events[0].Op != cdc.OpInsert || events[0].After["v"] != "new" || events[0].Before != nil {
		t.Errorf("insert = %+v", events[0])
	}
	if events[1].Op != cdc.OpUpdate || events[1].Before["v"] != "before" || events[1].After["v"] != "after" {
		t.Errorf("update = %+v", events[1])
	}
	if events[2].Op != cdc.OpDelete || events[2].Before["v"] != "gone" || events[2].After != nil {
		t.Errorf("delete = %+v", events[2])
	}
}

// A before image with no after image is not an update anyone can apply, and a
// poll window can split the pair. Reporting half of one would write the old
// value as though it were the new one.
func TestPairDropsAnUnmatchedBeforeImage(t *testing.T) {
	events := pair([]change{
		{operation: opUpdateOld, schema: "dbo", table: "t", startLSN: make([]byte, lsnLen), values: map[string]any{"v": "before"}},
	})
	if len(events) != 0 {
		t.Fatalf("got %d events from a lone before image, want 0: %+v", len(events), events)
	}
}

// Each capture instance is queried separately, so the batch arrives as several
// ordered runs. A consumer replaying downstream needs the one order the server
// committed them in.
func TestOrderChangesMergesInstances(t *testing.T) {
	lsn := func(n byte) []byte {
		out := make([]byte, lsnLen)
		out[lsnLen-1] = n
		return out
	}
	changes := []change{
		{table: "a", startLSN: lsn(1), seqval: lsn(1)},
		{table: "a", startLSN: lsn(5), seqval: lsn(1)},
		{table: "b", startLSN: lsn(2), seqval: lsn(1)},
		{table: "b", startLSN: lsn(3), seqval: lsn(1)},
	}

	orderChanges(changes)

	want := []byte{1, 2, 3, 5}
	for i, change := range changes {
		if change.startLSN[lsnLen-1] != want[i] {
			t.Fatalf("position %d has LSN ending %d, want %d", i, change.startLSN[lsnLen-1], want[i])
		}
	}
}

// The capture instance name is interpolated into a function name, where it can
// be neither bound nor quoted. It is read from the catalog rather than from a
// caller, but a table named to look like SQL must not become SQL.
func TestCaptureInstancePatternRejectsSQL(t *testing.T) {
	for _, name := range []string{
		"dbo_users; DROP TABLE users--",
		"dbo_users(1)",
		"dbo users",
		"",
		"dbo_users'",
	} {
		if captureInstancePattern.MatchString(name) {
			t.Errorf("captureInstancePattern accepted %q", name)
		}
	}
	if !captureInstancePattern.MatchString("dbo_users") {
		t.Error("captureInstancePattern rejected an ordinary instance name")
	}
}

func TestSplitQualified(t *testing.T) {
	tests := []struct {
		in     string
		schema string
		table  string
		fails  bool
	}{
		{in: "dbo.users", schema: "dbo", table: "users"},
		{in: "  sales.orders  ", schema: "sales", table: "orders"},
		{in: "users", schema: "dbo", table: "users"},
		{in: "dbo.users; DROP TABLE x", fails: true},
		{in: "dbo.", fails: true},
		{in: "", fails: true},
	}

	for _, test := range tests {
		t.Run(test.in, func(t *testing.T) {
			schema, table, err := splitQualified(test.in)
			if test.fails {
				if err == nil {
					t.Errorf("splitQualified(%q) succeeded, want an error", test.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitQualified(%q) = %v", test.in, err)
			}
			if schema != test.schema || table != test.table {
				t.Errorf("got %s.%s, want %s.%s", schema, table, test.schema, test.table)
			}
		})
	}
}
