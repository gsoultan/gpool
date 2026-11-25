// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"encoding/binary"
	"testing"
)

// benchUpdate is an UPDATE under REPLICA IDENTITY FULL on a six-column table:
// both images, a NULL, and an unchanged TOASTed value. The same bytes are
// decoded with pglogrepl in benchmarks/cdc_decode_test.go, which is the
// comparison this benchmark exists for.
func benchUpdate() []byte {
	values := func(email string) []byte {
		return tuple(textColumn("1042"), textColumn(email), textColumn("2026-08-08 12:00:00+00"),
			nullColumn, textColumn("{\"plan\":\"pro\",\"seats\":12}"), unchangedColumn)
	}
	out := binary.BigEndian.AppendUint32([]byte{msgUpdate}, 7)
	out = append(out, tupleOld)
	out = append(out, values("old@example.com")...)
	out = append(out, tupleNew)
	return append(out, values("new@example.com")...)
}

func BenchmarkDecodeUpdate(b *testing.B) {
	_, rel, err := parseRelation(relationMessage(7, "public", "accounts",
		"id", "email", "updated_at", "deleted_at", "settings", "notes")[1:])
	if err != nil {
		b.Fatal(err)
	}
	relations := map[uint32]*relation{7: rel}
	record := benchUpdate()
	var current transaction

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := decodeRecord(record, relations, &current, 1); err != nil {
			b.Fatal(err)
		}
	}
}
