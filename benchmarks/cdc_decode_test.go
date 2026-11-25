// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package benchmarks

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pglogrepl"
)

// BenchmarkPglogreplDecodeUpdate decodes the bytes BenchmarkDecodeUpdate in
// pkg/vendors/postgres/cdc decodes, the way gpool's CDC decoder did while it was
// built on pglogrepl: parse the message into pglogrepl's structs, then build the
// event's two column maps from them. Both sides end with the same maps of
// strings, so the difference is the decoding alone.
func BenchmarkPglogreplDecodeUpdate(b *testing.B) {
	columns := []string{"id", "email", "updated_at", "deleted_at", "settings", "notes"}
	relation := &pglogrepl.RelationMessage{RelationID: 7, Namespace: "public", RelationName: "accounts"}
	for _, name := range columns {
		relation.Columns = append(relation.Columns, &pglogrepl.RelationMessageColumn{Name: name})
	}
	record := updateRecord()

	b.ReportAllocs()
	for b.Loop() {
		message, err := pglogrepl.Parse(record)
		if err != nil {
			b.Fatal(err)
		}
		update := message.(*pglogrepl.UpdateMessage)
		_ = columnMap(relation, update.OldTuple)
		_ = columnMap(relation, update.NewTuple)
		_ = pglogrepl.LSN(1).String()
	}
}

// columnMap is the map-building half of the decoder gpool had on pglogrepl.
func columnMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	if tuple == nil {
		return nil
	}
	data := make(map[string]any, min(len(tuple.Columns), len(rel.Columns)))
	for i := range min(len(tuple.Columns), len(rel.Columns)) {
		col := tuple.Columns[i]
		switch col.DataType {
		case 'n':
			data[rel.Columns[i].Name] = nil
		case 't':
			data[rel.Columns[i].Name] = string(col.Data)
		}
	}
	return data
}

// updateRecord is the same UPDATE as benchUpdate in the cdc package.
func updateRecord() []byte {
	text := func(value string) []byte {
		out := binary.BigEndian.AppendUint32([]byte{'t'}, uint32(len(value)))
		return append(out, value...)
	}
	values := func(email string) []byte {
		out := binary.BigEndian.AppendUint16(nil, 6)
		for _, column := range [][]byte{text("1042"), text(email), text("2026-08-08 12:00:00+00"),
			{'n'}, text("{\"plan\":\"pro\",\"seats\":12}"), {'u'}} {
			out = append(out, column...)
		}
		return out
	}
	out := binary.BigEndian.AppendUint32([]byte{'U'}, 7)
	out = append(out, 'O')
	out = append(out, values("old@example.com")...)
	out = append(out, 'N')
	return append(out, values("new@example.com")...)
}
