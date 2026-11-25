// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package benchmarks

import (
	"encoding/binary"
	"testing"

	"github.com/gsoultan/caduceus/replication"
	"github.com/jackc/pglogrepl"
)

// The same UPDATE decoded both ways, ending in the same two maps of strings,
// so the difference is the decoding alone.

func relationRecord(columns []string) []byte {
	out := binary.BigEndian.AppendUint32([]byte{'R'}, 7)
	out = append(out, "public\x00accounts\x00d"...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(columns)))
	for _, name := range columns {
		out = append(out, 0)
		out = append(append(out, name...), 0)
		out = binary.BigEndian.AppendUint32(out, 25)
		out = binary.BigEndian.AppendUint32(out, 0xFFFFFFFF)
	}
	return out
}

func BenchmarkDecodeUpdate(b *testing.B) {
	columns := []string{"id", "email", "updated_at", "deleted_at", "settings", "notes"}
	b.Run("caduceus", func(b *testing.B) {
		d := replication.NewDecoder()
		if _, _, err := d.Decode(relationRecord(columns)); err != nil {
			b.Fatal(err)
		}
		record := updateRecord()
		b.ReportAllocs()
		for b.Loop() {
			change, ok, err := d.Decode(record)
			if err != nil || !ok || change.After["email"] != "new@example.com" {
				b.Fatal(change, err)
			}
			_ = replication.LSN(1).String()
		}
	})
	b.Run("pglogrepl", benchmarkPglogreplDecodeUpdate)
}

// benchmarkPglogreplDecodeUpdate parses the message into pglogrepl's structs,
// then builds the two column maps from them.
func benchmarkPglogreplDecodeUpdate(b *testing.B) {
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

// columnMap builds a change's column map from pglogrepl's tuple.
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

// updateRecord is an UPDATE under REPLICA IDENTITY FULL, with a NULL and an
// unchanged TOASTed column.
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
