// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

func relationMessage(id uint32, namespace, name string, columns ...string) []byte {
	out := binary.BigEndian.AppendUint32([]byte{msgRelation}, id)
	out = append(append(out, namespace...), 0)
	out = append(append(out, name...), 0)
	out = append(out, 'd')
	out = binary.BigEndian.AppendUint16(out, uint16(len(columns)))
	for _, column := range columns {
		out = append(out, 0)
		out = append(append(out, column...), 0)
		out = binary.BigEndian.AppendUint32(out, 25)
		out = binary.BigEndian.AppendUint32(out, 0xFFFFFFFF)
	}
	return out
}

func beginMessage(final uint64, at time.Time) []byte {
	out := binary.BigEndian.AppendUint64([]byte{msgBegin}, final)
	out = binary.BigEndian.AppendUint64(out, uint64(at.Sub(postgresEpoch).Microseconds()))
	return binary.BigEndian.AppendUint32(out, 777)
}

// The decoder reads pgoutput's bytes itself now, so a whole transaction is run
// through it as the server sends one: Begin, Relation, then a row change that
// must be named by the Relation and stamped by the Begin.
func TestDecodeRecordFollowsATransaction(t *testing.T) {
	t.Parallel()

	relations := map[uint32]*relation{}
	var current transaction
	at := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)

	records := [][]byte{
		beginMessage(0x1_0000_00FF, at),
		relationMessage(7, "public", "users", "id", "email"),
	}
	for _, record := range records {
		if _, ok, err := decodeRecord(record, relations, &current, 0); ok || err != nil {
			t.Fatalf("decodeRecord(%q) = %v, %v; want no event", record[0], ok, err)
		}
	}

	insert := binary.BigEndian.AppendUint32([]byte{msgInsert}, 7)
	insert = append(insert, tupleNew)
	insert = append(insert, tuple(textColumn("1"), nullColumn)...)
	pending, ok, err := decodeRecord(insert, relations, &current, 0x1_0000_0100)
	if err != nil || !ok {
		t.Fatalf("decodeRecord(insert) = %v, %v", ok, err)
	}

	event := pending.event
	if event.Op != cdc.OpInsert || event.Schema != "public" || event.Table != "users" {
		t.Errorf("event = %v %s.%s", event.Op, event.Schema, event.Table)
	}
	if event.Transaction != "1/FF" || event.Position != "1/100" || pending.lsn != 0x1_0000_0100 {
		t.Errorf("positions = %s, %s, %d", event.Transaction, event.Position, pending.lsn)
	}
	if !event.Timestamp.Equal(at) {
		t.Errorf("Timestamp = %s, want %s", event.Timestamp, at)
	}
	if v, present := event.After["email"]; !present || v != nil || event.After["id"] != "1" {
		t.Errorf("After = %v", event.After)
	}
}

// A change for a relation the stream has not described is skipped, as it was
// before: there is nothing to name its columns with.
func TestDecodeRecordSkipsAnUnknownRelation(t *testing.T) {
	t.Parallel()

	insert := binary.BigEndian.AppendUint32([]byte{msgInsert}, 9)
	insert = append(insert, tupleNew)
	insert = append(insert, tuple(textColumn("1"))...)
	var current transaction
	if _, ok, err := decodeRecord(insert, map[uint32]*relation{}, &current, 1); ok || err != nil {
		t.Fatalf("decodeRecord() = %v, %v; want skipped", ok, err)
	}
}

// The stream is bytes from the network. No input may panic the reader
// goroutine, whose panic would take the consumer's whole process down.
func FuzzDecodeRecord(f *testing.F) {
	f.Add(beginMessage(1, time.Now()))
	f.Add(relationMessage(7, "public", "users", "id", "email"))
	insert := binary.BigEndian.AppendUint32([]byte{msgUpdate}, 7)
	insert = append(insert, tupleKey)
	insert = append(insert, tuple(textColumn("1"), unchangedColumn)...)
	f.Add(insert)

	f.Fuzz(func(t *testing.T, record []byte) {
		if len(record) == 0 {
			return
		}
		relations := map[uint32]*relation{7: testRelation()}
		var current transaction
		_, _, _ = decodeRecord(record, relations, &current, 1)
	})
}
