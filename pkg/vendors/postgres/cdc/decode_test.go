// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/gsoultan/caduceus/replication"
	"github.com/gsoultan/nextar/pkg/gpool/cdc"
)

// The pgoutput decoding itself is caduceus's and is tested there. These tests
// cover what this vendor adds: turning a decoded change into a cdc.Event with
// the right positions, timestamp and operation.

var postgresEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func relationMessage(id uint32, namespace, name string, columns ...string) []byte {
	out := binary.BigEndian.AppendUint32([]byte{'R'}, id)
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
	out := binary.BigEndian.AppendUint64([]byte{'B'}, final)
	out = binary.BigEndian.AppendUint64(out, uint64(at.Sub(postgresEpoch).Microseconds()))
	return binary.BigEndian.AppendUint32(out, 777)
}

// changeMessage is a row change whose new tuple is id = "1", email = NULL;
// a delete carries it as the old tuple instead.
func changeMessage(op byte, relation uint32) []byte {
	out := binary.BigEndian.AppendUint32([]byte{op}, relation)
	if op == 'D' {
		out = append(out, 'K')
	} else {
		out = append(out, 'N')
	}
	out = binary.BigEndian.AppendUint16(out, 2)
	out = binary.BigEndian.AppendUint32(append(out, 't'), 1)
	return append(out, '1', 'n')
}

// Every change carries two positions that must not be confused: its own
// record's end, which is what is confirmed to the server, and its
// transaction's commit LSN, which is the same for the whole transaction.
func TestDecodeRecordFollowsATransaction(t *testing.T) {
	t.Parallel()

	decoder := replication.NewDecoder()
	at := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	for _, record := range [][]byte{beginMessage(0x1_0000_00FF, at), relationMessage(7, "public", "users", "id", "email")} {
		if _, ok, err := decodeRecord(record, decoder, 0); ok || err != nil {
			t.Fatalf("decodeRecord(%q) = %v, %v; want no event", record[0], ok, err)
		}
	}

	pending, ok, err := decodeRecord(changeMessage('I', 7), decoder, 0x1_0000_0100)
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

func TestDecodeRecordMapsEveryOperation(t *testing.T) {
	t.Parallel()

	decoder := replication.NewDecoder()
	_, _, _ = decodeRecord(relationMessage(7, "public", "users", "id", "email"), decoder, 0)
	for op, want := range map[byte]cdc.Op{'I': cdc.OpInsert, 'U': cdc.OpUpdate, 'D': cdc.OpDelete} {
		pending, ok, err := decodeRecord(changeMessage(op, 7), decoder, 1)
		if err != nil || !ok || pending.event.Op != want {
			t.Errorf("%c: event = %v, %v, %v; want %v", op, pending.event.Op, ok, err, want)
		}
	}
	if _, ok, err := decodeRecord(changeMessage('I', 9), decoder, 1); ok || err != nil {
		t.Errorf("unknown relation: %v, %v; want skipped", ok, err)
	}
}

func TestOpString(t *testing.T) {
	t.Parallel()

	tests := map[cdc.Op]string{
		cdc.OpInsert: "INSERT",
		cdc.OpUpdate: "UPDATE",
		cdc.OpDelete: "DELETE",
		cdc.Op(99):   "UNKNOWN",
	}
	for op, want := range tests {
		if got := op.String(); got != want {
			t.Errorf("Op(%d).String() = %q, want %q", op, got, want)
		}
	}
}

// The stream is bytes from the network. No input may panic the reader
// goroutine, whose panic would take the consumer's whole process down.
func FuzzDecodeRecord(f *testing.F) {
	f.Add(beginMessage(1, time.Now()))
	f.Add(relationMessage(7, "public", "users", "id", "email"))
	f.Add(changeMessage('U', 7))
	f.Fuzz(func(t *testing.T, record []byte) {
		decoder := replication.NewDecoder()
		_, _, _ = decodeRecord(relationMessage(7, "public", "users", "id", "email"), decoder, 0)
		_, _, _ = decodeRecord(record, decoder, 1)
	})
}
