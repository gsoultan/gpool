// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"time"

	"testing"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	"github.com/jackc/pglogrepl"
)

// committed is the transaction commit time pgoutput reports in its Begin
// message; every change decoded from that transaction carries it.
var committed = time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)

func testRelation() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id"},
			{Name: "email"},
			{Name: "bio"},
		},
	}
}

func tuple(columns ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{ColumnNum: uint16(len(columns)), Columns: columns}
}

func textColumn(value string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: tupleText, Data: []byte(value), Length: uint32(len(value))}
}

func TestDecodeTuple(t *testing.T) {
	t.Parallel()

	rel := testRelation()

	t.Run("text values are decoded", func(t *testing.T) {
		t.Parallel()

		got := decodeTuple(rel, tuple(
			textColumn("1"),
			textColumn("a@example.com"),
			textColumn("hello"),
		))

		want := map[string]any{"id": "1", "email": "a@example.com", "bio": "hello"}
		assertColumns(t, got, want)
	})

	t.Run("null is a present key with a nil value", func(t *testing.T) {
		t.Parallel()

		got := decodeTuple(rel, tuple(
			textColumn("1"),
			&pglogrepl.TupleDataColumn{DataType: tupleNull},
			textColumn("hello"),
		))

		value, present := got["email"]
		if !present {
			t.Fatal("a SQL NULL should be present in the map")
		}
		if value != nil {
			t.Fatalf("email = %v, want nil", value)
		}
	})

	// An unchanged TOASTed value is not transmitted. Recording it as nil would be
	// indistinguishable from a real NULL and would blank the column in any consumer
	// replaying the event.
	t.Run("unchanged toast is omitted, not nulled", func(t *testing.T) {
		t.Parallel()

		got := decodeTuple(rel, tuple(
			textColumn("1"),
			textColumn("a@example.com"),
			&pglogrepl.TupleDataColumn{DataType: tupleUnchanged},
		))

		if _, present := got["bio"]; present {
			t.Fatalf("an unchanged TOASTed column should be absent, got %v", got["bio"])
		}
		if len(got) != 2 {
			t.Fatalf("got %d columns, want 2", len(got))
		}
	})

	t.Run("nil tuple decodes to nil", func(t *testing.T) {
		t.Parallel()

		if got := decodeTuple(rel, nil); got != nil {
			t.Fatalf("decodeTuple(nil) = %v, want nil", got)
		}
	})

	// A relation message that arrived before a schema change can describe fewer
	// columns than the tuple carries; indexing past it must not panic.
	t.Run("more tuple columns than the relation describes", func(t *testing.T) {
		t.Parallel()

		got := decodeTuple(rel, tuple(
			textColumn("1"),
			textColumn("a@example.com"),
			textColumn("hello"),
			textColumn("surplus"),
		))

		if len(got) != 3 {
			t.Fatalf("got %d columns, want 3", len(got))
		}
	})

	t.Run("fewer tuple columns than the relation describes", func(t *testing.T) {
		t.Parallel()

		got := decodeTuple(rel, tuple(textColumn("1")))
		assertColumns(t, got, map[string]any{"id": "1"})
	})
}

// Each event owns its maps outright. They used to come from a sync.Pool and were
// cleared as soon as the loop body returned, silently emptying any event a consumer
// kept.
func TestDecodedMapsAreNotShared(t *testing.T) {
	t.Parallel()

	rel := testRelation()

	first := decodeTuple(rel, tuple(textColumn("1"), textColumn("a@example.com"), textColumn("x")))
	second := decodeTuple(rel, tuple(textColumn("2"), textColumn("b@example.com"), textColumn("y")))

	if first["id"] != "1" || second["id"] != "2" {
		t.Fatalf("maps alias each other: first=%v second=%v", first, second)
	}

	clear(second)
	if first["id"] != "1" {
		t.Fatal("clearing one event's map emptied another's")
	}
}

func TestDecodeOperations(t *testing.T) {
	t.Parallel()

	rel := testRelation()
	const lsn = uint64(0xDEAD)

	t.Run("insert", func(t *testing.T) {
		t.Parallel()

		event := decodeInsert(rel, &pglogrepl.InsertMessage{
			RelationID: rel.RelationID,
			Tuple:      tuple(textColumn("1"), textColumn("a@example.com"), textColumn("x")),
		}, lsn, committed)

		assertHeader(t, event, cdc.OpInsert, lsn)
		if event.Before != nil {
			t.Error("an insert has no before image")
		}
		if event.After["id"] != "1" {
			t.Errorf("After[id] = %v, want 1", event.After["id"])
		}
	})

	t.Run("update carries both images", func(t *testing.T) {
		t.Parallel()

		event := decodeUpdate(rel, &pglogrepl.UpdateMessage{
			RelationID: rel.RelationID,
			OldTuple:   tuple(textColumn("1"), textColumn("old@example.com"), textColumn("x")),
			NewTuple:   tuple(textColumn("1"), textColumn("new@example.com"), textColumn("x")),
		}, lsn, committed)

		assertHeader(t, event, cdc.OpUpdate, lsn)
		if event.Before["email"] != "old@example.com" {
			t.Errorf("Before[email] = %v", event.Before["email"])
		}
		if event.After["email"] != "new@example.com" {
			t.Errorf("After[email] = %v", event.After["email"])
		}
	})

	// Under the default REPLICA IDENTITY an update sends no old tuple at all.
	t.Run("update without an old tuple", func(t *testing.T) {
		t.Parallel()

		event := decodeUpdate(rel, &pglogrepl.UpdateMessage{
			RelationID: rel.RelationID,
			NewTuple:   tuple(textColumn("1"), textColumn("a@example.com"), textColumn("x")),
		}, lsn, committed)

		if event.Before != nil {
			t.Errorf("Before = %v, want nil", event.Before)
		}
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		event := decodeDelete(rel, &pglogrepl.DeleteMessage{
			RelationID: rel.RelationID,
			OldTuple:   tuple(textColumn("1"), textColumn("a@example.com"), textColumn("x")),
		}, lsn, committed)

		assertHeader(t, event, cdc.OpDelete, lsn)
		if event.After != nil {
			t.Error("a delete has no after image")
		}
		if event.Before["id"] != "1" {
			t.Errorf("Before[id] = %v, want 1", event.Before["id"])
		}
	})
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

func assertHeader(t *testing.T, event cdc.Event, op cdc.Op, lsn uint64) {
	t.Helper()

	if event.Op != op {
		t.Errorf("Op = %v, want %v", event.Op, op)
	}
	if event.Schema != "public" || event.Table != "users" {
		t.Errorf("relation = %s.%s, want public.users", event.Schema, event.Table)
	}
	if event.Position != position(lsn) {
		t.Errorf("Position = %q, want %q", event.Position, position(lsn))
	}
	// The commit time comes from the Begin that opened the transaction, so every
	// change decoded from it carries the same value.
	if !event.Timestamp.Equal(committed) {
		t.Errorf("Timestamp = %s, want %s", event.Timestamp, committed)
	}
}

func assertColumns(t *testing.T, got, want map[string]any) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d columns, want %d: %v", len(got), len(want), got)
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %v, want %v", name, got[name], value)
		}
	}
}
