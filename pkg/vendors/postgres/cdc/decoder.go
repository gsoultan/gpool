// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	"github.com/jackc/pglogrepl"
)

// Column data-type markers used by the pgoutput protocol.
const (
	tupleNull      = 'n' // SQL NULL
	tupleText      = 't' // value present, in text format
	tupleUnchanged = 'u' // TOASTed value that did not change, not transmitted
)

// decodeInsert converts an insert record into an event.
func decodeInsert(rel *pglogrepl.RelationMessage, m *pglogrepl.InsertMessage, lsn uint64) cdc.Event {
	return cdc.Event{
		Op:     cdc.OpInsert,
		Schema: rel.Namespace,
		Table:  rel.RelationName,
		LSN:    lsn,
		After:  decodeTuple(rel, m.Tuple),
	}
}

// decodeUpdate converts an update record into an event. Before is populated only
// for the columns the table's REPLICA IDENTITY covers, and is nil under the default
// identity unless the primary key changed.
func decodeUpdate(rel *pglogrepl.RelationMessage, m *pglogrepl.UpdateMessage, lsn uint64) cdc.Event {
	return cdc.Event{
		Op:     cdc.OpUpdate,
		Schema: rel.Namespace,
		Table:  rel.RelationName,
		LSN:    lsn,
		Before: decodeTuple(rel, m.OldTuple),
		After:  decodeTuple(rel, m.NewTuple),
	}
}

// decodeDelete converts a delete record into an event.
func decodeDelete(rel *pglogrepl.RelationMessage, m *pglogrepl.DeleteMessage, lsn uint64) cdc.Event {
	return cdc.Event{
		Op:     cdc.OpDelete,
		Schema: rel.Namespace,
		Table:  rel.RelationName,
		LSN:    lsn,
		Before: decodeTuple(rel, m.OldTuple),
	}
}

// decodeTuple converts a wire tuple into a column map.
//
// The map is freshly allocated per tuple and handed to the consumer outright. An
// earlier design recycled these through a sync.Pool, which silently cleared the map
// under any consumer that kept the event past the current loop iteration.
//
// Values are returned as strings, exactly as pgoutput transmits them in text
// format; decoding to Go types would need the destination type, which the
// replication stream does not carry.
func decodeTuple(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	if tuple == nil {
		return nil
	}

	columns := min(len(tuple.Columns), len(rel.Columns))
	data := make(map[string]any, columns)

	for i := range columns {
		col := tuple.Columns[i]
		name := rel.Columns[i].Name

		switch col.DataType {
		case tupleNull:
			data[name] = nil
		case tupleText:
			data[name] = string(col.Data)
		case tupleUnchanged:
			// A TOASTed value that did not change is not sent. Recording it as nil
			// would be indistinguishable from a real NULL and would blank the column
			// in any consumer replaying the event, so the key is omitted instead.
		}
	}
	return data
}
