// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// decodeRows converts one binlog row event into change events.
//
// A single binlog event carries every row a statement touched, so one event
// becomes many. They all share a position: the binlog names a place in the log,
// not a place within a statement, and resuming lands on the statement boundary
// either way.
func decodeRows(rows *replication.RowsEvent, op cdc.Op, names []string, at cdc.Position) []cdc.Event {
	schema, table := string(rows.Table.Schema), string(rows.Table.Table)

	// An update carries its rows in before/after pairs, so it produces half as
	// many events as it has rows.
	if op == cdc.OpUpdate {
		events := make([]cdc.Event, 0, len(rows.Rows)/2)
		for i := 0; i+1 < len(rows.Rows); i += 2 {
			events = append(events, cdc.Event{
				Op:       cdc.OpUpdate,
				Schema:   schema,
				Table:    table,
				Position: at,
				Before:   columnMap(names, rows.Rows[i]),
				After:    columnMap(names, rows.Rows[i+1]),
			})
		}
		return events
	}

	events := make([]cdc.Event, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		event := cdc.Event{
			Op:       op,
			Schema:   schema,
			Table:    table,
			Position: at,
		}
		if op == cdc.OpDelete {
			event.Before = columnMap(names, row)
		} else {
			event.After = columnMap(names, row)
		}
		events = append(events, event)
	}
	return events
}

// columnMap pairs a row's values with their column names.
//
// The map is freshly allocated per row and handed to the consumer outright, so
// it is safe to retain past the loop body that received it.
//
// Values keep the Go types the binlog parser produced — int64, float64, string,
// time.Time and so on — rather than being flattened to text the way pgoutput
// forces on the PostgreSQL vendor. Byte slices are copied into strings, both
// because the consumer owns what it is given and because a []byte here is
// almost always a VARCHAR or TEXT.
func columnMap(names []string, row []any) map[string]any {
	if len(row) == 0 {
		return nil
	}

	values := make(map[string]any, len(row))
	for i, value := range row {
		if i >= len(names) {
			break
		}
		if raw, ok := value.([]byte); ok {
			values[names[i]] = string(raw)
			continue
		}
		values[names[i]] = value
	}
	return values
}

// opOf maps a binlog event type to a change operation, reporting whether the
// event describes a row change at all.
//
// Every version of each row event is listed. v0 predates MySQL 5.1.15 and v2 is
// what current servers write; a subscriber that only understood v2 would go
// silent against an older source rather than fail, which is the worse outcome.
func opOf(eventType replication.EventType) (cdc.Op, bool) {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return cdc.OpInsert, true
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT:
		return cdc.OpUpdate, true
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return cdc.OpDelete, true
	default:
		return 0, false
	}
}
