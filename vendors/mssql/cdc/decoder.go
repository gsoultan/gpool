// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// Values SQL Server writes into __$operation.
const (
	opDelete      = 1
	opInsert      = 2
	opUpdateOld   = 3
	opUpdateNew   = 4
	metadataPfx   = "__$"
	commitTimeCol = "__$commit_time"
	startLSNCol   = "__$start_lsn"
	seqvalCol     = "__$seqval"
	operationCol  = "__$operation"
)

// change is one row from a capture instance, before before/after images have
// been paired up.
type change struct {
	startLSN  []byte
	seqval    []byte
	operation int
	committed time.Time
	schema    string
	table     string
	values    map[string]any
}

// scanChanges reads every row a capture function returned.
//
// The column set is discovered rather than assumed: a capture instance's shape
// follows its source table, and the metadata columns SQL Server prepends are
// recognised by their __$ prefix rather than by position, so a future server that
// adds one does not shift the data columns out from under the decoder.
func scanChanges(rows *sql.Rows, schema, table string) ([]change, error) {
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var changes []change
	for rows.Next() {
		cells := make([]any, len(names))
		for i := range cells {
			cells[i] = new(any)
		}
		if err := rows.Scan(cells...); err != nil {
			return nil, err
		}

		row := change{schema: schema, table: table, values: make(map[string]any, len(names))}
		for i, name := range names {
			value := *(cells[i].(*any))
			switch name {
			case commitTimeCol:
				if t, ok := value.(time.Time); ok {
					row.committed = t.UTC()
				}
			case startLSNCol:
				row.startLSN, _ = value.([]byte)
			case seqvalCol:
				row.seqval, _ = value.([]byte)
			case operationCol:
				row.operation = asInt(value)
			default:
				if strings.HasPrefix(name, metadataPfx) {
					continue
				}
				row.values[name] = normalize(value)
			}
		}
		changes = append(changes, row)
	}
	return changes, rows.Err()
}

// pair turns capture rows into change events.
//
// Under "all update old" an update arrives as two rows, the before image then
// the after image, and they belong to one event. Everything else is one row to
// one event.
func pair(changes []change) []cdc.Event {
	events := make([]cdc.Event, 0, len(changes))

	for i := 0; i < len(changes); i++ {
		row := changes[i]
		event := cdc.Event{
			Schema:   row.schema,
			Table:    row.table,
			Position: position(row.startLSN),
			// Every row a transaction produced shares its __$start_lsn, so the
			// position names the transaction as well as the change.
			Transaction: position(row.startLSN),
			Timestamp:   row.committed,
		}

		switch row.operation {
		case opInsert:
			event.Op = cdc.OpInsert
			event.After = row.values
		case opDelete:
			event.Op = cdc.OpDelete
			event.Before = row.values
		case opUpdateOld:
			// The after image is the next row. Without it there is nothing to
			// report as an update, so a truncated pair is dropped rather than
			// delivered as a change with no new value.
			if i+1 >= len(changes) || changes[i+1].operation != opUpdateNew {
				continue
			}
			i++
			event.Op = cdc.OpUpdate
			event.Before = row.values
			event.After = changes[i].values
		case opUpdateNew:
			// An after image with no before image in front of it, which happens
			// when a window boundary splits the pair.
			event.Op = cdc.OpUpdate
			event.After = row.values
		default:
			continue
		}
		events = append(events, event)
	}
	return events
}

// normalize converts driver values into what a consumer owns.
//
// Byte slices are copied into strings: the driver reuses its buffers between
// rows, and Event maps are documented as safe to retain.
func normalize(value any) any {
	if raw, ok := value.([]byte); ok {
		return string(raw)
	}
	return value
}

// asInt reads __$operation, which the driver may hand back as any integer width.
func asInt(value any) int {
	switch v := value.(type) {
	case int64:
		return int(v)
	case int32:
		return int(v)
	case int:
		return v
	default:
		return 0
	}
}

// orderChanges sorts changes across capture instances into commit order.
//
// Each instance is queried separately, so their rows arrive in as many ordered
// runs as there are tables. A consumer replaying into another database needs one
// order, and (start_lsn, seqval) is the order the server committed them in.
func orderChanges(changes []change) {
	// A stable insertion sort: the input is already several sorted runs, which is
	// the case this is fastest on, and the batches are one poll interval's worth.
	for i := 1; i < len(changes); i++ {
		for j := i; j > 0 && lessThan(changes[j], changes[j-1]); j-- {
			changes[j], changes[j-1] = changes[j-1], changes[j]
		}
	}
}

func lessThan(a, b change) bool {
	if c := compareLSN(a.startLSN, b.startLSN); c != 0 {
		return c < 0
	}
	return compareLSN(a.seqval, b.seqval) < 0
}

// describe names a capture instance for an error message.
func describe(schema, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}
