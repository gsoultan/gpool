// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// Column kinds in a pgoutput tuple.
const (
	tupleNull      = 'n' // SQL NULL
	tupleText      = 't' // value present, in text format
	tupleUnchanged = 'u' // TOASTed value that did not change, not transmitted
	tupleBinary    = 'b' // value present, in binary format; never requested here
)

// decodeChange decodes an Insert, Update or Delete message into an event. body
// is what follows the relation id, which the caller read to find rel.
//
// The tuples are decoded straight from the message into the event's maps, with
// no intermediate representation: the only allocations are the maps and the
// values the consumer receives.
func decodeChange(op byte, rel *relation, body []byte, lsn uint64, tx transaction) (cdc.Event, error) {
	event := cdc.Event{
		Schema:      rel.namespace,
		Table:       rel.name,
		Position:    position(lsn),
		Timestamp:   tx.committed,
		Transaction: tx.position,
	}
	r := reader{data: body}
	switch op {
	case msgInsert:
		event.Op = cdc.OpInsert
		event.After = r.tupleAfter(rel, tupleNew)
	case msgUpdate:
		event.Op = cdc.OpUpdate
		// Before is sent only for what the table's REPLICA IDENTITY covers, and
		// not at all under the default identity unless the key changed.
		kind := r.peek()
		if kind == tupleKey || kind == tupleOld {
			event.Before = r.tupleAfter(rel, kind)
		}
		event.After = r.tupleAfter(rel, tupleNew)
	case msgDelete:
		event.Op = cdc.OpDelete
		kind := r.peek()
		if kind != tupleKey && kind != tupleOld {
			return cdc.Event{}, errMalformed
		}
		event.Before = r.tupleAfter(rel, kind)
	}
	return event, r.err
}

// peek returns the next byte without consuming it.
func (r *reader) peek() byte {
	if r.err != nil || len(r.data) == 0 {
		r.err = errMalformed
		return 0
	}
	return r.data[0]
}

// tupleAfter reads the tuple that follows a marker byte, checking the marker.
func (r *reader) tupleAfter(rel *relation, marker byte) map[string]any {
	if r.byte() != marker {
		r.err = errMalformed
		return nil
	}
	return r.tuple(rel)
}

// tuple decodes a tuple into a column map.
//
// The map is freshly allocated per tuple and handed to the consumer outright. An
// earlier design recycled these through a sync.Pool, which silently cleared the map
// under any consumer that kept the event past the current loop iteration.
//
// Values are returned as strings, exactly as pgoutput transmits them in text
// format; decoding to Go types would need the destination type, which the
// replication stream does not carry.
func (r *reader) tuple(rel *relation) map[string]any {
	count := int(r.uint16())
	if r.err != nil {
		return nil
	}
	data := make(map[string]any, min(count, len(rel.columns)))
	for i := range count {
		kind := r.byte()
		var value []byte
		if kind == tupleText || kind == tupleBinary {
			value = r.take(int(int32(r.uint32())))
		}
		if r.err != nil {
			return nil
		}
		if i >= len(rel.columns) {
			// More columns than the relation named: a table altered since its
			// Relation message would be sent a new one first, so this is
			// tolerated the way the earlier decoder tolerated it, by dropping
			// what cannot be named.
			continue
		}
		name := rel.columns[i]
		switch kind {
		case tupleNull:
			data[name] = nil
		case tupleText, tupleBinary:
			data[name] = string(value)
		case tupleUnchanged:
			// A TOASTed value that did not change is not sent. Recording it as nil
			// would be indistinguishable from a real NULL and would blank the column
			// in any consumer replaying the event, so the key is omitted instead.
		default:
			r.err = errMalformed
			return nil
		}
	}
	return data
}
