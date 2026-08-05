// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// Batch is a set of statements sent to the server in one round trip.
//
// Batching trades nothing for latency: the statements are pipelined rather than
// waiting for each other's replies, so N statements cost roughly one round trip
// instead of N. Over a link with any real latency that is the single largest win
// available to a chatty workload.
//
// The zero value is ready to use.
type Batch struct {
	queries []BatchQuery
}

// BatchQuery is one queued statement.
type BatchQuery struct {
	SQL       string
	Arguments []any
}

// Queue appends a statement to the batch.
func (b *Batch) Queue(sql string, arguments ...any) {
	b.queries = append(b.queries, BatchQuery{SQL: sql, Arguments: arguments})
}

// Len returns how many statements are queued.
func (b *Batch) Len() int {
	return len(b.queries)
}

// Queries returns the queued statements in order. The slice aliases the batch's
// own storage; treat it as read-only.
func (b *Batch) Queries() []BatchQuery {
	return b.queries
}

// Reset empties the batch so it can be reused without reallocating.
func (b *Batch) Reset() {
	clear(b.queries)
	b.queries = b.queries[:0]
}
