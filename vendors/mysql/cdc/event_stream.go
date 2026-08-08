// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// mysqlEventStream is a live binlog stream implementing cdc.EventStream.
//
// One goroutine owns the binlog connection for the stream's whole life: it
// reads events, decodes them, and hands changes to a buffered channel. Nothing
// else touches the syncer.
//
// Unlike the PostgreSQL stream there is no position to confirm. MySQL records
// nothing on the consumer's behalf, so falling behind costs the consumer its
// changes when the logs expire rather than costing the source its disk. The
// consumer's own copy of Event.Position is the only durable record of progress.
type mysqlEventStream struct {
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
	columns  *columns
	filter   *filter

	events chan cdc.Event
	ctx    context.Context
	cancel context.CancelFunc

	mu  sync.Mutex
	err error

	iterating atomic.Bool
	closeOnce sync.Once
	onClose   func()
	done      chan struct{}

	// start is where the stream was told to begin, and the position reported
	// with every change until the first transaction commits.
	start position
}

var _ cdc.EventStream = (*mysqlEventStream)(nil)

// newEventStream starts the reader goroutine for an already-started sync. The
// stream takes ownership of the syncer.
func newEventStream(parent context.Context, s *mysqlEventStream) *mysqlEventStream {
	// The stream outlives the call that created it, so it is detached from the
	// caller's context: cancelling Subscribe must not tear down a stream that is
	// already handed over. Close is the way to stop it.
	s.ctx, s.cancel = context.WithCancel(context.WithoutCancel(parent))
	s.done = make(chan struct{})

	go s.run()
	return s
}

// All iterates the stream's changes.
//
// There is no position to confirm on the consumer's behalf, so unlike the
// PostgreSQL stream nothing happens when the loop body returns. Record
// Event.Position durably before treating a change as processed; a crash resumes
// from whatever was last recorded.
func (s *mysqlEventStream) All() iter.Seq[cdc.Event] {
	return func(yield func(cdc.Event) bool) {
		// A second concurrent iteration would silently split the stream between
		// two consumers, each seeing an arbitrary subset.
		if !s.iterating.CompareAndSwap(false, true) {
			return
		}
		defer s.iterating.Store(false)
		defer func() { _ = s.Close() }()

		for event := range s.events {
			if !yield(event) {
				return
			}
		}
	}
}

// Err returns the error that terminated the stream, if any.
func (s *mysqlEventStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops the stream and releases its connection. It is idempotent.
func (s *mysqlEventStream) Close() error {
	s.closeOnce.Do(func() {
		// Cancel first so the reader stops asking for events, then wait for it
		// to exit before closing the syncer: the syncer is not safe to close
		// underneath a goroutine still reading from it.
		s.cancel()
		<-s.done
		s.syncer.Close()

		if s.onClose != nil {
			s.onClose()
		}
	})
	return nil
}

// run is the reader loop. It is the only goroutine that touches the streamer.
func (s *mysqlEventStream) run() {
	defer close(s.done)
	defer close(s.events)

	// resume is what a consumer hands back to replay from the start of the
	// transaction currently being read. It advances only at a commit, so a
	// change delivered mid-transaction reports the position before it — which
	// replays that transaction rather than skipping it.
	resume := s.start.encode()
	tracker := newTracker(s.start)

	for {
		event, err := s.streamer.GetEvent(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				s.setErr(err)
			}
			return
		}

		tracker.observe(event)

		switch body := event.Event.(type) {
		case *replication.RowsEvent:
			if !s.handleRows(event, body, resume) {
				return
			}
		case *replication.XIDEvent:
			resume = tracker.commit()
		case *replication.QueryEvent:
			// A storage engine without XID events — or a MariaDB transaction —
			// ends with a literal COMMIT statement instead.
			if strings.EqualFold(strings.TrimSpace(string(body.Query)), "COMMIT") {
				resume = tracker.commit()
			}
		}
	}
}

// handleRows decodes one row event and delivers its changes. It reports whether
// to keep running.
func (s *mysqlEventStream) handleRows(event *replication.BinlogEvent, rows *replication.RowsEvent, at cdc.Position) bool {
	op, ok := opOf(event.Header.EventType)
	if !ok {
		return true
	}
	if !s.filter.allows(string(rows.Table.Schema), string(rows.Table.Table)) {
		return true
	}

	// Every binlog event header carries the time the server wrote it, which for a
	// row event is the commit time of its transaction — whole seconds, which is
	// all the format records.
	committed := time.Unix(int64(event.Header.Timestamp), 0).UTC()

	names, err := s.columns.names(s.ctx, rows.Table, int(rows.ColumnCount))
	if err != nil {
		if s.ctx.Err() == nil {
			s.setErr(err)
		}
		return false
	}

	for _, change := range decodeRows(rows, op, names, at, committed) {
		select {
		case s.events <- change:
		case <-s.ctx.Done():
			return false
		}
	}
	return true
}

// setErr records the first error to terminate the stream.
func (s *mysqlEventStream) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// tracker follows the binary log's own bookkeeping so a committed position can
// be named in whichever notation the stream started with.
type tracker struct {
	gtid mysql.GTIDSet
	next string
	file mysql.Position
}

func newTracker(start position) *tracker {
	// Cloned for the same reason the syncer gets its own copy: two goroutines
	// advancing one GTID set race on the map inside it.
	var gtid mysql.GTIDSet
	if start.gtid != nil {
		gtid = start.gtid.Clone()
	}
	return &tracker{gtid: gtid, file: start.file}
}

// observe updates the tracker from one binlog event.
func (t *tracker) observe(event *replication.BinlogEvent) {
	switch body := event.Event.(type) {
	case *replication.RotateEvent:
		// Sent for real rotations and once, artificially, at the start of a
		// dump — which is how the file name is learned in the first place.
		t.file = mysql.Position{Name: string(body.NextLogName), Pos: uint32(body.Position)}
		return
	case *replication.GTIDEvent:
		if next, err := body.GTIDNext(); err == nil {
			t.next = next.String()
		}
	case *replication.MariadbGTIDEvent:
		t.next = body.GTID.String()
	}

	if event.Header.LogPos > 0 {
		t.file.Pos = event.Header.LogPos
	}
}

// commit returns the position to resume from now that a transaction has ended.
func (t *tracker) commit() cdc.Position {
	if t.gtid != nil && t.next != "" {
		// Update is what folds the finished transaction's GTID into the set; a
		// set updated at the GTID event instead would claim a transaction as
		// executed before it committed.
		_ = t.gtid.Update(t.next)
		t.next = ""
		return position{gtid: t.gtid}.encode()
	}
	return position{file: t.file}.encode()
}
