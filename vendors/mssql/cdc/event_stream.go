// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// sqlEventStream polls SQL Server's change tables, implementing cdc.EventStream.
//
// One goroutine owns the polling for the stream's whole life: it reads each
// capture instance in turn, orders what it finds, and hands the changes to a
// buffered channel.
//
// There is no position to confirm. SQL Server retains changes on a timer rather
// than until a consumer acknowledges them, so falling behind costs the consumer
// its changes when the cleanup job runs — the same failure mode as an expiring
// MySQL binlog, and the opposite of a PostgreSQL slot filling the primary's disk.
type sqlEventStream struct {
	db       *sql.DB
	subject  *SQLServer
	interval time.Duration
	timeout  time.Duration

	events chan cdc.Event
	ctx    context.Context
	cancel context.CancelFunc

	mu  sync.Mutex
	err error

	iterating atomic.Bool
	closeOnce sync.Once
	onClose   func()
	done      chan struct{}

	// from is the first LSN the next poll will include.
	from []byte
}

var _ cdc.EventStream = (*sqlEventStream)(nil)

func newEventStream(parent context.Context, s *sqlEventStream) *sqlEventStream {
	// Detached from the caller's context: cancelling Subscribe must not tear down
	// a stream that has already been handed over. Close is the way to stop it.
	s.ctx, s.cancel = context.WithCancel(context.WithoutCancel(parent))
	s.done = make(chan struct{})

	go s.run()
	return s
}

// All iterates the stream's changes.
//
// Nothing is confirmed when the loop body returns, because there is nothing on
// the server to confirm to. Record Event.Position durably before treating a
// change as processed; a crash resumes from whatever was last recorded.
func (s *sqlEventStream) All() iter.Seq[cdc.Event] {
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

func (s *sqlEventStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops the stream. It is idempotent.
func (s *sqlEventStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done
		if s.onClose != nil {
			s.onClose()
		}
	})
	return nil
}

// run is the polling loop. It is the only goroutine that reads the change tables.
func (s *sqlEventStream) run() {
	defer close(s.done)
	defer close(s.events)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		if !s.poll() {
			return
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// poll reads one window of changes and delivers them. It reports whether to
// keep running.
func (s *sqlEventStream) poll() bool {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()

	to, err := s.maxLSN(ctx)
	if err != nil {
		return s.fail(err)
	}
	// Nothing committed past where we are: the capture job has not moved, which
	// on an idle database is every poll.
	if to == nil || compareLSN(to, s.from) < 0 {
		return true
	}

	instances, err := s.subject.trackedInstances(ctx)
	if err != nil {
		return s.fail(err)
	}

	var batch []change
	for _, instance := range instances {
		found, err := s.readInstance(ctx, instance, to)
		if err != nil {
			return s.fail(err)
		}
		batch = append(batch, found...)
	}

	orderChanges(batch)
	for _, event := range pair(batch) {
		select {
		case s.events <- event:
		case <-s.ctx.Done():
			return false
		}
	}

	next, err := s.incrementLSN(ctx, to)
	if err != nil {
		return s.fail(err)
	}
	s.from = next
	return true
}

// readInstance reads one capture instance's changes in [from, to].
//
// The window is clamped to what this instance actually holds. Each capture
// instance has its own oldest LSN — it begins where sp_cdc_enable_table left it
// and moves forward as the cleanup job runs — so a window that reaches back
// before it is not merely empty. The capture function rejects the whole call
// with "an insufficient number of arguments were supplied", which names neither
// the argument nor the reason, and a stream against a freshly enabled table
// would fail on its first poll rather than wait for the capture job to catch up.
func (s *sqlEventStream) readInstance(ctx context.Context, instance captureInstance, to []byte) ([]change, error) {
	oldest, err := s.minLSN(ctx, instance)
	if err != nil {
		return nil, err
	}
	// No oldest LSN at all: capture is enabled but the job has not yet produced
	// anything for it, which is the normal state for the first seconds.
	if oldest == nil {
		return nil, nil
	}

	from := s.from
	if compareLSN(from, oldest) < 0 {
		from = oldest
	}
	if compareLSN(from, to) > 0 {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx, changesSQL(instance.name), from, to)
	if err != nil {
		return nil, fmt.Errorf("gpool/mssql/cdc: reading changes for %s in [%s, %s]: %w",
			describe(instance.schema, instance.table), position(from), position(to), err)
	}
	defer rows.Close()

	return scanChanges(rows, instance.schema, instance.table)
}

// minLSN is the oldest change an instance still holds, or nil if it holds none.
func (s *sqlEventStream) minLSN(ctx context.Context, instance captureInstance) ([]byte, error) {
	var oldest []byte
	if err := s.db.QueryRowContext(ctx, minLSNSQL, instance.name).Scan(&oldest); err != nil {
		return nil, fmt.Errorf("gpool/mssql/cdc: reading the oldest retained LSN for %s: %w",
			describe(instance.schema, instance.table), err)
	}
	if len(oldest) != lsnLen {
		return nil, nil
	}
	return oldest, nil
}

func (s *sqlEventStream) maxLSN(ctx context.Context) ([]byte, error) {
	var lsn []byte
	if err := s.db.QueryRowContext(ctx, maxLSNSQL).Scan(&lsn); err != nil {
		return nil, fmt.Errorf("gpool/mssql/cdc: reading the maximum LSN: %w", err)
	}
	return lsn, nil
}

func (s *sqlEventStream) incrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
	var next []byte
	if err := s.db.QueryRowContext(ctx, incrementLSNSQL, lsn).Scan(&next); err != nil {
		return nil, fmt.Errorf("gpool/mssql/cdc: advancing past %s: %w", position(lsn), err)
	}
	if len(next) != lsnLen {
		return nil, fmt.Errorf("gpool/mssql/cdc: advancing past %s returned %d bytes, want %d",
			position(lsn), len(next), lsnLen)
	}
	return next, nil
}

// fail records the error that ended the stream and reports "stop".
func (s *sqlEventStream) fail(err error) bool {
	if s.ctx.Err() == nil {
		s.mu.Lock()
		if s.err == nil {
			s.err = err
		}
		s.mu.Unlock()
	}
	return false
}
