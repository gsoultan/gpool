// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestAdvanceIsMonotonic(t *testing.T) {
	t.Parallel()

	var position atomic.Uint64

	advance(&position, 100)
	if got := position.Load(); got != 100 {
		t.Fatalf("position = %d, want 100", got)
	}

	// A lower position must never rewind a confirmed one.
	advance(&position, 50)
	if got := position.Load(); got != 100 {
		t.Fatalf("position = %d, want 100 after a lower advance", got)
	}

	advance(&position, 150)
	if got := position.Load(); got != 150 {
		t.Fatalf("position = %d, want 150", got)
	}
}

func TestAdvanceUnderConcurrency(t *testing.T) {
	t.Parallel()

	var position atomic.Uint64
	var wg sync.WaitGroup

	const writers = 16
	for i := range writers {
		wg.Go(func() {
			advance(&position, uint64(i+1))
		})
	}
	wg.Wait()

	if got := position.Load(); got != writers {
		t.Fatalf("position = %d, want %d", got, writers)
	}
}

// Without catch-up the confirmed position only ever moves on a row change, so a
// publication that goes quiet pins every WAL segment written since the last event
// and eventually fills the primary's disk.
func TestCatchUpAdvancesOnlyWhenDrained(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		buffered    int
		pushed      uint64
		acked       uint64
		flushed     uint64
		serverEnd   uint64
		wantFlushed uint64
	}{
		{
			name:        "fully drained, catches up to the server",
			pushed:      1,
			acked:       1,
			flushed:     100,
			serverEnd:   500,
			wantFlushed: 500,
		},
		{
			name:        "events still buffered",
			buffered:    1,
			pushed:      2,
			acked:       1,
			flushed:     100,
			serverEnd:   500,
			wantFlushed: 100,
		},
		{
			name:        "consumer has not acknowledged the last event",
			pushed:      2,
			acked:       1,
			flushed:     50,
			serverEnd:   500,
			wantFlushed: 50,
		},
		{
			name:        "server is behind the confirmed position",
			pushed:      1,
			acked:       1,
			flushed:     500,
			serverEnd:   400,
			wantFlushed: 500,
		},
		{
			name:        "idle from the start",
			serverEnd:   900,
			wantFlushed: 900,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &pgEventStream{events: make(chan pendingEvent, 4)}
			for range tt.buffered {
				s.events <- pendingEvent{}
			}
			s.pushed.Store(tt.pushed)
			s.acked.Store(tt.acked)
			s.flushed.Store(tt.flushed)

			s.catchUp(tt.serverEnd)

			if got := s.flushed.Load(); got != tt.wantFlushed {
				t.Fatalf("flushed = %d, want %d", got, tt.wantFlushed)
			}
		})
	}
}

// A following idle period must keep advancing rather than latching at the
// position of the last event.
func TestCatchUpKeepsAdvancingAcrossIdlePeriods(t *testing.T) {
	t.Parallel()

	s := &pgEventStream{events: make(chan pendingEvent, 4)}
	deliver(s, 100)

	s.catchUp(500)
	s.catchUp(900)

	if got := s.flushed.Load(); got != 900 {
		t.Fatalf("flushed = %d, want 900 after a second idle period", got)
	}
}

// pgoutput sends transactions in commit order, but each change carries the LSN
// of its own record. T1 writes at 60 and commits last; T2 writes at 90 and
// commits first; so T2's change is delivered before T1's lower-positioned one.
//
// catchUp used to require the last pushed position (60) to equal the confirmed
// one (90, a maximum). After an interleave they never matched again, so a quiet
// publication stopped confirming the server's WAL end and pinned WAL on the
// primary until the next published change.
func TestCatchUpAdvancesAfterInterleavedTransactions(t *testing.T) {
	t.Parallel()

	s := &pgEventStream{events: make(chan pendingEvent, 4)}
	deliver(s, 90) // T2, committed first
	deliver(s, 60) // T1, written first

	s.catchUp(500)

	if got := s.flushed.Load(); got != 500 {
		t.Fatalf("flushed = %d, want 500: the slot is pinned after interleaved transactions", got)
	}
}

// The tempting fix for the interleave — comparing against the highest pushed
// position — would lose data. With T1's lower-positioned change still inside the
// consumer's loop body, the highest pushed and the confirmed position are both
// T2's, and confirming the server's end would skip T1 on restart. Only a count
// knows that an event is still outstanding.
func TestCatchUpWaitsForALowerPositionedEventInFlight(t *testing.T) {
	t.Parallel()

	s := &pgEventStream{events: make(chan pendingEvent, 4)}
	deliver(s, 90)

	// T1's change has left the channel but its loop body has not returned.
	s.pushed.Add(1)

	s.catchUp(500)

	if got := s.flushed.Load(); got != 90 {
		t.Fatalf("flushed = %d, want 90 while an event is still being processed", got)
	}
}

// deliver records one event as pushed by the reader and fully processed by the
// consumer, in the order emit and All do it.
func deliver(s *pgEventStream, lsn uint64) {
	s.pushed.Add(1)
	advance(&s.flushed, lsn)
	s.acked.Add(1)
}

func TestStreamErrRecordsTheFirstFailure(t *testing.T) {
	t.Parallel()

	s := &pgEventStream{}
	if s.Err() != nil {
		t.Fatal("a fresh stream should have no error")
	}

	first := errors.New("connection reset")
	s.setErr(first)
	s.setErr(errors.New("later noise"))

	if !errors.Is(s.Err(), first) {
		t.Fatalf("Err() = %v, want the first failure", s.Err())
	}
}

// A second concurrent iteration would split the stream between two consumers, each
// silently seeing an arbitrary subset of the changes.
func TestAllRefusesConcurrentIteration(t *testing.T) {
	t.Parallel()

	s := &pgEventStream{
		events: make(chan pendingEvent, 1),
		done:   make(chan struct{}),
	}
	close(s.done)
	s.iterating.Store(true)

	for range s.All() {
		t.Fatal("a second iteration should yield nothing")
	}
}
