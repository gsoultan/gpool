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
		lastPushed  uint64
		flushed     uint64
		serverEnd   uint64
		wantFlushed uint64
	}{
		{
			name:        "fully drained, catches up to the server",
			lastPushed:  100,
			flushed:     100,
			serverEnd:   500,
			wantFlushed: 500,
		},
		{
			name:        "events still buffered",
			buffered:    1,
			lastPushed:  100,
			flushed:     100,
			serverEnd:   500,
			wantFlushed: 100,
		},
		{
			name:        "consumer has not acknowledged the last event",
			lastPushed:  100,
			flushed:     50,
			serverEnd:   500,
			wantFlushed: 50,
		},
		{
			name:        "server is behind the confirmed position",
			lastPushed:  500,
			flushed:     500,
			serverEnd:   400,
			wantFlushed: 500,
		},
		{
			name:        "idle from the start",
			lastPushed:  0,
			flushed:     0,
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
			s.lastPushed.Store(tt.lastPushed)
			s.flushed.Store(tt.flushed)

			s.catchUp(tt.serverEnd)

			if got := s.flushed.Load(); got != tt.wantFlushed {
				t.Fatalf("flushed = %d, want %d", got, tt.wantFlushed)
			}
		})
	}
}

// catchUp keeps lastPushed and flushed equal so a following idle period can keep
// advancing rather than latching at the position of the last event.
func TestCatchUpKeepsPositionsAligned(t *testing.T) {
	t.Parallel()

	s := &pgEventStream{events: make(chan pendingEvent, 4)}
	s.lastPushed.Store(100)
	s.flushed.Store(100)

	s.catchUp(500)
	s.catchUp(900)

	if got := s.flushed.Load(); got != 900 {
		t.Fatalf("flushed = %d, want 900 after a second idle period", got)
	}
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
