// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"sync"
	"sync/atomic"
)

// waiters is the sessions queued for a backend, oldest first.
//
// The pooling engine queues callers by parking them, which is the right thing
// for a goroutine and impossible for an event loop: a loop that parked would
// take every client it serves with it. So a session that finds the pool empty
// waits here instead, as one pointer in a queue, and whichever thread next
// returns a connection hands it over.
//
// That also keeps a saturated proxy cheap. Three thousand clients sharing sixteen
// backends means nearly three thousand waiting at any moment, and a parked
// goroutine each would put back the memory the event loop exists to save.
type waiters struct {
	mu    sync.Mutex
	queue []*session
	head  int

	// count mirrors the queue's length so the release path — every transaction
	// — can see an empty queue without taking the lock.
	count atomic.Int32
}

func (w *waiters) push(s *session) {
	w.mu.Lock()
	w.queue = append(w.queue, s)
	w.count.Add(1)
	w.mu.Unlock()
}

// pop returns the longest-waiting session that still wants a connection, or nil.
//
// A session leaves the queue by clearing its waiting flag, not by being removed,
// so entries can be stale: the session gave up, or found a connection by itself.
// Winning the flag is what makes the caller the one, and only one, to grant it.
func (w *waiters) pop() *session {
	w.mu.Lock()
	defer w.mu.Unlock()

	for w.head < len(w.queue) {
		s := w.queue[w.head]
		w.queue[w.head] = nil
		w.head++
		w.count.Add(-1)

		if s.waiting.CompareAndSwap(true, false) {
			w.compact()
			return s
		}
	}
	w.compact()
	return nil
}

// compact reclaims the consumed front of the queue once it is the larger half,
// so the slice does not grow for as long as the proxy stays busy.
func (w *waiters) compact() {
	if w.head == len(w.queue) {
		w.queue, w.head = w.queue[:0], 0
		return
	}
	if w.head > len(w.queue)/2 {
		w.queue = w.queue[:copy(w.queue, w.queue[w.head:])]
		w.head = 0
	}
}

func (w *waiters) empty() bool {
	return w.count.Load() == 0
}
