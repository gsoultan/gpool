// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"github.com/gsoultan/gpool/pkg/pooling"
)

const (
	// fairShare is how many full reads one socket gets before the worker moves
	// on. A client streaming a large COPY would otherwise hold a worker, and
	// every event queued behind it, for as long as it kept sending.
	fairShare = 8

	// pollBatch is how many ready sockets a worker takes from the kernel at once.
	// Kept small on purpose: events a worker has taken wait for that worker, even
	// while another sits idle, so a large batch trades latency for nothing.
	pollBatch = 32
)

// worker is one event-loop thread: it waits for sockets to become ready and then
// moves bytes between them, never blocking on any one.
//
// The proxy runs one per core, all waiting on the same poller. A session is not
// tied to a worker — whichever worker the kernel hands an event to takes the
// session's lock, advances it, and lets go — so the invariant that one goroutine
// owns a connection holds one event at a time, with the goroutine shared by a few
// thousand connections instead of spent on each.
//
// The cost of a connected client is therefore not a stack and a pair of buffers
// but an entry in the kernel's interest list. This is the same shape as
// PgBouncer, which is where the idea is proven — with the difference that there
// are as many workers as cores, and PgBouncer is one.
type worker struct {
	proxy *Proxy

	events [pollBatch]pollEvent

	// buf is the buffer every socket this worker serves is read through. One per
	// worker, not one per connection, because a worker reads one socket at a
	// time.
	buf []byte

	// spare is returned borrow-buffers kept for reuse. Only this worker touches
	// it, so it is a plain stack rather than a sync.Pool.
	spare [][]byte

	// resume is the sessions with work that no kernel event will announce,
	// because a socket was left unread for fairness.
	resume []*session

	// grants is the backends this worker has released and matched to a waiting
	// session. They are delivered once the session being served is unlocked:
	// granting from inside it would mean taking a second session's lock while
	// holding the first.
	grants []grant
}

// grant is a checked-out backend on its way to the session that was waiting for
// it.
type grant struct {
	to     *session
	handle pooling.Handle[*backend]
}

func newWorker(proxy *Proxy) *worker {
	return &worker{proxy: proxy, buf: make([]byte, readBufSize)}
}

// run serves events until the proxy stops.
func (w *worker) run() {
	for !w.proxy.stopping.Load() {
		n, err := w.proxy.poller.wait(w.events[:], len(w.resume) == 0)
		if err != nil {
			return
		}
		for i := range n {
			w.dispatch(readiness(&w.events[i]))
		}
		w.runResumed()
	}
}

// dispatch delivers one readiness report to the session it concerns.
func (w *worker) dispatch(fd int, readable, writable, ended bool) {
	if fd == w.proxy.wakeR {
		w.proxy.runInbox(w)
		return
	}

	sk := w.proxy.socks.get(fd)
	if sk == nil {
		return
	}

	s := sk.owner.Load()
	if s == nil {
		// A pooled backend that nobody holds has spoken or hung up. An idle
		// server connection is silent, so either way it is not one to hand out.
		if sk.backend != nil && readable {
			sk.backend.fail()
		}
		return
	}

	w.serve(s, func() {
		// The report may be older than the session's hold on the socket: a
		// backend since released, or a descriptor number since reused.
		if s.holds(sk) {
			s.ready(sk, readable, writable, ended)
		}
	})
}

// serve runs work on a session under its lock, then delivers whatever backends
// that work released.
func (w *worker) serve(s *session, work func()) {
	s.mu.Lock()
	if !s.closed {
		s.worker = w
		work()
		s.worker = nil
	}
	s.mu.Unlock()

	w.deliver()
}

// deliver hands each released backend to the session it was matched with.
// Advancing that session may release another, so it runs until there are none.
func (w *worker) deliver() {
	for len(w.grants) > 0 {
		last := len(w.grants) - 1
		next := w.grants[last]
		w.grants[last] = grant{}
		w.grants = w.grants[:last]

		s := next.to
		s.mu.Lock()
		s.worker = w
		s.granted(next.handle)
		s.worker = nil
		s.mu.Unlock()
	}
}

func (w *worker) runResumed() {
	// Taken by length rather than ranged to the end: a session resumed here may
	// be scheduled again, and it waits for the next turn so kernel events are
	// not starved by one busy socket.
	for range len(w.resume) {
		s := w.resume[0]
		w.resume[0] = nil
		w.resume = w.resume[1:]

		w.serve(s, func() {
			s.scheduled = false
			s.advance()
		})
	}
	if len(w.resume) == 0 {
		w.resume = nil
	}
}

// schedule asks for a session to be advanced on this worker's next turn.
func (w *worker) schedule(s *session) {
	if s.scheduled || s.closed {
		return
	}
	s.scheduled = true
	w.resume = append(w.resume, s)
}

// borrow returns an empty buffer for bytes that must outlive the current event.
func (w *worker) borrow() []byte {
	if last := len(w.spare) - 1; last >= 0 {
		buf := w.spare[last]
		w.spare[last] = nil
		w.spare = w.spare[:last]
		return buf
	}
	return make([]byte, 0, spareBufSize)
}

// giveBack returns a borrowed buffer. One that grew past the standard size is
// left to the collector instead, so a single large message does not pin its
// size in the reuse list.
func (w *worker) giveBack(buf []byte) {
	if cap(buf) != spareBufSize || len(w.spare) >= spareBufKeep {
		return
	}
	w.spare = append(w.spare, buf[:0])
}
