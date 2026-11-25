// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/gsoultan/gpool/pkg/pooling"
)

// session is one authenticated client, served by the event-loop workers.
//
// It is a state machine rather than a pair of goroutines. A proxy is full duplex
// — a client may pipeline a second batch before the first is answered — and the
// blocking way to honour that is one goroutine per direction, which is two stacks
// and two buffers for every client whether or not it is doing anything. Here each
// direction is a function a worker calls when that socket has bytes, so the two
// directions still advance independently and a client that is doing nothing
// holds nothing.
//
// mu guards everything but waiting. Any worker may be handed an event for the
// session, and the lock is what makes the one that is serving it the only one.
// It is all but uncontended: two workers meet on it only when both of a
// session's sockets become ready in the same instant.
type session struct {
	proxy *Proxy

	mu sync.Mutex

	// worker is whichever worker is serving the session right now, and nil
	// otherwise. It is where the session finds a read buffer, which belongs to
	// the thread doing the reading and not to the client.
	worker *worker

	client sock
	key    cancelKey

	// backend is the server connection this session holds, and handle the
	// checkout it has to give back. Both are zero between transactions, which in
	// transaction-mode pooling is most of the time.
	backend *backend
	handle  pooling.Handle[*backend]

	// statements is every prepared statement this client has created, kept so it
	// can be replayed onto whichever backend the next transaction lands on.
	statements statements

	// expect is the completion messages the backend owes for work this proxy
	// injected rather than the client sending it. They are swallowed on the way
	// back, because a client counting replies to its own messages must not
	// receive one for a message it never sent.
	expect []byte

	// pending counts client messages that will each be answered with exactly one
	// ReadyForQuery, and open says the client has since sent extended-protocol
	// messages that a Sync has yet to close. A backend goes back to the pool only
	// when both are clear, so a pipelined client is fully answered — and a batch
	// that arrived in two segments is fully sent — before its backend is handed
	// on.
	pending int
	open    bool

	// cancels counts cancellation requests in flight against the held backend.
	// The backend is kept until they land: released, it could be running another
	// client's query by the time the server acted, and that query would be the
	// one cancelled.
	cancels int

	// waiting is set while the session is queued for a backend. It is the one
	// field other threads touch: whoever clears it owns the right to grant the
	// session a connection, which is what makes the queue safe to pop from any
	// loop.
	//
	// queued is the session's own view of the same thing, under its lock, and it
	// outlasts waiting: it stays set until the grant has actually arrived.
	// Between another thread claiming the session and delivering its connection
	// the session is no longer waiting but does not yet hold anything, and a
	// session that went looking for a backend in that window would end up with
	// two.
	waiting atomic.Bool
	queued  bool

	// stalled says the client's held input contains whole messages that could
	// not be forwarded for want of a backend, and so are worth parsing again.
	stalled bool

	scheduled bool
	closed    bool
}

// holds reports whether a socket is one this session is currently using.
func (s *session) holds(sk *sock) bool {
	return sk == &s.client || (s.backend != nil && sk == &s.backend.sock)
}

// ready handles one readiness report for either of the session's sockets.
func (s *session) ready(sk *sock, readable, writable, ended bool) {
	if ended {
		sk.ended = true
	}
	if readable {
		sk.readable = true
	}
	if writable && sk.out != nil {
		s.drain(sk)
	}
	if !s.closed {
		s.advance()
	}
}

// advance moves bytes in both directions for as long as there are any to move.
//
// The backend is drained first. Its replies are what complete a transaction and
// free the connection, and a session that has just been answered is the one
// whose client is about to send again.
func (s *session) advance() {
	s.pumpBackend()
	s.pumpClient()

	// A boundary is usually reached on a ReadyForQuery and settled there. This
	// catches the ones reached any other way — the last bytes of a message the
	// backend was slow to take, say — so a backend is never held for want of an
	// event to notice it is free.
	if !s.closed {
		s.settle()
	}
}

// drain writes output a socket previously refused.
func (s *session) drain(sk *sock) {
	if _, err := sk.flush(s.worker); err != nil {
		s.fault(sk)
	}
}

// fault ends the session after a socket failed. A backend that failed is not
// one to return to the pool.
func (s *session) fault(sk *sock) {
	if sk.backend != nil {
		sk.backend.fail()
	}
	s.close()
}

// pumpClient reads the client and forwards what it sent.
func (s *session) pumpClient() {
	client := &s.client

	for turn := 0; !s.closed; turn++ {
		// Queued for a backend: the request that needs one is already held, and
		// reading further would only buffer more behind it.
		if s.queued {
			return
		}
		// The backend has not taken what it was last sent. Reading the client
		// now would move that backlog from the kernel's memory into this
		// process's, without limit.
		if s.backend != nil && s.backend.sock.out != nil {
			return
		}

		if s.stalled {
			s.stalled = false
			s.forward(client.in, true)
			continue
		}
		if !client.readable {
			return
		}
		if turn >= fairShare {
			s.worker.schedule(s)
			return
		}

		n, err := client.read(s.worker.buf)
		if err != nil {
			s.close()
			return
		}
		if n == 0 {
			return
		}

		if client.in != nil {
			client.in = append(client.in, s.worker.buf[:n]...)
			s.forward(client.in, true)
		} else {
			s.forward(s.worker.buf[:n], false)
		}
	}
}

// forward parses client input, sends on what it can, and keeps the rest.
func (s *session) forward(data []byte, stored bool) {
	rest := s.feedClient(data)
	if s.closed {
		return
	}
	s.client.retain(s.worker, rest, stored)

	if s.backend != nil {
		s.drain(&s.backend.sock)
	}
}

// feedClient forwards as many whole client messages as it can and returns what
// is left: an incomplete message, or everything from the first message that
// needs a backend the session could not get.
//
// Messages that are forwarded untouched are not copied or written one by one.
// The run of them is sent as a single slice of the input, so a pipelined batch
// costs one write however many messages it holds.
func (s *session) feedClient(data []byte) []byte {
	client := &s.client
	sent, at := 0, 0

	// flush sends the run of untouched messages gathered so far.
	flush := func() bool {
		if at == sent {
			return true
		}
		if err := s.backend.sock.send(s.worker, data[sent:at]); err != nil {
			s.fault(&s.backend.sock)
			return false
		}
		sent = at
		return true
	}

	for at < len(data) {
		if client.pass > 0 {
			step := min(client.pass, len(data)-at)
			at += step
			if client.pass -= step; client.pass == 0 {
				if !flush() {
					return nil
				}
				s.settle()
			}
			continue
		}

		if len(data)-at < headerSize {
			break
		}
		kind := data[at]
		length := int(binary.BigEndian.Uint32(data[at+1:]))
		if length < 4 || length > maxMessageLen {
			s.close()
			return nil
		}
		total := 1 + length

		if kind == msgTerminate {
			s.close()
			return nil
		}

		// A message the proxy must understand, or one small enough to be cheap
		// to wait for, is taken whole. Only then is a backend acquired for it.
		whole := namesStatement(kind) || total <= wholeMessageLen
		if whole && len(data)-at < total {
			break
		}

		if s.backend == nil && !s.acquire() {
			s.stalled = !s.closed
			return data[at:]
		}
		if s.closed {
			return nil
		}

		if namesStatement(kind) {
			// Anything injected is queued behind the run sent so far and ahead
			// of this message, which joins the next run.
			s.reconcile(kind, data[at:at+total], flush)
			if s.closed {
				return nil
			}
			at += total
		} else {
			step := min(total, len(data)-at)
			at += step
			client.pass = total - step
		}

		switch {
		case endsTransactionUnit(kind):
			s.pending++
			s.open = false
		case opensTransactionUnit(kind):
			s.open = true
		}
	}

	if s.backend != nil && !flush() {
		return nil
	}
	return data[at:]
}

// reconcile makes the backend hold the prepared statement the message refers to.
//
// This is what makes named prepared statements survive transaction pooling. A
// statement lives on the backend that parsed it, and the next transaction may
// land somewhere else; replaying the remembered Parse first is the whole trick,
// and it is what PgBouncer added in 1.21.
//
// If it has to inject anything it first calls before, so messages the client
// sent earlier reach the backend ahead of the injection the current one depends
// on.
func (s *session) reconcile(kind byte, message []byte, before func() bool) {
	target := s.backend
	body := message[headerSize:]

	// inject writes a message the client did not send, and records the
	// completion the backend will answer with so it can be swallowed.
	injected := false
	inject := func(message []byte, completion byte) {
		if s.closed || (!injected && !before()) {
			return
		}
		injected = true
		target.sock.queue(s.worker, message)
		s.expect = append(s.expect, completion)
	}

	// hold records a statement as present on the backend, closing whatever had
	// to be discarded to make room for it.
	//
	// Forgetting alone would be a lie: the statement is on the server whether the
	// proxy remembers it or not, and a set that silently drifts below what the
	// server holds is how the server's memory grows without anything accounting
	// for it. The client that prepared the evicted statement is unharmed — its
	// own set still has the Parse, so the next backend it lands on gets it
	// replayed.
	hold := func(name string, message []byte) {
		if evicted, didEvict := target.prepared.remember(name, message); didEvict {
			inject(closeStatement(evicted), msgCloseComplete)
		}
	}

	switch kind {
	case msgParse:
		name, ok := parseName(body)
		if !ok || name == "" {
			// The unnamed statement is replaced by every Parse and never replayed.
			return
		}
		// A name already on this backend belongs to whoever put it there, which
		// may be a different client with different SQL. Re-parsing over it fails
		// with 42P05, and *not* re-parsing would silently run their statement
		// instead of this one — so it is closed first, either way.
		if _, held := target.prepared.lookup(name); held {
			inject(closeStatement(name), msgCloseComplete)
		}
		// Whatever this displaces from the client's own set simply stops being
		// replayable. It is still on the backend that has it, and that backend's
		// limit is what eventually takes it off the server.
		s.statements.remember(name, message)
		remembered, _ := s.statements.lookup(name)
		hold(name, remembered)

	case msgBind, msgDescribe, msgClose:
		name, ok := statementNameOf(kind, body)
		if !ok || len(name) == 0 {
			return
		}
		if kind == msgClose {
			// The client is deallocating it, so neither side should remember it.
			target.prepared.forget(string(name))
			s.statements.forget(string(name))
			return
		}

		remembered, known := s.statements.find(name)
		if !known {
			// The client is binding something it never parsed on this session.
			// The server will say so; forwarding is more honest than inventing
			// an error.
			return
		}
		if held, ok := target.prepared.find(name); ok {
			if bytes.Equal(held, remembered) {
				return
			}
			inject(closeStatement(string(name)), msgCloseComplete)
		}
		inject(remembered, msgParseComplete)
		hold(string(name), remembered)
	}
}

// pumpBackend reads the held backend and relays its replies to the client.
func (s *session) pumpBackend() {
	for turn := 0; !s.closed; turn++ {
		target := s.backend
		if target == nil || !target.sock.readable {
			return
		}
		// The client has not taken what it was last sent, so the backend is left
		// unread and the server blocks on its own socket — which is the same
		// backpressure a direct connection would apply.
		if s.client.out != nil {
			return
		}
		if turn >= fairShare {
			s.worker.schedule(s)
			return
		}

		n, err := target.sock.read(s.worker.buf)
		if err != nil {
			s.fault(&target.sock)
			return
		}
		if n == 0 {
			return
		}

		data, stored := s.worker.buf[:n], false
		if target.sock.in != nil {
			target.sock.in = append(target.sock.in, data...)
			data, stored = target.sock.in, true
		}

		rest := s.feedBackend(target, data)
		if s.closed {
			return
		}
		// A backend that was released while parsing has already been reset, and
		// is no longer this session's to write to.
		if s.backend == target {
			target.sock.retain(s.worker, rest, stored)
		}
		s.drain(&s.client)
	}
}

// feedBackend relays as many whole backend messages as it can and returns the
// incomplete tail. Like feedClient, a run of relayed messages leaves as one
// slice of the input.
func (s *session) feedBackend(source *backend, data []byte) []byte {
	from := &source.sock
	sent, at := 0, 0

	flush := func() bool {
		if at == sent {
			return true
		}
		if err := s.client.send(s.worker, data[sent:at]); err != nil {
			s.close()
			return false
		}
		sent = at
		return true
	}

	for at < len(data) {
		if from.pass > 0 {
			step := min(from.pass, len(data)-at)
			at += step
			from.pass -= step
			continue
		}
		if from.drop > 0 {
			if !flush() {
				return nil
			}
			step := min(from.drop, len(data)-at)
			at += step
			from.drop -= step
			sent = at
			continue
		}

		if len(data)-at < headerSize {
			break
		}
		kind := data[at]
		length := int(binary.BigEndian.Uint32(data[at+1:]))
		if length < 4 || length > maxMessageLen {
			s.fault(from)
			return nil
		}
		total := 1 + length

		// A completion for a Parse or Close this proxy injected is not the
		// client's to see: it is counting replies to its own messages.
		if len(s.expect) > 0 && s.expect[0] == kind {
			if !flush() {
				return nil
			}
			if s.expect = s.expect[1:]; len(s.expect) == 0 {
				s.expect = nil
			}
			step := min(total, len(data)-at)
			at += step
			from.drop = total - step
			sent = at
			continue
		}

		if kind == msgReadyForQuery && length == 5 {
			if len(data)-at < total {
				break
			}
			status := data[at+headerSize]
			at += total
			if !flush() {
				return nil
			}

			source.txStatus = status
			if s.pending > 0 {
				s.pending--
			}
			// Anything after the ReadyForQuery that ends the transaction has no
			// one to go to once the backend is handed on, so it cannot be.
			if at < len(data) && s.atBoundary() {
				source.fail()
			}
			if s.settle() {
				return nil
			}
			continue
		}

		step := min(total, len(data)-at)
		at += step
		from.pass = total - step
	}

	if !flush() {
		return nil
	}
	return data[at:]
}

// atBoundary reports whether the held backend is between transactions with
// nothing owed in either direction, which is the only state it may be handed on
// in.
//
// An open transaction keeps its backend even at zero pending: releasing it would
// hand another client a live transaction. Transaction-mode pooling means one
// backend per transaction, not one per statement.
func (s *session) atBoundary() bool {
	return s.pending == 0 && !s.open && s.client.pass == 0 &&
		s.backend.txStatus == txIdle && s.backend.sock.out == nil
}

// settle returns the backend to the pool if the session has reached a
// transaction boundary, and reports whether it did.
func (s *session) settle() bool {
	if s.backend == nil || s.cancels > 0 || !s.atBoundary() {
		return false
	}
	s.release()
	return true
}

// acquire attaches a backend, or queues the session for one. It reports whether
// the session now holds a backend.
func (s *session) acquire() bool {
	core := s.proxy.core

	if handle, ok := core.TryAcquire(); ok {
		s.attach(handle)
		return true
	}

	s.queued = true
	s.waiting.Store(true)
	s.proxy.waiters.push(s)

	// Looked for again now that the session is visibly waiting. A connection
	// released between the first attempt and the push was released to an empty
	// queue, and nobody would ever come back for this session.
	if handle, ok := core.TryAcquire(); ok {
		if s.waiting.CompareAndSwap(true, false) {
			s.queued = false
			s.attach(handle)
			return true
		}
		// Someone else is already granting this session a connection.
		s.proxy.restore(handle, s.worker)
		return false
	}

	s.proxy.grow()
	return false
}

// attach takes ownership of a checked-out backend and makes its events arrive on
// this session's loop.
func (s *session) attach(handle pooling.Handle[*backend]) {
	target := handle.Conn()
	s.handle = handle
	s.backend = target

	target.sock.owner.Store(s)
	if err := target.enlist(s.proxy.poller); err != nil {
		s.fault(&target.sock)
	}
}

// granted is the answer to a queued acquire: the request that was held is sent
// on at once, by whichever worker released the connection.
func (s *session) granted(handle pooling.Handle[*backend]) {
	s.queued = false
	if s.closed {
		s.proxy.restore(handle, s.worker)
		return
	}
	s.attach(handle)
	if !s.closed {
		s.advance()
	}
}

// refused is the answer to a queued acquire the pool could not satisfy: the
// server is unreachable or the pool is closed. The client is told, in the
// protocol's own terms, rather than left to time out.
func (s *session) refused() {
	s.queued = false
	if s.closed {
		return
	}
	_ = s.client.send(s.worker, upstreamUnavailable)
	s.close()
}

// release returns the held backend to the pool and offers it to whoever is
// waiting.
func (s *session) release() {
	target, handle := s.backend, s.handle
	s.backend = nil
	s.handle = pooling.Handle[*backend]{}
	s.expect = nil

	if target.sock.midMessage() {
		target.fail()
	}
	target.sock.reset(s.worker)
	target.sock.owner.Store(nil)

	// The usual release is a connection at a clean boundary, and costs no I/O.
	// One that needs rolling back or closing does, and that must not happen on a
	// thread a few thousand other clients are waiting on.
	if target.dirty() {
		target.delist(s.proxy.poller)
		s.proxy.background(func() {
			handle.Release()
			s.proxy.handoff(nil)
		})
		return
	}

	handle.Release()
	s.proxy.handoff(s.worker)
}

// close ends the session. It is idempotent.
func (s *session) close() {
	if s.closed {
		return
	}
	s.closed = true

	// Leave the queue. If this loses the race, a grant is already on its way and
	// granted will give the connection straight back.
	s.waiting.CompareAndSwap(true, false)

	if s.backend != nil {
		// A client that left in the middle of an exchange leaves the backend's
		// stream somewhere no one else can pick it up. Mid-transaction but
		// between messages is recoverable, and release rolls that back.
		// A cancellation still in flight is aimed at this backend, and
		// must not find another client's query on it.
		if s.pending > 0 || s.open || len(s.expect) > 0 || s.client.pass > 0 || s.cancels > 0 {
			s.backend.fail()
		}
		s.release()
	}

	s.proxy.socks.drop(&s.client)
	_ = syscall.Close(s.client.fd)
	s.client.reset(s.worker)
	s.proxy.departed(s)
}

// cancel forwards a cancellation request to the backend this session holds.
//
// PostgreSQL wants it on a separate connection, which is a dial and therefore
// not something to do on a worker. The backend is pinned until the request has
// been sent, so it cannot be handed to another client whose query would then be
// the one cancelled.
func (s *session) cancel() {
	if s.closed || s.backend == nil {
		return
	}
	pid, secret := s.backend.pid, s.backend.secret

	s.cancels++
	s.proxy.background(func() {
		s.proxy.sendCancel(pid, secret)
		// If the workers have gone they took the session with them, and there
		// is nothing left to unpin.
		s.proxy.post(func(w *worker) { w.serve(s, s.cancelled) })
	})
}

// cancelled runs on a worker once a cancellation request has been sent.
func (s *session) cancelled() {
	s.cancels--
	s.settle()
}
