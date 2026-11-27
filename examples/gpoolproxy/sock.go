// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"io"
	"sync/atomic"
	"syscall"
)

// sock is one non-blocking socket as a worker sees it: a descriptor, and whatever
// had to be remembered between events.
//
// In the steady state that is nothing. Bytes are read into the worker's buffer,
// parsed there, and written straight to the peer, so neither buffer below exists
// unless a message arrived split or the peer's socket was full. That is the whole
// reason an idle client costs a few hundred bytes rather than tens of kilobytes.
//
// Everything except owner is guarded by the lock of the session that holds the
// socket.
type sock struct {
	fd int

	// backend is set for a server connection and nil for a client. It lets a
	// worker condemn a pooled connection that produced an event while nobody was
	// using it.
	backend *backend

	// owner is the session using this socket: always, for a client; only while
	// checked out, for a backend. It is how a worker holding nothing but a
	// descriptor number finds the lock to take, so it is read atomically.
	owner atomic.Pointer[session]

	// in is the unparsed tail of the input: a message whose rest has not arrived,
	// or whole messages that could not be forwarded yet.
	in []byte

	// out is output the kernel would not take, and sent is how much of it has
	// since gone. While out is non-nil the peer feeding this socket is not read,
	// which is the backpressure that bounds it.
	out  []byte
	sent int

	// pass is how much of the current message's body is still to be forwarded
	// without being looked at, and drop how much is to be discarded. They are
	// what lets a message larger than any buffer cross the proxy in pieces.
	pass int
	drop int

	// readable records that the kernel may be holding input. Readiness is
	// edge-triggered, so it is this flag, not another event, that says a socket
	// left unread on purpose still has to be read.
	readable bool

	// ended records that the peer has closed or the socket has failed. A short
	// read normally proves the socket is drained; once this is set it no longer
	// does, because the end-of-stream is still to be collected.
	ended bool
}

// read fills the worker's buffer from the socket. It returns zero and no error
// when there is nothing to read right now.
func (sk *sock) read(buf []byte) (int, error) {
	for {
		n, err := syscall.Read(sk.fd, buf)
		switch {
		case err == syscall.EINTR:
			continue
		case err == syscall.EAGAIN:
			sk.readable = false
			return 0, nil
		case err != nil:
			return 0, err
		case n == 0:
			return 0, io.EOF
		}

		// A read that did not fill the buffer emptied the socket, so the next
		// read would only be told so. Skipping it is one system call saved per
		// query in each direction.
		if n < len(buf) && !sk.ended {
			sk.readable = false
		}
		return n, nil
	}
}

// write hands bytes to the kernel and reports how many it took. Taking fewer
// than offered is not an error; it means the peer is slow.
func (sk *sock) write(p []byte) (int, error) {
	for {
		n, err := syscall.Write(sk.fd, p)
		switch {
		case err == syscall.EINTR:
			continue
		case err == syscall.EAGAIN:
			return 0, nil
		case err != nil:
			return 0, err
		}
		return n, nil
	}
}

// send writes bytes to the socket, keeping whatever the kernel would not take.
//
// Nothing is copied when the socket accepts everything, which is the usual case:
// the bytes go from the worker's read buffer into the peer's socket and are never
// held by the proxy at all. Anything already queued goes first, so order holds.
func (sk *sock) send(w *worker, p []byte) error {
	if len(p) == 0 {
		return nil
	}
	if sk.out == nil {
		n, err := sk.write(p)
		if err != nil {
			return err
		}
		if p = p[n:]; len(p) == 0 {
			return nil
		}
	}
	sk.queue(w, p)
	return nil
}

// queue holds bytes for the next flush without attempting a write. It is for a
// message that should leave in the same segment as what follows it.
func (sk *sock) queue(w *worker, p []byte) {
	if sk.out == nil {
		sk.out = w.borrow()
	}
	sk.out = append(sk.out, p...)
}

// flush writes queued output. It reports whether the queue is now empty.
func (sk *sock) flush(w *worker) (bool, error) {
	for sk.out != nil {
		n, err := sk.write(sk.out[sk.sent:])
		if err != nil {
			return false, err
		}
		if n == 0 {
			return false, nil
		}
		if sk.sent += n; sk.sent == len(sk.out) {
			w.giveBack(sk.out)
			sk.out, sk.sent = nil, 0
		}
	}
	return true, nil
}

// retain keeps the unparsed tail of the input for the next event. stored says
// the tail is a suffix of sk.in itself rather than of the worker's buffer.
func (sk *sock) retain(w *worker, rest []byte, stored bool) {
	switch {
	case len(rest) == 0:
		sk.forget(w)
	case stored:
		sk.in = sk.in[:copy(sk.in, rest)]
	case len(rest) <= smallTail:
		// Sized to fit. This is the request a client waits behind when every
		// backend is busy, and at three thousand waiting clients a standard
		// buffer apiece is twelve megabytes holding sixty bytes each.
		sk.in = append(make([]byte, 0, len(rest)), rest...)
	default:
		sk.in = append(w.borrow(), rest...)
	}
}

// forget drops any held input.
func (sk *sock) forget(w *worker) {
	if sk.in != nil {
		w.giveBack(sk.in)
		sk.in = nil
	}
}

// reset returns everything held between events, for a socket changing hands or
// closing.
func (sk *sock) reset(w *worker) {
	sk.forget(w)
	if sk.out != nil {
		w.giveBack(sk.out)
		sk.out, sk.sent = nil, 0
	}
	sk.pass, sk.drop = 0, 0
}

// midMessage reports whether the socket was left part-way through a message in
// either direction, which makes the stream unusable by anyone else.
func (sk *sock) midMessage() bool {
	return sk.pass > 0 || sk.drop > 0 || len(sk.in) > 0 || sk.out != nil
}

// sockTable finds a socket by its descriptor.
//
// A poller reports a descriptor number, and something has to turn that back into
// the socket. Indexing by the number itself needs no lookup structure and no
// lock; descriptors are small integers the kernel reuses, so the table is as
// large as the process's descriptor limit and no larger.
//
// A reused number can deliver a stale event to a newer socket. That is harmless
// by construction: every socket is non-blocking, so a readiness report that turns
// out to be untrue costs one read that says "nothing here".
type sockTable struct {
	slots []atomic.Pointer[sock]
}

func newSockTable(size int) *sockTable {
	return &sockTable{slots: make([]atomic.Pointer[sock], size)}
}

// put records a socket, reporting false if its descriptor is beyond the table.
func (t *sockTable) put(sk *sock) bool {
	if sk.fd < 0 || sk.fd >= len(t.slots) {
		return false
	}
	t.slots[sk.fd].Store(sk)
	return true
}

func (t *sockTable) get(fd int) *sock {
	if fd < 0 || fd >= len(t.slots) {
		return nil
	}
	return t.slots[fd].Load()
}

// drop forgets a socket. It must happen before the descriptor is closed, or the
// number could be reused and overwritten in between.
func (t *sockTable) drop(sk *sock) {
	if sk.fd >= 0 && sk.fd < len(t.slots) {
		t.slots[sk.fd].CompareAndSwap(sk, nil)
	}
}
