// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"sync/atomic"
)

// Handle is a checked-out connection. A vendor wraps one to implement gpool.Conn.
//
// One handle is allocated per acquisition and is never recycled. That is
// deliberate: its lifetime is controlled by user code, so pooling it would let a
// second Release from one goroutine hand a live connection back while another
// goroutine is still using it. A guard flag stored inside a recycled object
// cannot prevent that — after it is reused the flag has been reset.
// Handle is returned by value so a vendor can store it inline in its own
// connection wrapper and pay one allocation for the pair rather than two. That is
// why released is a plain word operated on atomically rather than an atomic.Bool:
// atomic.Bool carries a noCopy marker, which would make returning a Handle a vet
// error. Store it once and use only that copy — a second copy would carry its own
// release flag and could return the connection twice.
type Handle[C any] struct {
	core     *Core[C]
	idle     *idleConn[C]
	conn     C
	shardIdx int
	released uint32
}

// Conn returns the driver's connection. It stays valid until Release.
func (h *Handle[C]) Conn() C {
	return h.conn
}

// Released reports whether the handle has been returned to the pool.
// A vendor checks this before using the connection so a use-after-release is an
// error rather than a corrupted session.
func (h *Handle[C]) Released() bool {
	return atomic.LoadUint32(&h.released) == 1
}

// Release returns the connection to the pool. It is idempotent; releasing twice
// is a no-op rather than a double return that would corrupt pool accounting.
func (h *Handle[C]) Release() {
	if !atomic.CompareAndSwapUint32(&h.released, 0, 1) {
		return
	}
	h.core.release(h.idle, h.shardIdx)
}
