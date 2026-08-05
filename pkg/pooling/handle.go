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
type Handle[C any] struct {
	core     *Core[C]
	idle     *idleConn[C]
	conn     C
	shardIdx int
	released atomic.Bool
}

// Conn returns the driver's connection. It stays valid until Release.
func (h *Handle[C]) Conn() C {
	return h.conn
}

// Released reports whether the handle has been returned to the pool.
// A vendor checks this before using the connection so a use-after-release is an
// error rather than a corrupted session.
func (h *Handle[C]) Released() bool {
	return h.released.Load()
}

// Release returns the connection to the pool. It is idempotent; releasing twice
// is a no-op rather than a double return that would corrupt pool accounting.
func (h *Handle[C]) Release() {
	if !h.released.CompareAndSwap(false, true) {
		return
	}
	h.core.release(h.idle, h.shardIdx)
}
