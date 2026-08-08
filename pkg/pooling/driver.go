// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Package pooling is the vendor-agnostic connection pooling engine.
//
// It owns everything that is the same for every database — capacity, lock-striped
// idle buckets, the background reaper, lifecycle, and statistics — and leaves each
// vendor only what is genuinely vendor-specific: how to dial a connection, how to
// tell whether one is still healthy, and how to return it to a clean state.
//
// The engine is generic over the driver's own connection type rather than hidden
// behind an interface, so nothing on the acquire path pays for dynamic dispatch.
package pooling

import (
	"context"
)

// Driver is what the engine needs from a vendor in order to manage connections.
//
// C is the driver's native connection type — `*pgx.Conn`, `driver.Conn`, whatever
// the vendor actually holds. Keeping it a type parameter means the engine never
// boxes it, and the vendor never type-asserts it back.
type Driver[C any] interface {
	// Connect establishes one new connection.
	Connect(ctx context.Context) (C, error)

	// Close terminates a connection. It is called for every connection the pool
	// discards, and must tolerate one that is already broken.
	Close(ctx context.Context, conn C) error

	// Dead reports whether a connection can no longer carry traffic. It must not
	// perform I/O: it is consulted on the hot path, so it can only look at state
	// the driver already has.
	Dead(conn C) bool

	// NeedsCleanup reports whether Recyclable has any work to do. Like Dead it
	// must not perform I/O.
	//
	// This exists to keep the common release free of cost. Bounding Recyclable
	// means building a context with a deadline, and that is four allocations and
	// a runtime timer — measured at over half a microsecond on a path that is
	// otherwise around two hundred nanoseconds. A connection returned with nothing
	// left on it should pay none of that, so the engine only builds the context
	// when the driver says there is something to clean.

	NeedsCleanup(conn C) bool

	// Recyclable reports whether a connection is fit to hand to the next caller,
	// cleaning up whatever the previous one left behind. It is called only when
	// NeedsCleanup reported true, and its context carries CleanupTimeout.
	//
	// This is the boundary that makes pooling safe. Whatever the previous caller
	// left — an open transaction, a subscription, session settings — must not be
	// observable by the next. Where cleaning up is cheaper than reconnecting, do
	// it here and report true; where the state cannot be established, report false
	// and the engine will discard the connection.
	Recyclable(ctx context.Context, conn C) bool
}

// ReadinessChecker is an optional Driver capability.
//
// A connection can carry traffic the moment Connect returns for most vendors,
// but some protocols need a per-connection setup exchange first — a startup
// packet, an authentication handshake, a session preamble. That step is often
// performed by the caller rather than the driver, because it depends on who is
// asking: a proxy forwards the client's own startup packet, so the pool hands
// out a raw socket and the caller completes it.
//
// The failure this prevents is quiet. A connection acquired and released
// without completing that exchange goes back to the idle set indistinguishable
// from a ready one. The next caller to assume readiness — typically a health
// check that sends a query — gets no valid response and condemns the whole
// backend, long after the acquisition that caused it.
//
// A driver that has such a step implements this, and the pool destroys rather
// than pools a connection that never became ready. Drivers without one do not
// implement it and are unaffected.
type ReadinessChecker[C any] interface {
	// Ready reports whether the connection has completed its setup.
	//
	// Like Dead it must not perform I/O: it is consulted on release, so it can
	// only look at state the driver already has.
	Ready(conn C) bool
}
