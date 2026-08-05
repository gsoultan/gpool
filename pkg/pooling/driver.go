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

	// Recyclable reports whether a connection is fit to hand to the next caller,
	// cleaning up whatever the previous one left behind.
	//
	// This is the boundary that makes pooling safe. Whatever the previous caller
	// left — an open transaction, a subscription, session settings — must not be
	// observable by the next. Where cleaning up is cheaper than reconnecting, do
	// it here and report true; where the state cannot be established, report false
	// and the engine will discard the connection.
	Recyclable(ctx context.Context, conn C) bool
}
