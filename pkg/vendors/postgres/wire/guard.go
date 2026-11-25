// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"context"
	"fmt"
	"time"
)

// cancelTimeout bounds the CancelRequest sent when a context ends mid-query.
const cancelTimeout = 5 * time.Second

// Guard ties one exchange on a connection to a context. Start it with Watch
// before the first write and end it with Done after the last read.
//
// A context with neither a deadline nor a Done channel — context.Background —
// costs nothing: no timer, no goroutine, no allocation.
type Guard struct {
	conn     *Conn
	ctx      context.Context
	stop     func() bool
	deadline bool
}

// Watch starts guarding an exchange with ctx. If ctx ends while a read or write
// is blocked, that call returns a timeout, which Done turns into ctx's error.
func (c *Conn) Watch(ctx context.Context) Guard {
	g := Guard{conn: c, ctx: ctx}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.netConn.SetDeadline(deadline)
		g.deadline = true
	}
	if ctx.Done() != nil {
		netConn := c.netConn
		g.stop = context.AfterFunc(ctx, func() { _ = netConn.SetDeadline(time.Unix(1, 0)) })
	}
	return g
}

// Done ends the exchange and reports how it went, given the error the exchange
// itself returned.
//
// When the context ended during the exchange, the server is asked to cancel
// whatever it is running and the connection is closed: where its stream stands
// is no longer known, and a pool must not hand it to anyone. The returned error
// then wraps the context's, so errors.Is(err, context.DeadlineExceeded) holds.
func (g Guard) Done(err error) error {
	fired := g.stop != nil && !g.stop()
	// Every deadline on the connection comes from the context, so a timeout
	// means it ended, even in the instant before ctx.Err() says so.
	guarded := g.deadline || g.stop != nil
	if fired || (err != nil && guarded && isTimeout(err)) {
		g.conn.abort()
		awaitExpiry(g.ctx)
		cause := context.Cause(g.ctx)
		if cause == nil {
			cause = context.DeadlineExceeded
		}
		if err == nil {
			// The exchange finished as the context ended. Its result stands,
			// but the connection's deadline is already in the past.
			return nil
		}
		return fmt.Errorf("gpool/postgres: %w (%w)", cause, err)
	}
	if g.deadline && !g.conn.closed.Load() {
		_ = g.conn.netConn.SetDeadline(time.Time{})
	}
	return err
}

// abort asks the server to cancel the running query and closes the connection.
// The cancel request travels on a connection of its own, from a goroutine, so
// that a slow server does not make the caller wait past its own deadline.
func (c *Conn) abort() {
	if c.closed.Load() {
		return
	}
	_ = c.fail(ErrClosed)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
		defer cancel()
		_ = c.CancelRequest(ctx)
	}()
}
