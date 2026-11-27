// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"fmt"
	"time"

	"github.com/gsoultan/caduceus/wire"
	"github.com/gsoultan/nextar/pkg/pooling"
)

// connCloseTimeout bounds the graceful close of a single connection.
const connCloseTimeout = 5 * time.Second

// pgConn is a pooled connection plus the per-connection state this vendor needs.
type pgConn struct {
	session *session
	// listening records that the connection has an active LISTEN. A subscription
	// belongs to the session, so without this the next caller would silently
	// inherit notifications it never asked for. The transaction check does not
	// catch it: the transaction status stays idle.
	listening bool
}

// wireDriver teaches the engine how to dial, inspect, and clean up a connection.
type wireDriver struct {
	config   Config
	settings *settings
}

var _ pooling.Driver[*pgConn] = (*wireDriver)(nil)

// Connect establishes one new connection from the pre-parsed template.
func (d *wireDriver) Connect(ctx context.Context) (*pgConn, error) {
	connConfig := d.settings.conn
	if d.config.BeforeConnect != nil {
		connConfig = connConfig.Copy()
		if err := d.config.BeforeConnect(connConfig); err != nil {
			return nil, fmt.Errorf("gpool/postgres: BeforeConnect: %w", err)
		}
	}
	conn, err := wire.Connect(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("gpool/postgres: connect: %w", err)
	}
	c := &pgConn{session: newSession(conn, d.settings.statements, d.settings.descriptions)}
	if d.config.AfterConnect != nil {
		if err := d.config.AfterConnect(ctx, detachedConn{c: c}); err != nil {
			closeConn(conn)
			return nil, fmt.Errorf("gpool/postgres: AfterConnect: %w", err)
		}
	}
	return c, nil
}

// Close terminates a connection.
func (d *wireDriver) Close(_ context.Context, c *pgConn) error {
	closeConn(c.session.conn)
	return nil
}

// Dead reports whether a connection can no longer carry traffic. It performs no
// I/O: the engine consults it on the hot path. A nil connection counts as dead so
// a bookkeeping slip degrades into a discarded connection rather than a panic.
func (d *wireDriver) Dead(c *pgConn) bool {
	return c == nil || c.session == nil || c.session.conn.IsClosed()
}

// NeedsCleanup reports whether Recyclable has work to do, without any I/O.
//
// The overwhelmingly common release — a caller that ran a query, settled anything
// it started, and handed the connection back — needs nothing, and reporting that
// keeps the release path free of a deadline context it would never use.
func (d *wireDriver) NeedsCleanup(c *pgConn) bool {
	return c.listening ||
		d.config.ResetQuery != "" ||
		c.session.busy() ||
		c.inTransaction()
}

// Recyclable returns the connection to a clean state for the next caller.
//
// This is the pooling contract: whatever the previous caller left behind must not
// be observable by the next one. Each new kind of session state needs its own
// gate here — note that the transaction check does not catch a subscription.
func (d *wireDriver) Recyclable(ctx context.Context, c *pgConn) bool {
	// An unread result leaves the connection mid-protocol. Reading the rest of it
	// could take as long as the query does, so it is not salvaged.
	if c.session.busy() {
		return false
	}
	// A caller that returns a connection without settling its transaction must not
	// leak it onward. Unwinding costs one round trip; replacing the connection
	// costs a full reconnect, so unwinding is tried first.
	if c.inTransaction() && !c.rollback(ctx) {
		return false
	}
	// A LISTEN outlives the caller that registered it. Only connections that
	// actually listened pay for this.
	if c.listening && !c.unlisten(ctx) {
		return false
	}
	if d.config.ResetQuery != "" {
		if _, err := c.session.exec(ctx, d.config.ResetQuery, nil); err != nil {
			return false
		}
		c.session.forgetStatements()
	}
	return true
}

// inTransaction reports whether a transaction is still open or has failed.
//
// A connection returned in either state carries the previous caller's work into
// the next one: an open transaction holds its locks and eventually its snapshot,
// and a failed one rejects every statement until it is unwound.
//
// The status is caduceus's, not the wire's: a transaction begun and not yet
// sent to the server is open there and idle on the wire, and returned as idle
// its BEGIN would go out in front of the next caller's first statement.
func (c *pgConn) inTransaction() bool {
	return c.session.db.TxStatus() != wire.TxIdle
}

// rollback unwinds a transaction left open by the previous caller, reporting
// whether the connection came back to a clean idle state.
//
// caduceus does the unwinding, because it knows two things the wire does not:
// a transaction whose BEGIN was never sent has nothing to roll back, so that
// one costs no round trip; and the caller may still hold the Tx, which is spent
// here so it cannot reach whoever is handed the connection next.
func (c *pgConn) rollback(ctx context.Context) bool {
	if err := c.session.db.Unwind(ctx); err != nil {
		return false
	}
	return !c.inTransaction()
}

// unlisten clears every subscription, reporting whether it came back clean.
func (c *pgConn) unlisten(ctx context.Context) bool {
	if _, err := c.session.exec(ctx, unlistenAll, nil); err != nil {
		return false
	}
	c.listening = false
	return true
}

// closeConn closes a connection with a bounded, cancellation-immune context so
// shutdown still gets a chance to send a graceful Terminate.
func closeConn(conn *wire.Conn) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), connCloseTimeout)
	defer cancel()
	_ = conn.Close(ctx)
}
