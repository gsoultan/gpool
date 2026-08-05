// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"fmt"
	"time"

	"github.com/gsoultan/gpool/pkg/pooling"
	"github.com/jackc/pgx/v5"
)

// txStatusIdle is the ReadyForQuery status byte meaning no transaction is open.
const txStatusIdle = 'I'

// connCloseTimeout bounds the graceful close of a single connection.
const connCloseTimeout = 5 * time.Second

// pgConn is a pooled connection plus the per-connection state this vendor needs.
//
// The engine is generic over this type rather than over *pgx.Conn, which is what
// lets the vendor keep its own bookkeeping — here, whether the connection has an
// active LISTEN — without a side table keyed by pointer.
type pgConn struct {
	conn *pgx.Conn

	// listening records that the connection has an active LISTEN. A subscription
	// belongs to the session, so without this the next caller would silently
	// inherit notifications it never asked for. The transaction check does not
	// catch it: TxStatus stays 'I'.
	listening bool
}

// pgxDriver teaches the engine how to dial, inspect, and clean up a pgx connection.
type pgxDriver struct {
	config     Config
	connConfig *pgx.ConnConfig
}

var _ pooling.Driver[*pgConn] = (*pgxDriver)(nil)

// Connect establishes one new connection from the pre-parsed template.
func (d *pgxDriver) Connect(ctx context.Context) (*pgConn, error) {
	connConfig := d.connConfig.Copy()
	if d.config.BeforeConnect != nil {
		if err := d.config.BeforeConnect(connConfig); err != nil {
			return nil, fmt.Errorf("gpool/postgres: BeforeConnect: %w", err)
		}
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("gpool/postgres: connect: %w", err)
	}

	if d.config.AfterConnect != nil {
		if err := d.config.AfterConnect(conn); err != nil {
			closeConn(conn)
			return nil, fmt.Errorf("gpool/postgres: AfterConnect: %w", err)
		}
	}
	return &pgConn{conn: conn}, nil
}

// Close terminates a connection.
func (d *pgxDriver) Close(_ context.Context, c *pgConn) error {
	closeConn(c.conn)
	return nil
}

// Dead reports whether a connection can no longer carry traffic. It performs no
// I/O: the engine consults it on the hot path, so it only reads state pgx already
// has. A nil connection counts as dead so a bookkeeping slip degrades into a
// discarded connection rather than a panic.
func (d *pgxDriver) Dead(c *pgConn) bool {
	return c == nil || c.conn == nil || c.conn.IsClosed()
}

// NeedsCleanup reports whether Recyclable has work to do, without any I/O.
//
// The overwhelmingly common release — a caller that ran a query, settled anything
// it started, and handed the connection back — needs nothing, and reporting that
// keeps the release path free of a deadline context it would never use.
func (d *pgxDriver) NeedsCleanup(c *pgConn) bool {
	return c.listening ||
		d.config.ResetQuery != "" ||
		c.conn.PgConn().IsBusy() ||
		c.inTransaction()
}

// Recyclable returns the connection to a clean state for the next caller.
//
// This is the pooling contract: whatever the previous caller left behind must not
// be observable by the next one. Each new kind of session state needs its own
// gate here — note that the transaction check does not catch a subscription.
func (d *pgxDriver) Recyclable(ctx context.Context, c *pgConn) bool {
	// An unread result leaves the connection mid-protocol. Nothing can be sent on
	// it, not even a cleanup statement, so it cannot be salvaged.
	if c.conn.PgConn().IsBusy() {
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
		if _, err := c.conn.Exec(ctx, d.config.ResetQuery); err != nil {
			return false
		}
	}
	return true
}

// inTransaction reports whether a transaction is still open or has failed.
//
// A connection returned in either state carries the previous caller's work into
// the next one: an open transaction holds its locks and eventually its snapshot,
// and a failed one rejects every statement until it is unwound.
func (c *pgConn) inTransaction() bool {
	return c.conn.PgConn().TxStatus() != txStatusIdle
}

// rollback unwinds a transaction left open by the previous caller, reporting
// whether the connection came back to a clean idle state.
func (c *pgConn) rollback(ctx context.Context) bool {
	if _, err := c.conn.Exec(ctx, "ROLLBACK"); err != nil {
		return false
	}
	return !c.inTransaction()
}

// unlisten clears every subscription, reporting whether it came back clean.
func (c *pgConn) unlisten(ctx context.Context) bool {
	if _, err := c.conn.Exec(ctx, unlistenAll); err != nil {
		return false
	}
	c.listening = false
	return true
}

// closeConn closes a pgx connection with a bounded, cancellation-immune context so
// shutdown still gets a chance to send a graceful Terminate.
func closeConn(conn *pgx.Conn) {
	if conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), connCloseTimeout)
	defer cancel()
	_ = conn.Close(ctx)
}
