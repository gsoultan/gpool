// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"time"

	"github.com/jackc/pgx/v5"
)

// idleConn is a pooled connection together with the timestamps the reaper needs.
// It is owned by exactly one place at a time: a shard's idle slice, or the
// connWrapper currently handed out to a caller.
type idleConn struct {
	conn      *pgx.Conn
	createdAt time.Time
	idleSince time.Time

	// listening records that the connection has an active LISTEN. A subscription
	// belongs to the session, so without this the next caller would silently
	// inherit notifications it never asked for.
	listening bool
}

// txStatusIdle is the ReadyForQuery status byte meaning no transaction is open.
const txStatusIdle = 'I'

// dead reports whether the underlying connection can no longer carry traffic.
// A nil connection counts as dead so a bookkeeping slip degrades into a discarded
// connection rather than a panic on a hot path.
func (ic *idleConn) dead() bool {
	return ic.conn == nil || ic.conn.IsClosed()
}

// busy reports whether the connection is mid-protocol with an unread result.
// Nothing can safely be sent on it, including a cleanup statement.
func (ic *idleConn) busy() bool {
	return !ic.dead() && ic.conn.PgConn().IsBusy()
}

// inTransaction reports whether a transaction is still open or has failed.
//
// A connection returned in either state carries the previous caller's work into
// the next one: an open transaction holds its locks and eventually its snapshot,
// and a failed one rejects every statement until it is unwound.
func (ic *idleConn) inTransaction() bool {
	return !ic.dead() && ic.conn.PgConn().TxStatus() != txStatusIdle
}

// expired reports whether the connection has outlived either bound.
// A non-positive bound disables that check.
func (ic *idleConn) expired(now time.Time, maxLifetime, maxIdle time.Duration) bool {
	if maxLifetime > 0 && now.Sub(ic.createdAt) >= maxLifetime {
		return true
	}
	if maxIdle > 0 && !ic.idleSince.IsZero() && now.Sub(ic.idleSince) >= maxIdle {
		return true
	}
	return false
}

// usable reports whether the connection can still be handed to a caller.
func (ic *idleConn) usable(now time.Time, maxLifetime, maxIdle time.Duration) bool {
	return !ic.dead() && !ic.expired(now, maxLifetime, maxIdle)
}
