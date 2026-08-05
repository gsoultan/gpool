// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"time"
)

// idleConn is a pooled connection together with the timestamps the reaper needs.
// It is owned by exactly one place at a time: a shard's idle slice, or the Handle
// currently checked out by a caller.
type idleConn[C any] struct {
	conn      C
	createdAt time.Time
	idleSince time.Time
}

// expired reports whether the connection has outlived either bound.
// A non-positive bound disables that check.
func (ic *idleConn[C]) expired(now time.Time, maxLifetime, maxIdle time.Duration) bool {
	if maxLifetime > 0 && now.Sub(ic.createdAt) >= maxLifetime {
		return true
	}
	if maxIdle > 0 && !ic.idleSince.IsZero() && now.Sub(ic.idleSince) >= maxIdle {
		return true
	}
	return false
}
