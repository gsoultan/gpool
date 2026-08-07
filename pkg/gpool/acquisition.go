// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"time"
)

// Acquisition reports cumulative, monotonically increasing acquisition counters.
//
// Occupancy alone cannot answer the question that actually matters when tuning a
// pool — "is MaxConns too low?" — because a pool that is permanently full looks
// identical to one that is merely busy. These counters separate the two: if
// EmptyAcquireCount stays near zero, callers never waited and the pool is large
// enough. If it tracks AcquireCount, every caller is queueing.
type Acquisition interface {
	// AcquireCount returns how many connections have been successfully acquired.
	AcquireCount() int64
	// AcquireDuration returns the total time callers spent waiting for a
	// connection. Only waits are counted; an acquisition served immediately
	// contributes nothing. Divide by EmptyAcquireCount for the mean wait.
	AcquireDuration() time.Duration
	// EmptyAcquireCount returns how many acquisitions had to wait because no
	// connection was free. This is the pressure signal.
	EmptyAcquireCount() int64
	// CanceledAcquireCount returns how many acquisitions gave up because the
	// caller's context ended first. A rising count means callers are timing out.
	CanceledAcquireCount() int64

	// WaitingAcquires returns how many callers are parked for a connection at
	// this instant.
	//
	// This is the one gauge among these counters, and it answers a question none
	// of the cumulative ones can: EmptyAcquireCount says the pool has been short
	// at some point since it started, not that it is short now. A dashboard
	// graphing saturation, or anything deciding whether to grow the pool, needs
	// the instantaneous depth.
	WaitingAcquires() int32
}
