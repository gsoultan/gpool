// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// Stat is an immutable snapshot of pool occupancy and cumulative acquisition
// counters, implementing gpool.Stat. Every vendor reports the same numbers
// because they all come from the same engine.
type Stat struct {
	total    int32
	idle     int32
	active   int32
	waiting  int32
	maxConns int32

	acquires  int64
	waitNanos int64
	empties   int64
	canceled  int64

	expired   int64
	unhealthy int64
	evicted   int64
}

var _ gpool.Stat = Stat{}

// Every engine accounts for its connections, so Lifecycle is available on every
// vendor rather than being a capability some have. It is still reached by
// assertion, because gpool.Stat is what a consumer holds and adding to that
// interface would break anyone implementing it.
var _ gpool.Lifecycle = Stat{}

// TotalConnections returns every connection the pool currently owns.
func (s Stat) TotalConnections() int32 {
	return s.total
}

// IdleConnections returns the connections sitting in the pool ready for reuse.
func (s Stat) IdleConnections() int32 {
	return s.idle
}

// ActiveConnections returns the connections currently checked out.
func (s Stat) ActiveConnections() int32 {
	return s.active
}

// MaxConnections returns the configured ceiling.
func (s Stat) MaxConnections() int32 {
	return s.maxConns
}

// WaitingAcquires returns how many callers are parked for a connection right now.
func (s Stat) WaitingAcquires() int32 {
	return s.waiting
}

// AcquireCount returns how many connections have been successfully acquired.
func (s Stat) AcquireCount() int64 {
	return s.acquires
}

// AcquireDuration returns the total time callers spent waiting for a connection.
func (s Stat) AcquireDuration() time.Duration {
	return time.Duration(s.waitNanos)
}

// EmptyAcquireCount returns how many acquisitions had to wait for a free connection.
func (s Stat) EmptyAcquireCount() int64 {
	return s.empties
}

// CanceledAcquireCount returns how many acquisitions ended with the caller's context.
func (s Stat) CanceledAcquireCount() int64 {
	return s.canceled
}

// ExpiredConnections returns connections retired for reaching MaxConnLifetime or
// MaxConnIdleTime.
func (s Stat) ExpiredConnections() int64 {
	return s.expired
}

// UnhealthyConnections returns connections discarded because they were dead,
// never became ready, or failed their reset.
func (s Stat) UnhealthyConnections() int64 {
	return s.unhealthy
}

// EvictedConnections returns connections closed to obey a lowered ceiling or an
// explicit EvictIdle.
func (s Stat) EvictedConnections() int64 {
	return s.evicted
}
