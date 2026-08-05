// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// poolStat is an immutable snapshot of pool occupancy and cumulative acquisition
// counters, implementing gpool.Stat.
type poolStat struct {
	total    int32
	idle     int32
	active   int32
	maxConns int32

	acquires  int64
	waitNanos int64
	empties   int64
	canceled  int64
}

var _ gpool.Stat = poolStat{}

// TotalConnections returns every connection the pool currently owns.
func (s poolStat) TotalConnections() int32 {
	return s.total
}

// IdleConnections returns the connections sitting in the pool ready for reuse.
func (s poolStat) IdleConnections() int32 {
	return s.idle
}

// ActiveConnections returns the connections currently checked out.
func (s poolStat) ActiveConnections() int32 {
	return s.active
}

// MaxConnections returns the configured ceiling.
func (s poolStat) MaxConnections() int32 {
	return s.maxConns
}

// AcquireCount returns how many connections have been successfully acquired.
func (s poolStat) AcquireCount() int64 {
	return s.acquires
}

// AcquireDuration returns the total time callers spent waiting for a connection.
func (s poolStat) AcquireDuration() time.Duration {
	return time.Duration(s.waitNanos)
}

// EmptyAcquireCount returns how many acquisitions had to wait for a free connection.
func (s poolStat) EmptyAcquireCount() int64 {
	return s.empties
}

// CanceledAcquireCount returns how many acquisitions ended with the caller's context.
func (s poolStat) CanceledAcquireCount() int64 {
	return s.canceled
}
