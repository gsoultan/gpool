// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// Occupancy is a point-in-time view of how much of a pool is in use.
//
// The three counts are sampled independently, so they are consistent enough to
// graph but should not be treated as a simultaneous snapshot.
type Occupancy interface {
	// TotalConnections returns every connection the pool currently owns.
	TotalConnections() int32
	// IdleConnections returns the connections sitting in the pool ready for reuse.
	IdleConnections() int32
	// ActiveConnections returns the connections currently checked out.
	ActiveConnections() int32
	// MaxConnections returns the configured ceiling, so a dashboard can show
	// utilisation without being told the configuration separately.
	MaxConnections() int32
}
