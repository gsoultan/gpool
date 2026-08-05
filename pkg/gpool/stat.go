// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// Stat is a pool's full statistics surface, composed from a point-in-time view of
// occupancy and cumulative acquisition counters. Consumers that only graph
// utilisation can depend on Occupancy alone; alerting on saturation needs Acquisition.
type Stat interface {
	Occupancy
	Acquisition
}
