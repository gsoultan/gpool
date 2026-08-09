// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// Lifecycle reports why a pool has discarded connections, as cumulative,
// monotonically increasing counters.
//
// Occupancy says how full a pool is and Acquisition says how hard callers are
// competing for it, but neither can say why. A pool whose EmptyAcquireCount is
// climbing may be too small, or it may be dialling replacements as fast as its
// connections die — and those want opposite responses. Separating the reasons is
// the difference between raising MaxConns and looking at the network.
//
// Reached by type assertion on the value Pool.Stat returns, like Resizable on
// the pool itself, so an engine that cannot account for its connections this way
// is not obliged to pretend:
//
//	if lifecycle, ok := pool.Stat().(gpool.Lifecycle); ok {
//		report(lifecycle.UnhealthyConnections())
//	}
//
// The three counts are disjoint, and together they are every connection the pool
// has closed while running. A connection closed because the pool itself was
// closing is in none of them: that is shutdown, not churn.
type Lifecycle interface {
	// ExpiredConnections returns connections retired for reaching
	// MaxConnLifetime or MaxConnIdleTime.
	//
	// This is the healthy one. It rising steadily is the pool doing what those
	// bounds exist for; it staying at zero on a long-lived pool means neither
	// bound is reached, which is worth knowing before a failover proves it.
	ExpiredConnections() int64

	// UnhealthyConnections returns connections discarded because they were dead,
	// never became ready, or failed the reset that returns them to a clean state.
	//
	// This is the one to alert on. In a healthy pool it is flat; against a
	// database that is failing over, restarting, or dropping connections it
	// tracks the damage, and each one of these is a caller that saw an error.
	UnhealthyConnections() int64

	// EvictedConnections returns connections closed to obey a lowered ceiling or
	// an explicit EvictIdle.
	//
	// Nothing is wrong when this moves; something asked for it. It is here so
	// that deliberate eviction cannot be mistaken for either of the others.
	EvictedConnections() int64
}
