// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

// Resizable is an optional Pool capability: changing capacity, and discarding
// idle connections, while the pool is running.
//
// It is separate from Pool and reached by type assertion, for the same reason
// BulkCopier and Notifier are: most callers never need it, and folding it into
// Pool would push that interface past the point where implementing it is
// reasonable.
//
//	if r, ok := pool.(gpool.Resizable); ok {
//		err := r.SetMaxConns(64)
//	}
//
// Both operations exist because a pool cannot infer either one. Only the caller
// knows that load has shifted, or that the connections it holds are no longer
// the right ones — a backend that changed role, a rotated credential, a failover
// that kept the address.
type Resizable interface {
	// MaxConns returns the ceiling currently in force, which may differ from the
	// value the pool was constructed with.
	MaxConns() int32

	// SetMaxConns changes how many connections may be handed out at once.
	//
	// It must not block. Raising the ceiling takes effect at once; lowering it
	// cannot reclaim a connection a caller is still holding, so implementations
	// apply the remainder as those connections come back rather than waiting.
	//
	// An implementation may refuse a value outside the bounds it was configured
	// with — growing without limit is how a pool exhausts the database, so the
	// headroom is declared at construction rather than assumed.
	SetMaxConns(n int32) error

	// EvictIdle closes every idle connection and reports how many it closed.
	// Connections currently checked out are left alone and judged on release.
	EvictIdle() int
}
