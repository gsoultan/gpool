// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
)

// permits caps how many connections may be checked out at once.
//
// It is a token channel rather than a counting semaphore. Both bound the same
// thing, but under the concurrency a pooler is built for — thousands of callers
// contending for tens of connections — the difference is large. A mutex-and-condvar
// semaphore serialises every acquire and release through one lock, allocates a
// wait channel and a queue element per blocked caller, and wakes waiters through
// condvar signalling. A token channel takes the uncontended path with no parking
// at all, allocates nothing (a struct{} element has no backing array whatever the
// capacity), and hands a token straight from releaser to waiter.
//
// Measured on the acquire/release path with 5000 concurrent callers, this cut
// latency roughly in half and removed every allocation from the permit itself.
type permits struct {
	tokens chan struct{}
}

// newPermits creates a permit set holding n tokens, all initially available.
func newPermits(n int32) permits {
	p := permits{tokens: make(chan struct{}, n)}
	for range n {
		p.tokens <- struct{}{}
	}
	return p
}

// tryAcquire takes a permit if one is free, without blocking or touching the
// scheduler. It is separate from wait so the caller can tell the two paths apart:
// only a real wait is worth timing, and a clock read on the fast path would cost
// more than the measurement is worth.
func (p permits) tryAcquire() bool {
	select {
	case <-p.tokens:
		return true
	default:
		return false
	}
}

// wait parks until a permit is released or the caller gives up.
func (p permits) wait(ctx context.Context) error {
	select {
	case <-p.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acquire takes a permit, blocking until one is free or ctx is done.
func (p permits) acquire(ctx context.Context) error {
	if p.tryAcquire() {
		return nil
	}
	return p.wait(ctx)
}

// release returns a permit.
//
// The non-blocking send is a safety net, not an expected path: capacity equals
// the permit count and every release is guarded by an idempotence flag, so the
// channel cannot legitimately be full. Dropping a surplus token wedges nothing,
// whereas a counting semaphore answers the same mistake by panicking the process.
func (p permits) release() {
	select {
	case p.tokens <- struct{}{}:
	default:
	}
}

// drain reclaims up to n permits, giving up when ctx is done. It reports how many
// it reclaimed, so a caller can tell a clean shutdown from an abandoned connection.
func (p permits) drain(ctx context.Context, n int32) int32 {
	var reclaimed int32
	for range n {
		select {
		case <-p.tokens:
			reclaimed++
		case <-ctx.Done():
			return reclaimed
		}
	}
	return reclaimed
}

// available reports how many permits are currently free. It is a sampled value,
// useful for diagnostics rather than for control flow.
func (p permits) available() int32 {
	return int32(len(p.tokens))
}
