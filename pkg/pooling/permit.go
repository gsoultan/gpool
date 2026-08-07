// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"context"
	"sync/atomic"
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
//
// The set is resizable. The channel is allocated once at the hard ceiling and is
// never replaced, because swapping a channel out from under parked waiters would
// strand them. Sizing it generously is free for the same reason the token channel
// was chosen: a struct{} element has no backing array, so make(chan struct{}, n)
// costs the same at n = 10000 as at n = 1.
type permits struct {
	tokens chan struct{}

	// limit is the current effective ceiling, within [0, cap(tokens)].
	limit atomic.Int32

	// debt is a shrink that could not be paid immediately. Lowering the limit
	// while connections are checked out cannot reclaim their tokens, and waiting
	// for them would make a resize block on user code — so the shortfall is
	// recorded here and the next releases swallow their tokens instead of
	// returning them.
	debt atomic.Int32
}

// newPermits creates a permit set holding n tokens and able to grow to ceiling.
func newPermits(n, ceiling int32) *permits {
	if ceiling < n {
		ceiling = n
	}

	p := &permits{tokens: make(chan struct{}, ceiling)}
	p.limit.Store(n)
	for range n {
		p.tokens <- struct{}{}
	}
	return p
}

// resize moves the effective ceiling to n and returns the previous value.
//
// Growing hands out the difference at once, cancelling any outstanding debt
// first. Shrinking reclaims whatever is free right now and records the remainder
// as debt, so the call never blocks waiting for a checked-out connection.
func (p *permits) resize(n int32) int32 {
	if n < 0 {
		n = 0
	}
	if ceiling := int32(cap(p.tokens)); n > ceiling {
		n = ceiling
	}

	old := p.limit.Swap(n)
	switch delta := n - old; {
	case delta > 0:
		for range delta {
			if p.repayDebt() {
				continue
			}
			select {
			case p.tokens <- struct{}{}:
			default:
				// At the hard ceiling; there is nothing further to hand out.
			}
		}
	case delta < 0:
		for range -delta {
			select {
			case <-p.tokens:
			default:
				p.debt.Add(1)
			}
		}
	}
	return old
}

// repayDebt cancels one unit of pending shrink, reporting whether it did.
func (p *permits) repayDebt() bool {
	for {
		d := p.debt.Load()
		if d <= 0 {
			return false
		}
		if p.debt.CompareAndSwap(d, d-1) {
			return true
		}
	}
}

// limitValue reports the current effective ceiling.
func (p *permits) limitValue() int32 {
	return p.limit.Load()
}

// tryAcquire takes a permit if one is free, without blocking or touching the
// scheduler. It is separate from wait so the caller can tell the two paths apart:
// only a real wait is worth timing, and a clock read on the fast path would cost
// more than the measurement is worth.
func (p *permits) tryAcquire() bool {
	select {
	case <-p.tokens:
		return true
	default:
		return false
	}
}

// wait parks until a permit is released or the caller gives up.
func (p *permits) wait(ctx context.Context) error {
	select {
	case <-p.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acquire takes a permit, blocking until one is free or ctx is done.
func (p *permits) acquire(ctx context.Context) error {
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
func (p *permits) release() {
	// A pending shrink is paid here: the token is swallowed rather than returned,
	// which is how a resize that could not reclaim in-use permits eventually takes
	// effect without ever having blocked.
	if p.repayDebt() {
		return
	}

	select {
	case p.tokens <- struct{}{}:
	default:
	}
}

// drain reclaims up to n permits, giving up when ctx is done. It reports how many
// it reclaimed, so a caller can tell a clean shutdown from an abandoned connection.
func (p *permits) drain(ctx context.Context, n int32) int32 {
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
func (p *permits) available() int32 {
	return int32(len(p.tokens))
}
