// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPermitsBoundConcurrency(t *testing.T) {
	t.Parallel()

	p := newPermits(2)

	if got := p.available(); got != 2 {
		t.Fatalf("available() = %d, want 2", got)
	}
	for range 2 {
		if err := p.acquire(t.Context()); err != nil {
			t.Fatalf("acquire() = %v", err)
		}
	}
	if got := p.available(); got != 0 {
		t.Fatalf("available() = %d, want 0", got)
	}

	exhausted, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := p.acquire(exhausted); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire() beyond capacity = %v, want a deadline", err)
	}

	p.release()
	if err := p.acquire(t.Context()); err != nil {
		t.Fatalf("acquire() after release = %v", err)
	}
}

func TestPermitsAcquireHonoursCancellation(t *testing.T) {
	t.Parallel()

	p := newPermits(1)
	if err := p.acquire(t.Context()); err != nil {
		t.Fatalf("acquire() = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := p.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("acquire() = %v, want context.Canceled", err)
	}
}

// A counting semaphore answers an over-release by panicking the whole process.
// A surplus token is a bookkeeping mistake, not grounds for taking the program down.
func TestPermitsSurvivesOverRelease(t *testing.T) {
	t.Parallel()

	p := newPermits(1)
	p.release()
	p.release()
	p.release()

	if got := p.available(); got != 1 {
		t.Fatalf("available() = %d, want 1 - capacity must still bound the pool", got)
	}
	if err := p.acquire(t.Context()); err != nil {
		t.Fatalf("acquire() = %v", err)
	}

	exhausted, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := p.acquire(exhausted); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("over-release inflated the pool's capacity")
	}
}

func TestPermitsDrain(t *testing.T) {
	t.Parallel()

	p := newPermits(4)
	if err := p.acquire(t.Context()); err != nil {
		t.Fatalf("acquire() = %v", err)
	}

	// Three are free; the fourth is held, so draining stops at the deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	if got := p.drain(ctx, 4); got != 3 {
		t.Fatalf("drain() = %d, want 3 reclaimed with one still held", got)
	}
}

// The bound must hold exactly under contention: never more holders than capacity,
// and never a lost permit that shrinks the pool over time.
func TestPermitsHoldTheBoundUnderContention(t *testing.T) {
	t.Parallel()

	const capacity = 8
	const workers = 200

	p := newPermits(capacity)

	var held, peak atomic.Int32
	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			for range 50 {
				if err := p.acquire(context.Background()); err != nil {
					return
				}

				current := held.Add(1)
				for {
					high := peak.Load()
					if current <= high || peak.CompareAndSwap(high, current) {
						break
					}
				}
				held.Add(-1)
				p.release()
			}
		})
	}
	wg.Wait()

	if got := peak.Load(); got > capacity {
		t.Errorf("peak concurrent holders = %d, want at most %d", got, capacity)
	}
	if got := held.Load(); got != 0 {
		t.Errorf("%d permits still marked held after every worker finished", got)
	}
	if got := p.available(); got != capacity {
		t.Errorf("available() = %d, want %d - permits leaked", got, capacity)
	}
}

// The uncontended path is what most acquisitions take, and it must not allocate.
// Not parallel: AllocsPerRun needs the process to itself to get a stable count.
func TestPermitsFastPathDoesNotAllocate(t *testing.T) {
	p := newPermits(1)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(1000, func() {
		if err := p.acquire(ctx); err != nil {
			t.Fatal(err)
		}
		p.release()
	})

	if allocs != 0 {
		t.Fatalf("acquire/release allocated %.1f times per operation, want 0", allocs)
	}
}
