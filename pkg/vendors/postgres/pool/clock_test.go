// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"testing"
	"time"
)

// An unseeded clock reads as the zero time, which would make every connection
// look older than any bound and have the pool discard connections on first use.
func TestCoarseClockIsSeeded(t *testing.T) {
	t.Parallel()

	clock := newCoarseClock()

	if drift := time.Since(clock.now()); drift > time.Second || drift < -time.Second {
		t.Fatalf("a new clock reads %v away from now; it was not seeded", drift)
	}
}

func TestCoarseClockUpdates(t *testing.T) {
	t.Parallel()

	clock := newCoarseClock()
	first := clock.now()

	time.Sleep(2 * clockResolution)
	if got := clock.now(); !got.Equal(first) {
		t.Fatal("the clock moved without being updated; only the maintainer may advance it")
	}

	clock.update()
	if !clock.now().After(first) {
		t.Fatal("update() did not advance the clock")
	}
}

// The pool's maintainer must keep the clock fresh, otherwise expiry judgements
// drift further from reality the longer the pool runs.
func TestPoolKeepsItsClockFresh(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 2})
	first := p.clock.now()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.clock.now().After(first) {
			if drift := time.Since(p.clock.now()); drift > time.Second {
				t.Fatalf("the clock is %v stale, want under a second", drift)
			}
			return
		}
		time.Sleep(clockResolution / 2)
	}
	t.Fatal("the pool's clock never advanced; the maintainer is not ticking it")
}

// Expiry still has to be judged correctly, just against a cached reading.
func TestPoolExpiryUsesTheCachedClock(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 2, MaxConnLifetime: time.Minute})

	fresh := &idleConn{createdAt: p.clock.now(), idleSince: p.clock.now()}
	if fresh.expired(p.clock.now(), p.config.MaxConnLifetime, 0) {
		t.Error("a connection created now should not be expired")
	}

	stale := &idleConn{createdAt: p.clock.now().Add(-2 * time.Minute), idleSince: p.clock.now()}
	if !stale.expired(p.clock.now(), p.config.MaxConnLifetime, 0) {
		t.Error("a connection older than MaxConnLifetime should be expired")
	}
}
