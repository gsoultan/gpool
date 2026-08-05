// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"sync/atomic"
	"time"
)

// clockResolution is how often the cached time is refreshed. It bounds how stale
// a reading can be, and 100ms is far finer than anything measured against it:
// the shortest bound a connection is judged by is its idle time, in minutes.
const clockResolution = 100 * time.Millisecond

// coarseClock caches the current time so the acquire and release paths do not
// have to read the system clock.
//
// Three reads per acquire/release cycle measured at roughly 200ns of a 740ns
// cycle under 5000 concurrent callers — more than a quarter of the whole path,
// for a value that only ever feeds comparisons against multi-second bounds. An
// atomic load of a line that is written ten times a second is nearly free by
// comparison, because every core keeps it in shared state.
//
// The zero value is not usable: it reads as the zero time until the first tick,
// which would make every connection look older than any bound. Construct it with
// newCoarseClock, which seeds it.
type coarseClock struct {
	nanos atomic.Int64
}

// newCoarseClock returns a clock already seeded with the current time.
func newCoarseClock() *coarseClock {
	c := &coarseClock{}
	c.update()
	return c
}

// now returns the cached time, at most clockResolution stale.
func (c *coarseClock) now() time.Time {
	return time.Unix(0, c.nanos.Load())
}

// update refreshes the cached time. Only the pool's maintainer calls it.
func (c *coarseClock) update() {
	c.nanos.Store(time.Now().UnixNano())
}
