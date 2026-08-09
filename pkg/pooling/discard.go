// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

// discard names why a connection was closed, so the pool can report churn a
// consumer can act on rather than a single number that mixes causes.
//
// Every call to destroy carries one, and every one is counted, so the four
// counters add up to every connection the pool has ever closed. A reason that
// went unrecorded would show as connections vanishing from the accounting.
type discard uint8

const (
	// discardExpired is MaxConnLifetime or MaxConnIdleTime being reached, which
	// is the pool working rather than failing.
	discardExpired discard = iota

	// discardUnhealthy is dead, never ready, or failed its reset — the reason
	// worth alerting on, because each one is a caller that saw an error.
	discardUnhealthy

	// discardEvicted is a lowered ceiling or an explicit EvictIdle: deliberate,
	// and not to be confused with either of the others.
	discardEvicted

	// discardClosed is the pool shutting down. Counted so the accounting stays
	// complete, and reported to nobody, because it is not churn.
	discardClosed

	discardCount
)
