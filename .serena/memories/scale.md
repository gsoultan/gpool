# Scale and footprint

Both wins here came from profiles, not intuition. Neither was guessable.

## Where the CPU went

At 5000 concurrent callers the acquire path was **62% `pthread_cond_wait`/`signal`**
and 3-4 allocs/op. Cause: `golang.org/x/sync/semaphore.Weighted` serialises every
acquire *and* release through one mutex, and allocates a wait channel plus a queue
element per blocked caller.

Replaced by `permits` (`permit.go`), a `chan struct{}` token set:

- Uncontended acquire is a non-blocking receive — no parking, no lock convoy.
- Zero allocation: a `struct{}` element has no backing array at any capacity, so
  `make(chan struct{}, 5000)` costs the same as capacity 1.
- Release hands a token straight to a waiter instead of condvar broadcast.
- Over-release drops a surplus token via the `default` branch instead of panicking
  the process, which is what `semaphore.Release` does.

Result: ~1134 -> ~599 ns/op, 4 -> 1 allocs/op. Mutex time 23% -> 3%. Remaining
`cond_wait` is inherent queueing (5000 callers, tens of permits), not contention.

Dropping the semaphore also removed `golang.org/x/sync` as a dependency.

## Where the memory went

**57% of the pool's heap was `stmtcache.NewLRUCache`.** pgx keeps two per-connection
caches (statement + description), defaults both to 512, and `NewLRUCache` does
`make(map[string]*list.Element, cap)` — preallocated whether or not a statement is
ever prepared. ~74 KiB per connection before any work.

`Config.StatementCacheCapacity` / `DescriptionCacheCapacity` now bound them,
defaulting to 64. Measured KiB/connection: disabled 25.8, cap16 26.1, **cap64 28.5**,
cap128 36.2, cap512 71.2. 64 sits within 10% of the floor while still caching a
typical service's query set.

`DisableCache` (-1) turns a cache off. A disabled statement cache also forces
`QueryExecModeExec`, same as `ResetQuery` does — the default mode has nowhere to
cache into.

## Invariants this produced

- Anything per-connection is multiplied by `MaxConns`; anything per-event by
  throughput. A default that suits one connection can be wrong for a pool.
- Goroutine cost is one, total — the maintainer. Not per connection, not per caller.
  `TestGoroutineCostAtScale` enforces this and that `Close` reclaims it.
- "5000 connections" has two readings. 5000 *callers* over a bounded pool is the
  normal shape and costs `MaxConns * 28.5 KiB` (~3 MiB at 100 conns). 5000 *backend*
  connections is ~139 MiB here, and PostgreSQL's own per-backend memory binds first.

## Benchmark hygiene

- Match capacity on both sides of a comparison. gpool read 9% slower than pgxpool
  only because its benchmark used `MaxConns: 10` against pgxpool's default
  `max(4, GOMAXPROCS)`; the gap was queueing. Publishing it would have been a false
  claim about another library.
- Pool-mechanics benchmarks need ~200k iterations before allocs/op settles; below
  that, warm-up allocations dominate and report ~4 instead of 1.
