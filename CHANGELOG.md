# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning policy

While the major version is `0`, the API may change in a minor release. `v0.x` means
the library is in production use but its surface is not yet frozen — pin an exact
version. Breaking changes are always listed under **Changed** or **Removed** with the
reason and the migration.

`v1.0.0` will be tagged once the interfaces in `pkg/gpool` and `pkg/gpool/cdc` have
gone a release cycle without a breaking change, and a second vendor exists to prove
the abstraction holds.

## [Unreleased]

### Added

- **MySQL and MariaDB**, as the separate module
  `github.com/gsoultan/gpool/vendors/mysql`. MariaDB speaks the MySQL wire
  protocol, so one implementation registers under both names. A consumer using
  only PostgreSQL never downloads the driver.
- **`pkg/sqldriver`, shared pooling for any `database/sql` driver.** Connections
  are pooled at the `driver.Conn` level rather than by wrapping `*sql.DB`;
  wrapping would mean `database/sql` does the pooling and none of gpool's
  guarantees would apply. Depends only on the standard library, so a vendor module
  adds its own driver and nothing else to a consumer's graph. This is what makes
  each `database/sql` vendor about a hundred lines.
- **`pkg/pooling`, the vendor-agnostic pooling engine.** Capacity, lock-striped
  idle buckets, the reaper, lifecycle, and statistics now live in one generic
  `Core[C]`, parameterised by a `Driver[C]` adapter. A vendor supplies only what is
  genuinely vendor-specific: how to dial, how to tell a connection is dead, and how
  to return one to a clean state. Generic over the driver's own connection type
  rather than an interface, so nothing on the acquire path pays for dynamic
  dispatch.

### Fixed

- **`MaxConns` did not bound the number of connections, only concurrent
  checkouts.** Holding a permit before dialling is not sufficient: a permit
  released by one caller creates no ordering with respect to a *different*
  caller's freshly pooled connection, so a caller could hold a permit, fail to see
  an idle connection that already existed, and dial a surplus one. Observed as a
  pool with `MaxConns: 4` holding five connections with one sitting idle, unseen.
  Dialling now reserves a slot in the total count, making the ceiling exact.
  Present in v0.1.0.

### Changed

- **Cut the acquire path by a third** — 293.9 to 198.3 ns/op — by caching the
  clock. Three `time.Now()` calls per acquire/release cycle measured at roughly a
  quarter of the whole path, for values only ever compared against multi-second
  bounds. `MaxConnIdleTime` and `MaxConnLifetime` are now judged against a reading
  at most 100ms stale, so sub-second bounds are imprecise.

## [0.1.0] - 2026-08-05

First tagged release. The library is usable and tested against PostgreSQL 17, but
the API is not frozen — see the versioning policy above.

### Added

- **Connection pool** for PostgreSQL over `pgx/v5`, with lock-striped idle buckets,
  a background reaper honouring `MinConns` / `MaxConnIdleTime` / `MaxConnLifetime`,
  and a bounded, idempotent `Close`.
- **Change Data Capture** over logical replication (`pgoutput`), with an iterator
  API, at-least-once delivery, dynamic table management, and explicit slot and
  publication lifecycle control.
- **Multiple databases**: `Engine` holds a named registry of pools and subscribers.
  Nothing is shared between them, so a saturated or failing database cannot starve
  another. `AddPool`, `Pool(name)`, `RemovePool`, `Pools`.
- **Optional capabilities**, exposed as separate interfaces reached by type
  assertion so `Pool` and `Conn` stay small:
  - `BulkCopier` — COPY-protocol bulk load, on `Pool` and `Conn`.
  - `Batcher` — pipelined statements, on `Conn`.
  - `Notifier` — LISTEN/NOTIFY, on `Conn`.
- **Statistics** for pool sizing: `Stat` composes `Occupancy` (total, idle, active,
  max) and `Acquisition` (acquire count, wait duration, empty and cancelled acquire
  counts). Lock-free; only waits are timed.
- **Configuration** for connection lifetime, health-check period, connect and
  cleanup timeouts, per-connection cache capacity, and `BeforeConnect`/`AfterConnect`
  hooks.
- **Tests**: 136 tests and benchmarks — unit tests needing no database, integration
  tests against a real server, soak tests, and comparative benchmarks against
  `pgxpool`, `database/sql` and PgBouncer.

### Fixed

Findings from a full audit of the pre-release code. Each is covered by a regression
test whose comment records the original failure mode.

- **Panics on the two most common Go idioms.** `defer tx.Rollback()` alongside
  `tx.Commit()`, and `defer rows.Close()` alongside `range rows.All()`, both
  dereferenced a nil driver handle. Every teardown method is now idempotent.
- **Object-pool aliasing.** Objects whose lifetime user code controls were recycled
  through `sync.Pool`, so a second release could hand a live resource to another
  goroutine. They are now allocated per use.
- **Connections returned mid-transaction.** A caller that released without
  committing leaked its transaction — locks, snapshot, and, after a failed
  statement, a session rejecting everything with `SQLSTATE 25P02`. The release gate
  now rolls back rather than leaking onward, and clears an active `LISTEN`.
- **A released-but-unscanned row left the connection `conn busy`.** `Row` now wraps
  the full result set so it can be closed without being read.
- **`ResetQuery: "DISCARD ALL"` broke the pool** by invalidating pgx's statement
  cache, failing with `SQLSTATE 26000`. It now selects a compatible execution mode.
- **CDC ran table management on the streaming connection**, corrupting the
  replication protocol. Control and replication connections are now separate.
- **CDC discarded its backlog on reconnect** by starting from the server's WAL head
  instead of the slot's confirmed position.
- **An idle publication pinned WAL until the primary's disk filled**, because the
  confirmed position only advanced on a row change. Idle keepalives now carry it
  forward.
- **A slow consumer killed the replication stream** by starving keepalives.
- **Event maps were recycled under the consumer**, silently emptying any event kept
  past one loop iteration.
- **Unchanged TOASTed columns were reported as SQL NULL**, which would blank them on
  replay. They are now omitted.
- **Schema-qualified table names were quoted as one identifier**, addressing a table
  literally named `public.users`.
- **Catalog errors were classified by English message text**, which fails on a
  server with a non-English `lc_messages`. Now by SQLSTATE.
- **`MaxConns: 0` blocked forever** with no error. Config is validated and defaulted.
- **Dead, expired, and reset-failed connections were recycled**, and `Close` neither
  stopped new acquisitions nor drained in flight.

### Changed

- **Halved acquisition latency and removed its allocations** by replacing the
  counting semaphore with a token channel: ~1134 → ~574 ns/op and 4 → 1 allocs/op
  at 5000 concurrent callers, with mutex time falling from 23% to 3%.
- **Cut per-connection memory by roughly 60%** — ~71 → ~28 KiB — by bounding the two
  per-connection caches pgx preallocates at 512. They were 57% of the pool's heap.
- **Removed `golang.org/x/sync`.** The only dependencies are `pgx/v5` and
  `pglogrepl`.
- **Moved the public API out of `internal/`.** Nothing outside the module could
  import the vendor packages or their `Config` types, so the documented usage could
  not compile and vendor self-registration could never run.
- **Module path is now `github.com/gsoultan/gpool`**, matching the repository.
- **`go.sum` is no longer git-ignored**, which had left the repository unbuildable
  from a fresh clone.

### Removed

- **The CLI proxy, the YAML configuration, the logger, and the Prometheus
  exporter.** gpool is a library: no binary, no config file, no process-global
  state. The proxy was also non-functional — `pgproto3.Backend.Send` only buffers
  and `Flush` was never called, so no byte ever reached a client; it additionally
  had no authentication.

### Known limitations

- CDC values are delivered as `string`, exactly as `pgoutput` transmits them. The
  replication stream does not carry the destination Go type, so decoding is left to
  the consumer.
- The vendor factory takes `config any`, so a mismatched config type is a runtime
  error rather than a compile error. This mirrors `database/sql`.
- PostgreSQL is the only vendor. The abstraction has not yet been proven against a
  second one.

[Unreleased]: https://github.com/gsoultan/gpool/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/gsoultan/gpool/releases/tag/v0.1.0
