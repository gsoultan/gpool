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

## [0.3.0] - 2026-08-06

> **Upgrading from v0.2.0.** Two breaking changes, both in `pkg/gpool/cdc`, and
> both mechanical: `Event.LSN uint64` became `Event.Position` (a `cdc.Position`
> string), and `cdc.ReplicationManager` left `cdc.Subscriber` — reach it with a
> type assertion. Nothing in `pkg/gpool`, `pkg/pooling` or `pkg/sqldriver`
> changed, so a consumer that does not use CDC can upgrade without edits.


### Added

- **MySQL and MariaDB change data capture**, over the binary log, in
  `vendors/mysql/cdc`. GTID positions where the source has GTIDs enabled and
  binlog file offsets where it does not; column names from the log itself under
  `binlog_row_metadata=FULL` and from `information_schema` otherwise, with a
  count mismatch reported rather than guessed at.

  A module of its own, nested inside the pool vendor, because the binlog reader
  pulls in the TiDB parser and thirty-odd other modules — the pool vendor stays
  at 11 while the CDC one resolves 47.

- **`Stream.SubscribeFrom`**, which resumes from a position the consumer
  recorded. This is what makes CDC possible at all against a source that keeps no
  per-consumer state: PostgreSQL remembers where a subscriber got to, MySQL
  remembers nothing, and without a caller-supplied position `Subscribe` on MySQL
  can only ever mean "from now on".

- **`examples/gpoolproxy`, a PostgreSQL pooler built on the engine.** An in-process
  pool cannot bound connections across applications — forty services holding
  twenty-five each still open a thousand, and none of them can see the others.
  Closing that gap needs a process rather than a library, so it is an example in
  its own module rather than anything a consumer of gpool downloads.

  It is also the strongest evidence that `pkg/pooling` is not shaped around pgx:
  the proxy drives `Core` with a connection type that is not a database driver at
  all, just a socket and a transaction status, and the whole vendor half is five
  methods. Twelve independent client pools with five connections each are held to
  four PostgreSQL backends.

  Measured against PgBouncer 1.25.2 in transaction mode, both pooling 16 server
  connections and accepting 3,000 clients, medians of three interleaved runs:
  PgBouncer is ahead to about thirty clients and behind past it — 31,046 against
  58,532 queries per second at 128 clients, 26,973 against 50,742 at 3,000, where
  a direct connection cannot reach at all. The cause is structural rather than
  incidental: PgBouncer runs one thread, so one core is its ceiling on any
  hardware, while the proxy was measured at 140% of a core under the same load.

  PgBouncer is otherwise the more efficient of the two by a wide margin — roughly
  half the CPU per query (14.8 µs against 27.6 µs) and a sixth of the memory per
  client (6 KiB against 37 KiB). A memory-bound deployment should prefer it on
  these numbers.

- **Four new databases**, each as its own Go module so a consumer downloads only
  the drivers it uses. The core resolves 20 modules; ClickHouse alone brings 89.
  - `vendors/mysql` — MySQL and MariaDB. MariaDB speaks the MySQL wire protocol,
    so one implementation registers under both names.
  - `vendors/mssql` — SQL Server.
  - `vendors/clickhouse` — ClickHouse. Transactions are not generally available
    there; the server's refusal is reported rather than hidden, and the connection
    stays usable afterwards.
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

- **Values from drivers outside `database/sql`'s documented type set were
  rejected.** `driver.Value` is an `any`, and a driver with a richer type system
  returns its own native types: ClickHouse hands back `uint8` for a boolean-ish
  column and `uint64` for an unsigned integer, so even `SELECT 1` failed to scan.
  Numeric conversion now falls back to reflection over the kind, as
  `database/sql` does, and unsigned destinations keep the full range rather than
  being truncated through `int64`.
- **`MaxConns` did not bound the number of connections, only concurrent
  checkouts.** Holding a permit before dialling is not sufficient: a permit
  released by one caller creates no ordering with respect to a *different*
  caller's freshly pooled connection, so a caller could hold a permit, fail to see
  an idle connection that already existed, and dial a surplus one. Observed as a
  pool with `MaxConns: 4` holding five connections with one sitting idle, unseen.
  Dialling now reserves a slot in the total count, making the ceiling exact.
  Present in v0.1.0.

### Changed

- **`Event.LSN uint64` is now `Event.Position`**, an opaque vendor-defined marker.
  A WAL offset is the only change log position that fits in a number: MySQL's is
  a set of UUID ranges or a file and offset, MongoDB's is a token, SQL Server's is
  sixteen bytes. PostgreSQL renders its LSN in the same `0/1A2B3C4D` notation
  psql uses, so a recorded position can still be compared against the server by
  hand.

  The contract is also now stated: resuming from a position starts at or before
  the change it came from, never after it. A resumed stream may repeat but does
  not skip. This was always true — the PostgreSQL integration test shows the last
  event replaying — it just was not written down.

- **`cdc.ReplicationManager` is no longer part of `cdc.Subscriber`.** Slots and
  publications are PostgreSQL's model, and a vendor without them would have had to
  implement four methods that only return errors, turning a compile-time mismatch
  into a runtime one. It is an optional capability now, reached by type assertion
  like `BulkCopier` and `Notifier`. PostgreSQL still implements it, with a
  compile-time proof so the capability cannot be dropped silently.

- **`SubscribeFrom` on PostgreSQL refuses a position behind the slot's confirmed
  position**, with `ErrPositionBehindSlot`. The server accepts such a request and
  silently begins at `confirmed_flush_lsn` instead, so a consumer resuming from
  older bookkeeping would receive a stream missing everything in between, with
  nothing to distinguish it from a complete one.

- **`NewSubscriber` distinguishes a vendor with no CDC from one that was never
  imported**, with `ErrNoCDCSupport`. Advising an import that cannot help sends
  the caller after a bug that is not there.

- **PostgreSQL now runs on the shared pooling engine.** It had its own copy of
  capacity, sharding, the reaper, the clock and the statistics — around 1,270
  lines that had already required the same `MaxConns` ceiling bug to be fixed
  twice. What remains in the vendor is what is genuinely PostgreSQL-specific:
  dialling pgx, judging a connection without I/O, and cleaning one up.

  Retargeting is performance-neutral, but only after two costs the benchmarks
  exposed. The engine was building a deadline context on every release to bound
  cleanup — four allocations and a runtime timer, which took the acquire path from
  198 to 889 ns/op; `Driver.NeedsCleanup` now lets a connection with nothing on it
  skip it. And `Handle` is returned by value so a vendor stores it inline and pays
  one allocation for the pair rather than two. Final: 200-217 ns/op against 198.3
  before, one allocation either way.
- **PostgreSQL is no longer the only vendor**, which resolves the corresponding
  entry under v0.1.0's known limitations. The abstraction has now been proven
  against three further engines, including one — ClickHouse — that is not
  transactional.
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

[Unreleased]: https://github.com/gsoultan/gpool/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/gsoultan/gpool/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/gsoultan/gpool/releases/tag/v0.1.0
