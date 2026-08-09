# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning policy

From `v1.0.0` the exported surface of `pkg/gpool`, `pkg/gpool/cdc`, `pkg/pooling`
and `pkg/sqldriver` is stable. A breaking change to any of them means `v2`, with
the module path change Go requires, not a minor release.

What that promise does *not* cover, deliberately:

- **Adding a field to a struct, or a value to `Op`.** `Event` has grown twice and
  will again. Consumers read it; nothing implements it.
- **A new optional capability interface.** `BulkCopier`, `Batcher`, `Notifier`,
  `Resizable`, `Lifecycle` and `ReplicationManager` are reached by type assertion
  precisely so that adding one breaks nobody.
- **Vendor `Config` structs**, which gain fields as their engines do.
- **`examples/`**, which is not part of the library.

Breaking changes are always listed under **Changed** or **Removed** with the
reason and the migration.

## [Unreleased]

## [1.1.0] - 2026-08-09

### Added

- **`gpool.Lifecycle`, so a pool can say why its connections are being
  replaced.** `Occupancy` says how full the pool is and `Acquisition` how hard
  callers are competing for it; neither says what is happening to what they are
  competing for. A climbing `EmptyAcquireCount` has two opposite causes — a pool
  that is too small, and a pool dialling replacements as fast as its connections
  die — and raising `MaxConns` fixes the first while making the second worse.

  Three disjoint counters: `ExpiredConnections` for reaching `MaxConnLifetime`
  or `MaxConnIdleTime`, `UnhealthyConnections` for dead, never ready, or failed
  its reset, and `EvictedConnections` for a lowered ceiling or an explicit
  `EvictIdle`. Together they are every connection closed while running.
  Connections closed by `Close` are in none of them, because shutdown is not
  churn.

  Reached by type assertion on the value `Stat()` returns, like `Resizable` on
  the pool itself, so it is additive under the v1.0 freeze rather than a change
  to an interface consumers may implement. It is counted in `pkg/pooling`, so
  every vendor has it without a line of vendor code.

  `UnhealthyConnections` counts the same events the failure-injection tests
  measure from the caller's side: four backends terminated cost four failed
  queries and are counted as four unhealthy connections.

### Fixed

- **`examples/gpoolproxy` let a client decide how much memory it spent, on both
  sides of the proxy.** Two maps keyed by client-supplied statement names grew
  without limit: a session's, holding one `Parse` message per name its client
  invented, and a pooled backend's, holding one per name *any* client had ever
  used on it. Nothing deallocates a prepared statement when a client goes away,
  and a backend outlives every client that touches it, so neither ever shrank.
  The second one is PostgreSQL's memory rather than the proxy's.

  Both are now bounded by `Config.MaxPreparedStatements`, discarding the least
  recently used at the limit. A backend's eviction also closes the statement on
  the server, so the proxy's idea of what exists and the server's stay in step,
  and the client that prepared it is unaffected — its own set still holds the
  `Parse` and replays it onto the next backend it lands on. Sixty statements
  prepared against a limit of eight leave eight in `pg_prepared_statements`.

  The default is 512 rather than PgBouncer's 200 because the limits interact: a
  client caches statement names and only Binds them afterwards, so a proxy that
  remembers fewer than the client does becomes "prepared statement does not
  exist" the first time that client moves backends. 512 is pgx's own default.

  This was the last item the proxy's README named as a gap against PgBouncer.

- **A client that connected before the proxy had opened a backend was told
  nothing about the server.** `examples/gpoolproxy` replays the server's own
  settings during client startup, captured from a real connection rather than
  invented — so until the pool had opened one there was nothing to say, and the
  first client got an empty set. pgx survives that on the extended protocol and
  refuses the simple protocol without `standard_conforming_strings`; another
  client library is entitled to do worse.

  `Serve` now opens one backend and lets it go before accepting anyone, and a
  session that still finds the set empty asks again rather than proceeding.
  Both are best effort and bounded: a proxy started before its server must still
  start, and a server unreachable then may not be later.

### Changed

- **`.junie/scripts/testdbs.sh` takes its host ports from the environment**
  (`GPOOL_POSTGRES_PORT` and one per engine). They are ports on a developer's
  machine and nothing reserves them; another project's container already holding
  one surfaced as a container that would not bind, several steps from the cause.

## [1.0.1] - 2026-08-08

### Fixed

- **`Resizable` could only ever shrink, on every vendor.** Growth is bounded by
  `MaxConnsLimit`, which the engine defaults to `MaxConns` — and no vendor
  `Config` had the field, so there was no way to declare the headroom. The error
  even said "raise the limit at construction", which was not possible. Every
  vendor config now exposes it, and the tests prove a grown pool really holds the
  extra connections rather than merely returning nil.

- **`database/sql` pools did not implement `Resizable` at all**, so runtime
  capacity control was a PostgreSQL privilege that MySQL, MariaDB, SQL Server and
  ClickHouse silently lacked. It is implemented once in `pkg/sqldriver`, so every
  one of them has it.

Both were found by writing the godoc example for `Resizable`: the example did not
compile, because the field it needed did not exist.

### Added

- **Continuous integration**, on GitHub Actions. Format and vet, unit tests with
  no database, the full integration matrix against all five engines, govulncheck,
  and — on tags only — a build from an empty module against the published version,
  which is the check that has caught three wrong `require` versions.

  The integration job brings the databases up with `.junie/scripts/testdbs.sh`,
  the same script a developer uses, rather than declaring them again in YAML.
  `services:` cannot pass arguments to a container and every engine here needs
  them. It also fails loudly if any engine never became reachable, because a test
  that skips for a missing DSN looks exactly like a test that passed.

  `testdbs.sh` now speaks Docker as well as Apple's `container`, and only forces
  an x86-64 platform for SQL Server when the host is not already x86-64 — CI
  runners are, so it runs natively there.

- **Nine godoc examples**, so pkg.go.dev shows compiling code rather than prose.

- **`errors.Is` coverage for every CDC sentinel.** `ErrPositionExpired`,
  `ErrSchemaMismatch` and `ErrCDCNotEnabled` were documented API with no test
  asserting the sentinel — one matched on the message text, which would have kept
  passing after the sentinel stopped being returned. `ErrSchemaMismatch` is
  exercised by resuming a stream across an `ALTER TABLE`, which is the situation
  it exists for.

### Added

- **Failure-injection tests.** The pool bounds connection lifetime precisely
  because servers go away, and nothing tested what happens when one does: every
  other test ran against a database that stayed up, which left the recovery path
  the least exercised code here.

  Backends are now terminated server-side, which is what a failover looks like
  from the pool's side and needs no control over the server process. Covered on
  the pgx path and on the `database/sql` path that MySQL, SQL Server and
  ClickHouse share, against PostgreSQL, MySQL and MariaDB: every connection
  killed at once, the single-connection case where the pool has nowhere to hide,
  and a database flapping under concurrent load.

  They also turned the recovery behaviour into a stated guarantee rather than an
  assumption. One query fails per connection that died, then the pool is healthy;
  a pool of four costs four failed queries. `TotalConnections` never exceeds
  `MaxConns` through any of it. Calling code should retry once on a connection
  error, and the README now says so.

  For CDC the guarantee is stronger: the stream reports the failure through
  `Err()` — `SQLSTATE 57P01` — rather than hanging, and reconnecting replays from
  the slot, so a change committed during the outage still arrives.

## [1.0.0] - 2026-08-08

The API is frozen. Everything in `pkg/gpool`, `pkg/gpool/cdc`, `pkg/pooling` and
`pkg/sqldriver` is now covered by the versioning policy above.

> **Upgrading from v0.5.0.** Nothing breaks. `Event` gained `Transaction`, which
> no consumer implements.

What the freeze rests on, since a version number is a claim and this one should
be checkable:

- **Three CDC vendors, one of which is not a log tail.** PostgreSQL and MySQL
  both follow a stream; SQL Server polls change tables the server fills on its
  own schedule. `Position`, `SubscribeFrom`, `Transaction` and the at-least-once
  contract carry across all three unchanged. Two vendors agreeing proved less
  than it looked, because the interfaces were designed while looking at exactly
  those two.
- **Five engines and a wire-protocol proxy on one pooling engine.** `pkg/pooling`
  is driven by pgx, by three `database/sql` drivers, and by a PostgreSQL proxy
  whose connection type is a socket and a transaction status rather than a
  database driver at all.
- **Every vendor exercised against a real server**, at the versions recorded in
  the README. Two of them — MariaDB and SQL Server — were documented as supported
  for weeks while never having been run, which is the reason that table now names
  versions rather than ticking boxes.


### Added

- **`Event.Transaction`** groups changes that were committed together, which is
  what a consumer replaying downstream needs in order to apply a batch atomically
  rather than a row at a time. Equal values mean one transaction and nothing else
  about the value is meaningful, so it reuses `Position` rather than inventing a
  type.

  This was deferred at v0.3.0 because PostgreSQL and MySQL disagreed about what
  identifies a transaction and the answer should not have been invented to fill a
  field. With a third vendor the shape was obvious: every source names the commit
  rather than the record — PostgreSQL in the Begin message's final LSN, MySQL in
  the position that only advances at a commit, SQL Server in the `__$start_lsn`
  its rows already share.

### Fixed

- **SQL Server `Subscribe` refused a database whose capture job had not yet
  produced anything**, reporting that CDC was not enabled when it plainly was.
  That is the normal state for the first seconds after enabling capture. An empty
  log's beginning and end are the same place, so it now starts there.

- **The goroutine soak test could fail without a leak.** It read the count after
  three identical samples, which under `-race` can lock onto a plateau that has
  not finished unwinding — reporting growth of six against a real drift of zero.
  It now takes the floor over the sampling window, which unwinding cannot lower
  and a leaked goroutine still raises.

## [0.5.0] - 2026-08-08

> **Upgrading from v0.4.0.** Nothing breaks. `Event` gained a field, which no
> consumer implements, and everything else is additive or internal. This is
> deliberately a quiet release: the versioning policy below asks for a cycle
> without a breaking change before `v1.0.0`, and this is it.

### Frozen for v1.0.0

Two design decisions have been reviewed repeatedly and are now settled, so that
`v1.0.0` does not arrive with them still open:

- **The vendor factory keeps `config any`.** A mismatched config type is a
  runtime error rather than a compile error. Making it generic would push the
  vendor's config type into every signature that touches a pool, for a mistake
  that surfaces on the first line of `main`. `database/sql` made the same
  trade.
- **PostgreSQL CDC keeps delivering values as `string`.** That is exactly how
  `pgoutput` transmits them, and the replication stream does not carry the
  destination Go type — so anything else would be this library guessing. MySQL
  and SQL Server deliver their drivers' native types for the same reason: what
  the protocol gives, unmodified. The difference is documented rather than
  smoothed over.

### Added

- **SQL Server change data capture**, in `vendors/mssql/cdc`. It reads the change
  tables `sys.sp_cdc_enable_table` populates, so `AddTables` is server-side DDL
  rather than a client-side filter and `VerifyTable` can report what the server
  is really capturing.

  It lives inside the pool vendor's module rather than its own, because unlike
  the MySQL binlog reader it needs no dependency the pool does not already have:
  one driver serves both.

  This is the third CDC vendor and the first that does not tail a log. Polling a
  table is a different shape from following a stream, and it is what established
  that `Position` generalises — an opaque marker was the right call, because
  SQL Server's is a ten-byte LSN rendered `0x0000002B00000582001C`, which fits
  neither a WAL offset nor a GTID set.

- **`Event.Timestamp`** carries the source's commit time on every change. Both
  existing vendors already reported it and both were discarding it: pgoutput
  sends it once in the Begin that opens a transaction, and every MySQL binlog
  event header is stamped. Adding a struct field breaks no consumer.

### Fixed

- **`SetMaxConns` published its two ceilings without serialising them.** The
  value `reserveSlot` checks before dialling and the permit set that bounds
  checkouts were separate writes, and `take()` reasons across both — a caller
  holding a permit implies room below the ceiling, which is why it yields and
  retries rather than dialling past it. Two callers resizing at once could leave
  the pair disagreeing permanently, at which point surplus permit holders spin
  in that retry loop against a ceiling that never admits them.

  The window is two instructions wide, and five thousand paired-resize trials
  never hit it; the defect was found by reading the invariant `take()` documents
  and checking whether anything enforced it.

## [0.4.0] - 2026-08-07

> **Upgrading from v0.3.0.** One breaking change: `gpool.Stat` gained
> `WaitingAcquires() int32`. Anything that *consumes* a `Stat` is unaffected; only
> a type implementing the interface itself needs the extra method. If you have one,
> return the number of callers currently blocked in Acquire, or 0 if you do not
> track it.

### Fixed

- **`Stat.ActiveConnections()` could read high.** It was derived as
  `total - idle`, two counters sampled independently, so a connection created by
  the background warm-up but not yet visible in a shard counted as active. Anything
  ranking pools by load multiplies by that number. It is now an exact count of
  checked-out connections.

- **MariaDB was a registered vendor that had never been run.** Both the pool and
  the CDC suites read `MYSQL_DSN` and used the MySQL flavour, so every MariaDB
  path — its GTID syntax, `gtid_binlog_pos` rather than `gtid_executed`, its own
  binlog event type — was unexecuted while the vendor was documented as supported.
  Both suites now fan out across `MYSQL_DSN` and `MARIADB_DSN` as named subtests,
  so a MariaDB-only failure is reported rather than hidden behind a passing MySQL
  run.

- **SQL Server had never been run either**, because its image is amd64-only and
  the previous container runtime segfaulted emulating it. It now passes under
  Apple's `container`, which does run it.

### Added

- **Runtime capacity control — `gpool.Resizable`.** `SetMaxConns` moves a pool's
  ceiling while it is running, and `EvictIdle` discards every idle connection.
  Both are optional capabilities reached by type assertion, like `BulkCopier` and
  `Notifier`, and both are implemented once in `pkg/pooling` so every vendor gets
  them.

  `SetMaxConns` never blocks. Growing hands out permits immediately; shrinking
  reclaims what is free and records the rest as debt that the next releases pay,
  because waiting for a checked-out connection would make a resize block on user
  code. Surplus connections are closed as they come back rather than pooled, so
  the ceiling bounds connections to the database and not merely concurrent
  checkouts.

  Growth requires `Config.MaxConnsLimit`, declared at construction and defaulting
  to `MaxConns`. A pool that can silently grow is a pool that can exhaust the
  database, so the headroom is the operator's decision. Reserving it is free: the
  permit set is a `struct{}` channel, whose element has no backing array at any
  capacity.

  `EvictIdle` exists because "the connections I hold are no longer the right ones"
  is a state only the caller knows — a backend that changed role, a rotated
  credential, a failover that kept the address. Closing and rebuilding the pool
  discards connections that are still fine.

- **`Stat.WaitingAcquires()`**, the number of callers parked for a connection at
  this instant. It is the one gauge among the acquisition counters and answers what
  none of the cumulative ones can: `EmptyAcquireCount` says the pool has been short
  at some point since start-up, not that it is short now.

- **`.junie/scripts/testdbs.sh`** brings up PostgreSQL, MySQL, MariaDB, ClickHouse
  and SQL Server with the settings the tests actually need — `wal_level=logical`,
  row-format binary logging — and prints the DSNs. Integration tests that are
  awkward to run are integration tests that do not get run, which is how two
  vendors stayed unverified.

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

[Unreleased]: https://github.com/gsoultan/gpool/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/gsoultan/gpool/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/gsoultan/gpool/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/gsoultan/gpool/compare/v0.5.0...v1.0.0
[0.5.0]: https://github.com/gsoultan/gpool/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/gsoultan/gpool/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/gsoultan/gpool/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/gsoultan/gpool/releases/tag/v0.1.0
