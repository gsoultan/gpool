# Graph Report - .  (2026-08-05)

## Corpus Check
- 131 files · ~78,076 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1503 nodes · 3143 edges · 72 communities (66 shown, 6 thin omitted)
- Extraction: 87% EXTRACTED · 13% INFERRED · 0% AMBIGUOUS · INFERRED: 409 edges (avg confidence: 0.84)
- Token cost: 127,394 input · 0 output

## Community Hubs (Navigation)
- CDC Subscriber and Publication Management
- SQL Driver Adapter Internals
- Pooling Engine Core
- CDC Stream Reader Loop
- Vendor Concepts and Trade-offs
- Batch and Copy Types
- Baseline Benchmarks
- Bulk Copy Types
- SQL Driver Pool
- SQL Server Vendor
- Coarse Clock
- ClickHouse Driver Dependencies
- CDC Integration Tests
- Scale and Footprint
- Gpool Benchmarks
- Project Invariants
- Architecture and Multi-Database
- Core Package Imports
- MySQL Vendor
- Concurrency and Lifecycle Invariants
- Core Interface Test Doubles
- Vendor Module Imports
- Vendor Registry and Factory
- Library-Only Shape
- CDC Replication Semantics
- CDC Internals and LSN Tracking
- ClickHouse Vendor
- Fake Driver Test Doubles
- Benchmark Hygiene and Measurement
- Engine Pool Registry
- Permit Token Channel
- Permit Token Channel (Engine)
- Row and Rows Types
- Interface Design and Role Profiles
- Engine Unit Tests
- Transaction Type
- Fake Driver Connection
- Rows and Row Unit Tests
- Testing Doctrine
- SQL Driver Rows
- Error Classification and Security
- Rows Iterator
- ClickHouse Config
- Postgres Connection and Config
- Postgres Connection Wrapper
- CDC Interfaces
- Multi-Database Integration Tests
- Knowledge Tooling
- Engine Shard
- Security Profile
- Sentinel Errors
- LISTEN and NOTIFY
- Statistics Interfaces
- Pool Statistics
- Superseded Template Rules
- Pool Config Unit Tests
- Junie Guidelines
- Exec Result
- Idle Connection State
- Postgres Shard
- SQL Driver Transaction
- Graphify Reconcile Script
- Postgres Coarse Clock
- Engine Idle Connection
- Transaction Unit Tests
- Occupancy and Acquisition
- Single Row Result
- SQL Driver Config
- CDC Config Validation
- MySQL Config
- Deferred Error Row
- Fake Transaction

## God Nodes (most connected - your core abstractions)
1. `Postgres` - 34 edges
2. `newPool()` - 31 edges
3. `pgEventStream` - 25 edges
4. `Rows` - 24 edges
5. `Postgres` - 24 edges
6. `assign()` - 22 edges
7. `v0.1.0 First Tagged Release` - 22 edges
8. `NewPool()` - 21 edges
9. `newTestPool()` - 21 edges
10. `newTestCore()` - 19 edges

## Surprising Connections (you probably didn't know these)
- `CLI Proxy Mode` --conceptually_related_to--> `Gpool: A Go Connection Pooling & CDC Library`  [AMBIGUOUS]
  .junie/plans/gpool-lib-init.md → README.md
- `Bound What Multiplies` --semantically_similar_to--> `Profile: Security Architect`  [INFERRED] [semantically similar]
  .serena/memories/scale.md → AGENTS.md
- `CLAUDE.md gpool Quick Reference` --semantically_similar_to--> `Library Only, Never an Application`  [INFERRED] [semantically similar]
  CLAUDE.md → AGENTS.md
- `init()` --calls--> `RegisterPool()`  [INFERRED]
  vendors/clickhouse/clickhouse.go → pkg/gpool/factory.go
- `sync.WaitGroup.Go from Stdlib` --conceptually_related_to--> `Dropped golang.org/x/sync Dependency`  [INFERRED]
  .junie/agents.md → .serena/memories/scale.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Multi-Module Vendor Packaging** — readme_multi_module_vendor_packaging, _serena_memories_vendors_module_layout, agents_vendor_is_own_module, changelog_four_new_databases, changelog_module_weight [INFERRED 0.95]
- **The database/sql Vendor Stack** — _serena_memories_vendors_pkg_sqldriver, _serena_memories_vendors_database_sql_vendor, changelog_vendors_mysql, changelog_vendors_mssql, changelog_vendors_clickhouse, readme_driver_conn_pooling [INFERRED 0.85]
- **Making the MaxConns Ceiling Exact** — changelog_maxconns_ceiling_bug, changelog_permit_insufficient, changelog_reserve_slot_in_total, readme_pool_configuration [INFERRED 0.85]

## Communities (72 total, 6 thin omitted)

### Community 0 - "CDC Subscriber and Publication Management"
Cohesion: 0.07
Nodes (49): Postgres, github.com/jackc/pgx/v5/pgconn, firstErr(), Config, T, longSlotName(), TestClosedSubscriberRefusesWork(), TestConfigDefaults() (+41 more)

### Community 1 - "SQL Driver Adapter Internals"
Cohesion: 0.05
Nodes (41): pgRows, Context, Stmt, Context, newConnWrapper(), Config, Connector, connWrapper (+33 more)

### Community 2 - "Pooling Engine Core"
Cohesion: 0.06
Nodes (45): Bool, C, CancelFunc, coarseClock, Config, Context, idleConn, Int32 (+37 more)

### Community 3 - "CDC Stream Reader Loop"
Cohesion: 0.06
Nodes (44): BackendMessage, Event, Op, pgEventStream, DeleteMessage, github.com/jackc/pglogrepl, github.com/jackc/pgx/v5/pgproto3, fakeStream (+36 more)

### Community 4 - "Vendor Concepts and Trade-offs"
Cohesion: 0.05
Nodes (58): Insert in Batches Against a Column Store, ClickHouse Vendor, Config.Options Escape Hatch, database/sql Vendor (~100 Lines), driver.Value Is Not a Closed Set, DSN Parsed Once in New, Inbound Value Conversion (value.go), LastInsertID by Type Assertion (+50 more)

### Community 5 - "Batch and Copy Types"
Cohesion: 0.06
Nodes (23): CopyFromSource, FieldDescription, Batch, BatchQuery, BatchResults, Identifier, LargeObjects, T (+15 more)

### Community 6 - "Baseline Benchmarks"
Cohesion: 0.09
Nodes (47): BenchmarkPgBouncer(), BenchmarkPgxPool(), BenchmarkPgxPoolStress(), BenchmarkStdlib(), BenchmarkStdlibStress(), B, database/sql, github.com/jackc/pgx/v5/pgxpool (+39 more)

### Community 7 - "Bulk Copy Types"
Cohesion: 0.09
Nodes (40): BulkCopier, CopyRequest, CopyRows, sliceRows, Pool, T, scratchTable(), TestCopyFromLoadsRows() (+32 more)

### Community 8 - "SQL Driver Pool"
Cohesion: 0.11
Nodes (19): Pool, newFromConfig(), closeConn(), Bool, CancelFunc, coarseClock, Config, ConnConfig (+11 more)

### Community 9 - "SQL Server Vendor"
Cohesion: 0.12
Nodes (33): github.com/gsoultan/gpool/vendors/mssql, strings, Config, dsn(), Config, Pool, T, newPool() (+25 more)

### Community 10 - "Coarse Clock"
Cohesion: 0.12
Nodes (30): testing, Int64, Time, newCoarseClock(), T, TestCoarseClockIsSeeded(), TestCoarseClockUpdates(), TestPoolExpiryUsesTheCachedClock() (+22 more)

### Community 11 - "ClickHouse Driver Dependencies"
Cohesion: 0.07
Nodes (33): filippo/io/edwards25519, github.com/andybalholm/brotli, github.com/cespare/xxhash/v2, github.com/clickhouse/ch/go, github.com/clickhouse/clickhouse/go/v2, github.com/go/faster/city, github.com/go/faster/errors, github.com/go/sql/driver/mysql (+25 more)

### Community 12 - "CDC Integration Tests"
Cohesion: 0.16
Nodes (28): github.com/gsoultan/gpool/pkg/vendors/postgres/cdc, collect(), Config, Duration, Pool, T, newCDCFixture(), TestCDCCloseIsIdempotent() (+20 more)

### Community 13 - "Scale and Footprint"
Cohesion: 0.10
Nodes (28): Bounded Statement and Description Caches, Dropped golang.org/x/sync, Step 12: Scale and Footprint, Token Channel Replaces Counting Semaphore, Benchmark Hygiene, Bound What Multiplies, Cache Capacity Default 64, DisableCache (+20 more)

### Community 14 - "Gpool Benchmarks"
Cohesion: 0.14
Nodes (26): BenchmarkGpoolAcquireRelease(), BenchmarkGpoolQueryIterator(), BenchmarkGpoolQueryRow(), BenchmarkGpoolQueryRowStress(), BenchmarkGpoolResetQuery(), B, Config, Pool (+18 more)

### Community 15 - "Project Invariants"
Cohesion: 0.08
Nodes (27): Caller-Owned Maps and Slices Allocated Fresh, Rows.FieldDescriptions: Column Names Only, Errors Classified by Code, Never Message Text, A Caller-Owned Map or Slice Is Allocated Fresh, Every Teardown Method Is Idempotent, Never Recycle an Object Whose Lifetime User Code Controls, No Panics Reach the Caller, One Goroutine Owns a Connection (+19 more)

### Community 16 - "Architecture and Multi-Database"
Cohesion: 0.11
Nodes (26): Inverted /internal Layout, File and Folder Readability Limits, Multi-Database Engine Pool Registry, Step 10: Multi-Database Support, Independent Replication Slot per Node, Pool(name) Naming Decision, VendorFactory Pattern, Architecture (+18 more)

### Community 17 - "Core Package Imports"
Cohesion: 0.18
Nodes (9): context, github.com/gsoultan/gpool/pkg/gpool, github.com/jackc/pgx/v5, iter, math/rand/v2, runtime, sync/atomic, Batcher (+1 more)

### Community 18 - "MySQL Vendor"
Cohesion: 0.19
Nodes (24): github.com/gsoultan/gpool/vendors/mysql, Pool, newFromConfig(), dsn(), Config, Pool, T, newPool() (+16 more)

### Community 19 - "Concurrency and Lifecycle Invariants"
Cohesion: 0.11
Nodes (24): run() Defer Order, Every Drain Is Bounded, Validate and Default Config at Construction, No Callback Invoked Under Its Own Lock, No Panic Reaches the Caller, Invariants (Memory), One Goroutine Owns a Connection, Only the Blocking Path Is Timed (+16 more)

### Community 20 - "Core Interface Test Doubles"
Cohesion: 0.13
Nodes (6): EventStream, fakePool, fakeSubscriber, Context, Int32, Stat

### Community 21 - "Vendor Module Imports"
Cohesion: 0.13
Nodes (7): database/sql/driver, fmt, github.com/go/sql/driver/mysql, github.com/gsoultan/gpool/pkg/pooling, github.com/microsoft/go/mssqldb, regexp, time

### Community 22 - "Vendor Registry and Factory"
Cohesion: 0.23
Nodes (20): github.com/gsoultan/gpool/pkg/gpool/cdc, PoolFactory, SubscriberFactory, Vendor, Pool, NewPool(), NewSubscriber(), RegisterPool() (+12 more)

### Community 23 - "Library-Only Shape"
Cohesion: 0.11
Nodes (21): AGENTS.md Wins Over These Guidelines, Project Shape: Gpool Is a Library, CLI Proxy Mode, Step 4: Wire Proxy and CLI Removed, Gpool Core Memory, Library-Only Go Module, Serena Memory Index, Module Path github.com/gsoultan/gpool (+13 more)

### Community 24 - "CDC Replication Semantics"
Cohesion: 0.11
Nodes (21): CDC Control and Replication Connection Split, Integrated CDC via Logical Replication, Gpool Library Initialization Plan, Go 1.26 Iterator API, PgBouncer Replacement, Step 11: Production Hardening, Transaction-Mode Pooling Constraints, Idempotent Teardown (+13 more)

### Community 25 - "CDC Internals and LSN Tracking"
Cohesion: 0.13
Nodes (19): advance() CAS Loop, catchUp (idle WAL release), CDC Internals Memory, Control Connection, emit (keepalive-aware handoff), LSN Position Tracking (received/lastPushed/flushed), quoteQualifiedName, Replication Connection (+11 more)

### Community 26 - "ClickHouse Vendor"
Cohesion: 0.25
Nodes (18): github.com/gsoultan/gpool/vendors/clickhouse, dsn(), Config, Pool, T, joinComma(), newPool(), scratchTable() (+10 more)

### Community 27 - "Fake Driver Test Doubles"
Cohesion: 0.16
Nodes (8): io, Int32, Value, bareConn, fakeConnector, fakeResult, fakeRows, fakeStmt

### Community 28 - "Benchmark Hygiene and Measurement"
Cohesion: 0.13
Nodes (18): Match Capacity on Both Sides, Podman Logical-Replication Test Server, Pools driver.Conn, Not *sql.DB, A Benchmark Comparison Must Match Capacity on Both Sides, Coarse Clock: 293.9 to 198.3 ns/op on the Acquire Path, Cost: Sub-Second Lifetime Bounds Become Imprecise, Statistics: Occupancy plus Acquisition, Token Channel Replaces the Counting Semaphore (+10 more)

### Community 29 - "Engine Pool Registry"
Cohesion: 0.16
Nodes (8): sync, Engine, Once, Pool, keysOf(), nameOrDefault(), RWMutex, V

### Community 30 - "Permit Token Channel"
Cohesion: 0.21
Nodes (10): Context, newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate(), TestPermitsHoldTheBoundUnderContention() (+2 more)

### Community 31 - "Permit Token Channel (Engine)"
Cohesion: 0.21
Nodes (10): Context, newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate(), TestPermitsHoldTheBoundUnderContention() (+2 more)

### Community 32 - "Row and Rows Types"
Cohesion: 0.16
Nodes (7): Row, Rows, Bool, closeRows(), batchResults, failedBatchResults, rowCursor

### Community 33 - "Interface Design and Role Profiles"
Cohesion: 0.18
Nodes (16): Vendor Registry (Only Process-Global State), Optional Capabilities (Memory), Interface Segregation (7-Method Ceiling, Composition), Optional Capabilities Reached by Type Assertion, One Pool Per Database, Sharing Nothing, Profile: Software Architect, Vendors Self-Register from init(), Architecture: Interfaces Separated from Vendor Implementations (+8 more)

### Community 34 - "Engine Unit Tests"
Cohesion: 0.31
Nodes (15): slices, NewEngine(), T, TestEngineAddAndRemoveSubscriber(), TestEngineAddPoolReplacesWithoutClosing(), TestEngineCloseClosesEveryPool(), TestEngineCloseIsIdempotent(), TestEngineCloseJoinsErrorsAndClosesEverything() (+7 more)

### Community 35 - "Transaction Type"
Cohesion: 0.18
Nodes (7): Tx, Bool, connWrapper, Context, newTx(), pgTx, TxOptions

### Community 36 - "Fake Driver Connection"
Cohesion: 0.19
Nodes (5): Bool, Context, NamedValue, Stmt, fakeDriverConn

### Community 37 - "Rows and Row Unit Tests"
Cohesion: 0.27
Nodes (15): newRow(), connWrapper, newRows(), T, TestErrorRowDefersTheError(), TestResultReportsRowsAffected(), TestRowReleaseWithoutScanClosesTheResultSet(), TestRowsAllClosesOnEarlyBreak() (+7 more)

### Community 38 - "Testing Doctrine"
Cohesion: 0.13
Nodes (15): Testing Standards, Never Recycle Caller-Owned Objects, Rows vs Row Ownership, CDC Fixtures Drop Their Slot, MaxConns: 1 as Leak Detector, Testing, Unit Tests Cannot See Driver State, SQL Server Untestable on Apple Silicon (+7 more)

### Community 39 - "SQL Driver Rows"
Cohesion: 0.17
Nodes (8): errArity(), Bool, connWrapper, Seq, Value, newRows(), scanInto(), pgRows

### Community 40 - "Error Classification and Security"
Cohesion: 0.16
Nodes (14): Classify Errors by SQLSTATE, go-sql-driver ResetSession Does Not Roll Back, The Transaction Gate Is Ours, Not the Driver's, Profile: Database Architect, Identifier and Literal Quoting, Injection Surface Discipline, The Pooling Contract (recyclable()), ResetQuery Forces QueryExecModeExec (+6 more)

### Community 41 - "Rows Iterator"
Cohesion: 0.16
Nodes (4): Field, Bool, Seq, pgRows

### Community 42 - "ClickHouse Config"
Cohesion: 0.21
Nodes (10): Config, github.com/clickhouse/clickhouse/go/v2, github.com/gsoultan/gpool/pkg/sqldriver, Options, Connector, Duration, Pool, init() (+2 more)

### Community 43 - "Postgres Connection and Config"
Cohesion: 0.17
Nodes (5): Conn, cacheCapacity(), ConnConfig, Duration, Config

### Community 44 - "Postgres Connection Wrapper"
Cohesion: 0.31
Nodes (6): Bool, Context, idleConn, connWrapper, Postgres, newConnWrapper()

### Community 45 - "CDC Interfaces"
Cohesion: 0.17
Nodes (6): ReplicationManager, Stream, Subscriber, TableManager, init(), newFromConfig()

### Community 46 - "Multi-Database Integration Tests"
Cohesion: 0.42
Nodes (11): net/url, databaseNamed(), Config, T, multiDBEngine(), provisionDatabases(), TestMultiDatabaseEngineCloseClosesAll(), TestMultiDatabaseIsolatesData() (+3 more)

### Community 47 - "Knowledge Tooling"
Cohesion: 0.20
Nodes (11): Graphify Knowledge Graph, Mandatory Workflow Rules, Obsidian Agentic Second Brain, rtk Token Optimization, Serena Persistent Memory, Hierarchy of Reading (Levels 0-4), RTK - Rust Token Killer, sqz (Compression and Dedup) (+3 more)

### Community 48 - "Engine Shard"
Cohesion: 0.31
Nodes (6): C, idleConn, Int32, Mutex, shard, shard[C]

### Community 49 - "Security Profile"
Cohesion: 0.20
Nodes (10): Credentials Never Logged, Wrapped, or Stored, Destructive Operations Are Explicit, Health-Gate the Return Path, Least Privilege (REPLICATION, Table Ownership), Bounded Queues, Capped Pools, Deadlined Waits, Profile: Security Architect, Fix: MaxConns Bounded Only Concurrent Checkouts, Holding a Permit Before Dialling Is Not Sufficient (+2 more)

### Community 50 - "Sentinel Errors"
Cohesion: 0.20
Nodes (3): errors, Duration, Config

### Community 51 - "LISTEN and NOTIFY"
Cohesion: 0.29
Nodes (5): Notification, Notifier, Context, connWrapper, quoteIdentifier()

### Community 54 - "Superseded Template Rules"
Cohesion: 0.25
Nodes (9): Integration Tests Moved to integration/, Layered Architecture Pattern, No Build Target, SQL Externalization Not Applicable, sync.WaitGroup.Go from Stdlib, Superseded Service Template Rules, Restricted sync.Pool Usage, Test Layout (+1 more)

### Community 55 - "Pool Config Unit Tests"
Cohesion: 0.39
Nodes (8): T, TestConfigBoundsPerConnectionCaches(), TestConfigDefaultsGiveUsableCapacity(), TestConfigDefaultsPreserveExplicitValues(), TestConfigParseIsOrderIndependent(), TestConfigParseRejectsBadConnString(), TestConfigResetQuerySelectsACompatibleExecMode(), TestConfigValidate()

### Community 56 - "Junie Guidelines"
Cohesion: 0.25
Nodes (8): Avoid Stuttering, PgBouncer Transaction Mode, Avoid Stuttering Rule, Interface Segregation Principle, Junie Guidelines, No Memory Leaks Rule, PgBouncer Best Practices Reference, Post-Task Cleanup and Maintenance

### Community 58 - "Idle Connection State"
Cohesion: 0.50
Nodes (3): Duration, Time, idleConn

### Community 59 - "Postgres Shard"
Cohesion: 0.39
Nodes (4): idleConn, Int32, Mutex, shard

### Community 60 - "SQL Driver Transaction"
Cohesion: 0.43
Nodes (3): Bool, Context, pgTx

### Community 61 - "Graphify Reconcile Script"
Cohesion: 0.43
Nodes (6): is_package(), main(), package_label(), Recover a readable import path from a synthesized package id.      The id is los, Return the extraction with package nodes added and unresolvable edges dropped., reconcile()

### Community 62 - "Postgres Coarse Clock"
Cohesion: 0.38
Nodes (4): Int64, Time, newCoarseClock(), coarseClock

### Community 63 - "Engine Idle Connection"
Cohesion: 0.29
Nodes (5): C, Duration, Time, idleConn, idleConn[C]

### Community 64 - "Transaction Unit Tests"
Cohesion: 0.57
Nodes (6): newTx(), T, TestTxCommitWithDeferredRollback(), TestTxRefusesUseAfterSettle(), TestTxRollbackWithDeferredRollback(), TestTxSettlesExactlyOnce()

### Community 65 - "Occupancy and Acquisition"
Cohesion: 0.33
Nodes (3): Acquisition, Occupancy, Stat

### Community 66 - "Single Row Result"
Cohesion: 0.47
Nodes (3): Bool, connWrapper, pgRow

### Community 67 - "SQL Driver Config"
Cohesion: 0.50
Nodes (3): Connector, Duration, Config

### Community 69 - "MySQL Config"
Cohesion: 0.50
Nodes (3): Config, Connector, Duration

## Ambiguous Edges - Review These
- `CLI Proxy Mode` → `Gpool: A Go Connection Pooling & CDC Library`  [AMBIGUOUS]
  .junie/plans/gpool-lib-init.md · relation: conceptually_related_to
- `Minimal Dependencies` → `Dropped golang.org/x/sync Dependency`  [AMBIGUOUS]
  .serena/memories/architecture.md · relation: conceptually_related_to
- `Gpool Core Memory` → `Supported Databases`  [AMBIGUOUS]
  .serena/memories/core.md · relation: conceptually_related_to
- `Known Limitations` → `Supported Databases`  [AMBIGUOUS]
  CHANGELOG.md · relation: conceptually_related_to

## Knowledge Gaps
- **47 isolated node(s):** `Batcher`, `BulkCopier`, `Notifier`, `Pool`, `idleConn[C]` (+42 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **6 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `CLI Proxy Mode` and `Gpool: A Go Connection Pooling & CDC Library`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Minimal Dependencies` and `Dropped golang.org/x/sync Dependency`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Gpool Core Memory` and `Supported Databases`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Known Limitations` and `Supported Databases`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `Rows` connect `Row and Rows Types` to `SQL Driver Adapter Internals`, `Single Row Result`, `Transaction Type`, `Fake Driver Connection`, `Rows and Row Unit Tests`, `Batch and Copy Types`, `SQL Driver Rows`, `SQL Driver Pool`, `Rows Iterator`, `Postgres Connection Wrapper`, `Core Interface Test Doubles`, `Fake Driver Test Doubles`, `SQL Driver Transaction`?**
  _High betweenness centrality (0.024) - this node is a cross-community bridge._
- **Why does `Handle` connect `Pooling Engine Core` to `SQL Driver Adapter Internals`?**
  _High betweenness centrality (0.021) - this node is a cross-community bridge._
- **Why does `Core` connect `Pooling Engine Core` to `Core Package Imports`, `SQL Driver Adapter Internals`?**
  _High betweenness centrality (0.020) - this node is a cross-community bridge._