# Graph Report - .  (2026-08-06)

## Corpus Check
- 159 files · ~103,042 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1751 nodes · 3783 edges · 81 communities (71 shown, 10 thin omitted)
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 404 edges (avg confidence: 0.82)
- Token cost: 116,430 input · 0 output

## Community Hubs (Navigation)
- Pooling Engine Core
- Comparative Pool Benchmarks
- PostgreSQL CDC Subscriber
- PostgreSQL Replication Stream
- database/sql Transactions
- database/sql Connection Wrapper
- PostgreSQL Batch and Copy
- database/sql Row Scanning
- Engine and CDC Interfaces
- Bulk Copy Capability Tests
- gpool Pool Benchmarks
- Module Dependency Graph
- CDC Integration Tests
- SQL Server Vendor
- Project Invariants
- CDC Design History
- PostgreSQL Pool Interfaces
- Scale and Footprint Notes
- Vendor Module Boundaries
- MySQL CDC Position Rules
- Proxy Server Lifecycle
- ClickHouse Vendor
- Pooling Configuration
- Pool Release Gate Notes
- MySQL CDC Subscriber
- Proxy Session Startup
- MySQL Pool Vendor
- Vendor Factory Registry
- gpool Interface Fakes
- MySQL CDC Integration Tests
- Proxy Module Structure
- Library-Only Project Shape
- Proxy Backend Connection
- MySQL CDC Dependencies
- Benchmark and Network Rules
- MySQL Binlog Stream
- Permit Token Channel
- PostgreSQL Row Results
- PostgreSQL Pool Construction
- PostgreSQL Row Tests
- Multi-Database Registry Notes
- Interface Segregation Notes
- PostgreSQL Transaction Tests
- PostgreSQL Rows Iterator
- PostgreSQL Connection Driver
- PostgreSQL Connection Handle
- MySQL Column Name Resolution
- Junie Workflow Rules
- MySQL Table Filter
- PostgreSQL Pool Facade
- Junie Architecture Rules
- Proxy Message Relay
- PostgreSQL LISTEN/NOTIFY
- Pooling Statistics
- database/sql Tx Wrapper
- Software Architect Profile
- MySQL Position Tracking
- database/sql Result
- PostgreSQL Config Tests
- MySQL Position Unit Tests
- Junie Coding Guidelines
- PgBouncer Comparison Findings
- PostgreSQL Pool Config
- Proxy Throughput Benchmark
- Proxy Connection Helpers
- Graph Reconcile Script
- MySQL Syncer Configuration
- PostgreSQL CDC Config
- Pool Statistics Interfaces
- PostgreSQL Command Result
- PostgreSQL Single Row
- Proxy Credential Handling
- MySQL CDC Config
- MySQL Pool Config
- Pooling Connection Handle
- database/sql Pool Config
- PostgreSQL Deferred Error Row
- Proxy Socket Takeover
- MySQL Schema Mismatch
- Replication Slot Per Node
- File Naming Convention

## God Nodes (most connected - your core abstractions)
1. `session` - 35 edges
2. `newPool()` - 31 edges
3. `Proxy` - 30 edges
4. `Postgres` - 27 edges
5. `pgEventStream` - 25 edges
6. `MySQL` - 25 edges
7. `Rows` - 24 edges
8. `Conn` - 23 edges
9. `NewPool()` - 22 edges
10. `assign()` - 22 edges

## Surprising Connections (you probably didn't know these)
- `CLAUDE.md gpool Quick Reference` --semantically_similar_to--> `Library Only, Never an Application`  [INFERRED] [semantically similar]
  CLAUDE.md → AGENTS.md
- `Bound What Multiplies` --semantically_similar_to--> `Profile: Security Architect`  [INFERRED] [semantically similar]
  .serena/memories/scale.md → AGENTS.md
- `Serena Persistent Memory` --references--> `Gpool Core Memory`  [INFERRED]
  .junie/agents.md → .serena/memories/core.md
- `CLI Proxy Mode` --conceptually_related_to--> `gpool: a Go connection pooling and CDC library`  [AMBIGUOUS]
  .junie/plans/gpool-lib-init.md → README.md
- `Clone the GTID set: BinlogSyncer retains and mutates it` --semantically_similar_to--> `Event shape: Op, Schema, Table, Position, Before, After`  [INFERRED] [semantically similar]
  .serena/memories/cdc_mysql.md → README.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Token Channel Permit Set Replacing the Counting Semaphore** — _serena_memories_scale_semaphore_weighted_bottleneck, _serena_memories_scale_token_channel_permits, _serena_memories_pool_permits, _junie_plans_gpool_lib_init_token_channel_replacement, _serena_memories_scale_dropped_x_sync [INFERRED 0.85]
- **The CDC surface proven against two vendors** — _serena_memories_cdc_subscriber, _serena_memories_cdc_replicationmanager, _serena_memories_cdc_subscribefrom, _serena_memories_cdc_event_position, _serena_memories_cdc_mysql_module, _serena_memories_cdc_errnocdcsupport [EXTRACTED 1.00]
- **The release gate: what a caller must not leave behind** — _serena_memories_pool_recyclable, _serena_memories_pool_transaction_gate, _serena_memories_pool_listen_state, _serena_memories_pool_resetquery, _serena_memories_pool_needscleanup, readme_release_gate, _serena_memories_vendors_transaction_gate [INFERRED 0.85]
- **Transaction-mode backend handover in gpoolproxy** — _serena_memories_proxy_pump, _serena_memories_proxy_relay, _serena_memories_proxy_pending, _serena_memories_proxy_session_mutex, _serena_memories_proxy_message_relay [EXTRACTED 1.00]

## Communities (81 total, 10 thin omitted)

### Community 0 - "Pooling Engine Core"
Cohesion: 0.05
Nodes (50): Int64, Time, newCoarseClock(), Bool, C, CancelFunc, Config, Context (+42 more)

### Community 1 - "Comparative Pool Benchmarks"
Cohesion: 0.06
Nodes (64): BenchmarkPgBouncer(), BenchmarkPgxPool(), BenchmarkPgxPoolStress(), BenchmarkStdlib(), BenchmarkStdlibStress(), B, Config, hash() (+56 more)

### Community 2 - "PostgreSQL CDC Subscriber"
Cohesion: 0.08
Nodes (47): Postgres, slices, firstErr(), Config, T, longSlotName(), TestClosedSubscriberRefusesWork(), TestConfigDefaults() (+39 more)

### Community 3 - "PostgreSQL Replication Stream"
Cohesion: 0.05
Nodes (49): BackendMessage, Event, Op, pendingEvent, pgEventStream, DeleteMessage, EventType, fakeStream (+41 more)

### Community 4 - "database/sql Transactions"
Cohesion: 0.06
Nodes (36): Tx, Bool, Context, Int32, NamedValue, Stmt, Value, Config (+28 more)

### Community 5 - "database/sql Connection Wrapper"
Cohesion: 0.07
Nodes (19): pgRows, Context, Stmt, Context, newConnWrapper(), Connector, connWrapper, Context (+11 more)

### Community 6 - "PostgreSQL Batch and Copy"
Cohesion: 0.05
Nodes (23): CopyFromSource, FieldDescription, Batch, BatchQuery, BatchResults, Identifier, LargeObjects, T (+15 more)

### Community 7 - "database/sql Row Scanning"
Cohesion: 0.08
Nodes (43): reflect, errArity(), Bool, connWrapper, Seq, Value, newRows(), scanInto() (+35 more)

### Community 8 - "Engine and CDC Interfaces"
Cohesion: 0.08
Nodes (36): Stream, Subscriber, TableManager, net/url, Engine, databaseNamed(), Config, T (+28 more)

### Community 9 - "Bulk Copy Capability Tests"
Cohesion: 0.09
Nodes (40): BulkCopier, CopyRequest, CopyRows, sliceRows, Pool, T, scratchTable(), TestCopyFromLoadsRows() (+32 more)

### Community 10 - "gpool Pool Benchmarks"
Cohesion: 0.09
Nodes (43): BenchmarkGpoolAcquireRelease(), BenchmarkGpoolQueryIterator(), BenchmarkGpoolQueryRow(), BenchmarkGpoolQueryRowStress(), BenchmarkGpoolResetQuery(), B, Config, Pool (+35 more)

### Community 11 - "Module Dependency Graph"
Cohesion: 0.06
Nodes (45): filippo/io/edwards25519, github.com/andybalholm/brotli, github.com/cespare/xxhash/v2, github.com/clickhouse/ch/go, github.com/clickhouse/clickhouse/go/v2, github.com/coreos/go/semver, github.com/go/faster/city, github.com/go/faster/errors (+37 more)

### Community 12 - "CDC Integration Tests"
Cohesion: 0.15
Nodes (32): github.com/gsoultan/gpool/pkg/vendors/postgres/cdc, collect(), emailsOf(), Config, Duration, Pool, T, newCDCFixture() (+24 more)

### Community 13 - "SQL Server Vendor"
Cohesion: 0.13
Nodes (32): github.com/gsoultan/gpool/vendors/mssql, Config, dsn(), Config, Pool, T, newPool(), scratchTable() (+24 more)

### Community 14 - "Project Invariants"
Cohesion: 0.07
Nodes (34): Every Drain Is Bounded, Validate and Default Config at Construction, Caller-Owned Maps and Slices Allocated Fresh, Idempotent Teardown, No Callback Invoked Under Its Own Lock, No Panic Reaches the Caller, Never Recycle Caller-Owned Objects, Invariants (Memory) (+26 more)

### Community 15 - "CDC Design History"
Cohesion: 0.07
Nodes (32): CDC Control and Replication Connection Split, Integrated CDC via Logical Replication, Gpool Library Initialization Plan, Go 1.26 Iterator API, PgBouncer Replacement, Step 11: Production Hardening, Transaction-Mode Pooling Constraints, VendorFactory Pattern (+24 more)

### Community 16 - "PostgreSQL Pool Interfaces"
Cohesion: 0.12
Nodes (8): ReplicationManager, context, database/sql/driver, github.com/gsoultan/gpool/pkg/gpool, github.com/gsoultan/gpool/pkg/pooling, github.com/jackc/pgx/v5, Batcher, Pool

### Community 17 - "Scale and Footprint Notes"
Cohesion: 0.09
Nodes (29): Dropped golang.org/x/sync, Step 12: Scale and Footprint, Token Channel Replaces Counting Semaphore, Deliberately minimal dependencies (pgx/v5, pglogrepl), go-mysql logging discarded (gpool does no logging), permits: a token channel, not a counting semaphore, shard.count atomic mirror of len(conns), 16 cache-line-padded shards with randomised probe start (+21 more)

### Community 18 - "Vendor Module Boundaries"
Cohesion: 0.10
Nodes (28): File and Folder Readability Limits, Nothing public under internal/, Interfaces in pkg/gpool, implementations in pkg/vendors, ErrNoCDCSupport, vendors/mysql/cdc: binary log CDC as a nested module, ServerID is required and must be unique, TestMySQLCDCOffersNoReplicationManager, ReplicationManager as optional capability (+20 more)

### Community 19 - "MySQL CDC Position Rules"
Cohesion: 0.08
Nodes (28): checkResumable guards against rewinding behind the slot, ErrPositionBehindSlot, Event.Position is an opaque cdc.Position string, Clone the GTID set: BinlogSyncer retains and mutates it, MySQL values keep the binlog parser's native Go types, resume advances only at commit (XID or literal COMMIT), SHOW BINARY LOG STATUS replaces SHOW MASTER STATUS in 8.4, Tagged positions: gtid:<set> or file:<name>:<offset> (+20 more)

### Community 20 - "Proxy Server Lifecycle"
Cohesion: 0.10
Nodes (16): Addr, clientTLS(), Bool, CancelFunc, Config, Context, Mutex, Stat (+8 more)

### Community 21 - "ClickHouse Vendor"
Cohesion: 0.15
Nodes (25): Config, github.com/gsoultan/gpool/vendors/clickhouse, Options, Connector, Duration, Pool, New(), newFromConfig() (+17 more)

### Community 22 - "Pooling Configuration"
Cohesion: 0.09
Nodes (10): errors, github.com/clickhouse/clickhouse/go/v2, github.com/gsoultan/gpool/pkg/sqldriver, github.com/microsoft/go/mssqldb, math/rand/v2, runtime, time, Config (+2 more)

### Community 23 - "Pool Release Gate Notes"
Cohesion: 0.09
Nodes (25): Bounded Statement and Description Caches, Per-connection pgx caches bounded at 64, Driver.NeedsCleanup keeps release cheap, pgRow wraps pgx.Rows, not pgx.Row, Pooling mode is usage, not config, recyclable(): the ordered release gate, ResetQuery couples to query exec mode, Rows/Row ownership depends on the call level (+17 more)

### Community 24 - "MySQL CDC Subscriber"
Cohesion: 0.17
Nodes (8): MySQL, Position, BinlogStreamer, BinlogSyncer, Context, DB, GTIDSet, Mutex

### Community 25 - "Proxy Session Startup"
Cohesion: 0.15
Nodes (9): newRelay(), cutCString(), CancelFunc, Context, Mutex, Reader, Writer, parameterOf() (+1 more)

### Community 26 - "MySQL Pool Vendor"
Cohesion: 0.19
Nodes (24): github.com/gsoultan/gpool/vendors/mysql, Pool, newFromConfig(), dsn(), Config, Pool, T, newPool() (+16 more)

### Community 27 - "Vendor Factory Registry"
Cohesion: 0.20
Nodes (23): PoolFactory, SubscriberFactory, Vendor, Pool, NewPool(), NewSubscriber(), RegisterPool(), RegisterSubscriber() (+15 more)

### Community 28 - "gpool Interface Fakes"
Cohesion: 0.13
Nodes (6): EventStream, fakePool, fakeSubscriber, Context, Int32, Stat

### Community 29 - "MySQL CDC Integration Tests"
Cohesion: 0.29
Nodes (18): fixture, github.com/gsoultan/gpool/vendors/mysql/cdc, collect(), dsn(), Config, DB, Duration, T (+10 more)

### Community 30 - "Proxy Module Structure"
Cohesion: 0.17
Nodes (10): bufio, crypto/tls, encoding/binary, github.com/jackc/pgx/v5/pgconn, github.com/jackc/pgx/v5/pgproto3, io, iter, net (+2 more)

### Community 31 - "Library-Only Project Shape"
Cohesion: 0.11
Nodes (21): AGENTS.md Wins Over These Guidelines, Project Shape: Gpool Is a Library, CLI Proxy Mode, Step 4: Wire Proxy and CLI Removed, Gpool Core Memory, Library-Only Go Module, Serena Memory Index, Module Path github.com/gsoultan/gpool (+13 more)

### Community 32 - "Proxy Backend Connection"
Cohesion: 0.15
Nodes (10): Bool, Context, Reader, Writer, Config, Context, networkAddress(), backend (+2 more)

### Community 33 - "MySQL CDC Dependencies"
Cohesion: 0.18
Nodes (10): fmt, github.com/go/mysql/org/go/mysql/mysql, github.com/go/mysql/org/go/mysql/replication, github.com/go/sql/driver/mysql, github.com/gsoultan/gpool/pkg/gpool/cdc, github.com/jackc/pglogrepl, log/slog, strconv (+2 more)

### Community 34 - "Benchmark and Network Rules"
Cohesion: 0.11
Nodes (20): Close drains by acquiring all permits with closeDrainTimeout, maintain: the one background goroutine, Match Capacity on Both Sides, Two Readings of 5000 Connections, A Benchmark Comparison Must Match Capacity on Both Sides, Iteration Count Changes What a Benchmark Reports, Bound Connection Lifetime for Failover Recovery, Every Network Operation Is Bounded (+12 more)

### Community 35 - "MySQL Binlog Stream"
Cohesion: 0.16
Nodes (12): BinlogEvent, mysqlEventStream, BinlogStreamer, BinlogSyncer, Bool, CancelFunc, Context, Mutex (+4 more)

### Community 36 - "Permit Token Channel"
Cohesion: 0.21
Nodes (10): Context, newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate(), TestPermitsHoldTheBoundUnderContention() (+2 more)

### Community 37 - "PostgreSQL Row Results"
Cohesion: 0.16
Nodes (7): Row, Rows, Bool, closeRows(), batchResults, failedBatchResults, rowCursor

### Community 38 - "PostgreSQL Pool Construction"
Cohesion: 0.22
Nodes (16): Pool, newFromConfig(), Config, New(), Config, T, newTestPool(), TestAcquireAfterCloseFailsFast() (+8 more)

### Community 39 - "PostgreSQL Row Tests"
Cohesion: 0.27
Nodes (15): newRow(), connWrapper, newRows(), T, TestErrorRowDefersTheError(), TestResultReportsRowsAffected(), TestRowReleaseWithoutScanClosesTheResultSet(), TestRowsAllClosesOnEarlyBreak() (+7 more)

### Community 40 - "Multi-Database Registry Notes"
Cohesion: 0.14
Nodes (15): Testing Standards, Multi-Database Engine Pool Registry, Step 10: Multi-Database Support, Pool(name) Naming Decision, Engine: named registries of pools and subscribers, One pool per database, sharing nothing, CDC Fixtures Drop Their Slot, MaxConns: 1 as Leak Detector (+7 more)

### Community 41 - "Interface Segregation Notes"
Cohesion: 0.16
Nodes (15): Interface Segregation: interfaces of 7 methods or fewer, Control connection (ordinary pgconn.PgConn), Client-side table filter shared with the running stream, quoteQualifiedName splits on the first dot, Replication connection (replication=database), slotNamePattern validation, Local table list updated only after the server accepts, CDC cannot be pooled (+7 more)

### Community 42 - "PostgreSQL Transaction Tests"
Cohesion: 0.24
Nodes (9): Bool, Context, newTx(), T, TestTxCommitWithDeferredRollback(), TestTxRefusesUseAfterSettle(), TestTxRollbackWithDeferredRollback(), TestTxSettlesExactlyOnce() (+1 more)

### Community 43 - "PostgreSQL Rows Iterator"
Cohesion: 0.16
Nodes (4): Field, Bool, Seq, pgRows

### Community 44 - "PostgreSQL Connection Driver"
Cohesion: 0.30
Nodes (6): closeConn(), Config, ConnConfig, Context, pgConn, pgxDriver

### Community 45 - "PostgreSQL Connection Handle"
Cohesion: 0.36
Nodes (4): Context, connWrapper, newConnWrapper(), Handle

### Community 46 - "MySQL Column Name Resolution"
Cohesion: 0.27
Nodes (7): columns, TableMapEvent, Context, DB, Mutex, newColumns(), qualify()

### Community 47 - "Junie Workflow Rules"
Cohesion: 0.20
Nodes (11): Graphify Knowledge Graph, Mandatory Workflow Rules, Obsidian Agentic Second Brain, rtk Token Optimization, Serena Persistent Memory, Hierarchy of Reading (Levels 0-4), RTK - Rust Token Killer, sqz (Compression and Dedup) (+3 more)

### Community 48 - "MySQL Table Filter"
Cohesion: 0.27
Nodes (4): filter, RWMutex, newFilter(), normalize()

### Community 49 - "PostgreSQL Pool Facade"
Cohesion: 0.25
Nodes (5): connWrapper, Context, Postgres, Stat, translate()

### Community 50 - "Junie Architecture Rules"
Cohesion: 0.22
Nodes (10): Integration Tests Moved to integration/, Inverted /internal Layout, Layered Architecture Pattern, No Build Target, SQL Externalization Not Applicable, sync.WaitGroup.Go from Stdlib, Superseded Service Template Rules, Restricted sync.Pool Usage (+2 more)

### Community 51 - "Proxy Message Relay"
Cohesion: 0.40
Nodes (5): endsTransactionUnit(), flushIfDrained(), Reader, Writer, relay

### Community 52 - "PostgreSQL LISTEN/NOTIFY"
Cohesion: 0.29
Nodes (5): Notification, Notifier, Context, connWrapper, quoteIdentifier()

### Community 54 - "database/sql Tx Wrapper"
Cohesion: 0.31
Nodes (5): Bool, connWrapper, Context, newTx(), pgTx

### Community 55 - "Software Architect Profile"
Cohesion: 0.22
Nodes (9): Vendor Registry (Only Process-Global State), Interface Segregation (7-Method Ceiling, Composition), Optional Capabilities Reached by Type Assertion, pkg/pooling (Shared Engine in Core Module), pkg/sqldriver (Stdlib-Only, Serves Every database/sql Driver), One Pool Per Database, Sharing Nothing, Profile: Software Architect, A Vendor Is Its Own Go Module (+1 more)

### Community 56 - "MySQL Position Tracking"
Cohesion: 0.31
Nodes (5): tracker, GTIDSet, newTracker(), position, GTIDSet

### Community 58 - "PostgreSQL Config Tests"
Cohesion: 0.39
Nodes (8): T, TestConfigBoundsPerConnectionCaches(), TestConfigDefaultsGiveUsableCapacity(), TestConfigDefaultsPreserveExplicitValues(), TestConfigParseIsOrderIndependent(), TestConfigParseRejectsBadConnString(), TestConfigResetQuerySelectsACompatibleExecMode(), TestConfigValidate()

### Community 59 - "MySQL Position Unit Tests"
Cohesion: 0.44
Nodes (8): parsePosition(), T, TestFilterMatching(), TestFilterMutation(), TestParsePositionIsFlavourSpecific(), TestParsePositionRejectsAForeignPosition(), TestPositionDistinguishesItsTwoNotations(), TestPositionRoundTrips()

### Community 60 - "Junie Coding Guidelines"
Cohesion: 0.25
Nodes (8): Avoid Stuttering, PgBouncer Transaction Mode, Avoid Stuttering Rule, Interface Segregation Principle, Junie Guidelines, No Memory Leaks Rule, PgBouncer Best Practices Reference, Post-Task Cleanup and Maintenance

### Community 61 - "PgBouncer Comparison Findings"
Cohesion: 0.29
Nodes (8): Interleave benchmark targets; do not sweep one to completion, PgBouncer's one-thread, one-core ceiling, Measured against PgBouncer 1.25.2, Interleaved targets and medians of three runs, PgBouncer cannot use a second core, Throughput measured against PgBouncer 1.25.2, PgBouncer is the more efficient of the two, so_reuseport as PgBouncer's answer to one core

### Community 62 - "PostgreSQL Pool Config"
Cohesion: 0.29
Nodes (4): cacheCapacity(), ConnConfig, Duration, Config

### Community 63 - "Proxy Throughput Benchmark"
Cohesion: 0.43
Nodes (5): benchmarkTarget(), BenchmarkThroughput(), B, Pool, warm()

### Community 65 - "Graph Reconcile Script"
Cohesion: 0.43
Nodes (6): is_package(), main(), package_label(), Recover a readable import path from a synthesized package id.      The id is los, Return the extraction with package nodes added and unresolvable edges dropped., reconcile()

### Community 66 - "MySQL Syncer Configuration"
Cohesion: 0.33
Nodes (5): BinlogSyncerConfig, newFromConfig(), Config, New(), splitHostPort()

### Community 67 - "PostgreSQL CDC Config"
Cohesion: 0.33
Nodes (3): regexp, Config, Duration

### Community 68 - "Pool Statistics Interfaces"
Cohesion: 0.33
Nodes (3): Acquisition, Occupancy, Stat

### Community 70 - "PostgreSQL Single Row"
Cohesion: 0.47
Nodes (3): Bool, connWrapper, pgRow

### Community 71 - "Proxy Credential Handling"
Cohesion: 0.40
Nodes (5): Decoy verifier defeats the username oracle, SCRAM-SHA-256, verifier only, Upstream string comes from the environment, not a flag, Userlist refused if readable beyond its owner, Credential handling: hash subcommand, userlist, env upstream

### Community 73 - "MySQL Pool Config"
Cohesion: 0.40
Nodes (3): Config, Connector, Duration

### Community 75 - "database/sql Pool Config"
Cohesion: 0.50
Nodes (3): Connector, Duration, Config

### Community 77 - "Proxy Socket Takeover"
Cohesion: 0.67
Nodes (3): Taking a socket over from pgx with Hijack(), ParameterStatus values captured from a real backend, Frontend.ReadBufferLen() asserted, not assumed

## Ambiguous Edges - Review These
- `CLI Proxy Mode` → `gpool: a Go connection pooling and CDC library`  [AMBIGUOUS]
  .junie/plans/gpool-lib-init.md · relation: conceptually_related_to
- `Gpool Core Memory` → `Supported databases and their modules`  [AMBIGUOUS]
  .serena/memories/core.md · relation: conceptually_related_to

## Knowledge Gaps
- **62 isolated node(s):** `Batcher`, `BulkCopier`, `ReplicationManager`, `Notifier`, `Pool` (+57 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `CLI Proxy Mode` and `gpool: a Go connection pooling and CDC library`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Gpool Core Memory` and `Supported databases and their modules`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `Core` connect `Pooling Engine Core` to `Permit Token Channel`, `database/sql Connection Wrapper`, `PostgreSQL Connection Handle`, `PostgreSQL Pool Facade`, `Proxy Server Lifecycle`, `Pooling Configuration`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Why does `Proxy` connect `Proxy Server Lifecycle` to `Proxy Backend Connection`, `Proxy Connection Helpers`, `Comparative Pool Benchmarks`, `Pooling Engine Core`, `Proxy Session Startup`, `Proxy Module Structure`?**
  _High betweenness centrality (0.035) - this node is a cross-community bridge._
- **Why does `MySQL` connect `MySQL CDC Subscriber` to `MySQL CDC Dependencies`, `MySQL Syncer Configuration`, `MySQL Binlog Stream`, `MySQL Column Name Resolution`, `MySQL Table Filter`?**
  _High betweenness centrality (0.033) - this node is a cross-community bridge._
- **What connects `Batcher`, `BulkCopier`, `ReplicationManager` to the rest of the system?**
  _62 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Pooling Engine Core` be split into smaller, more focused modules?**
  _Cohesion score 0.05393000573723465 - nodes in this community are weakly interconnected._