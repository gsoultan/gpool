# Graph Report - .  (2026-08-08)

## Corpus Check
- 180 files · ~132,138 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2119 nodes · 4934 edges · 84 communities (77 shown, 7 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 596 edges (avg confidence: 0.84)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- PostgreSQL Integration Fixtures
- PostgreSQL CDC Subscriber
- CDC Design Notes
- PostgreSQL Logical Decoding Stream
- SQL Server CDC Polling
- Vendor Factory And Godoc Examples
- Pooling Core Unit Tests
- database/sql Pool Adapter
- Batch And Bulk Copy Mocks
- Value Conversion And Scanning
- Engine And Multi-Database Tests
- Scale And Footprint Notes
- CDC Mocks And Fixtures
- Module Dependency Graph
- PostgreSQL Pool And Driver
- Cross-Application Proxy Notes
- CDC Package Imports
- Core Package Imports
- Junie Agent Guidelines
- SQL Server Pool Tests
- MySQL Integration And Failure Tests
- Project Invariants
- Proxy Listener And Userlist
- Testing And CI
- SCRAM Authentication
- Event Shape And Releases
- Pool Internals And Recovery
- Proxy Session Startup
- ClickHouse Pool Tests
- Test Database Script
- Benchmarks
- MySQL CDC Subscriber
- MySQL CDC Integration Tests
- Proxy Backend Driver
- Pooling Contract Notes
- Vendor Config And Errors
- sqldriver Pool Tests
- Library Scope
- Fake database/sql Driver
- MySQL Binlog Stream
- Rows, Row And Batch Results
- Pooling Core Engine
- Proxy Integration Tests
- database/sql Vendor Notes
- Prepared Statement Replay
- Rows And Row Unit Tests
- Multi-Database Architecture
- pgx Rows Wrapper
- Fake Connector Fixtures
- MySQL Column Cache
- Transaction Wrapper
- Core Driver Interface
- Proxy Entry Point
- Byte Relay
- Idle Connection Shards
- Permit Accounting
- Stat Accessors
- MySQL Table Filter
- Proxy Package Imports
- Command Result
- PgBouncer Stacking Tests
- LISTEN/NOTIFY Capability
- Connection Wrapper
- MySQL Position Tracker
- SQL Server CDC Config
- Bulk Copy Capability
- Permit Unit Tests
- MySQL Position Tests
- PostgreSQL Pool Config
- pgx Transaction Methods
- Graph Reconciliation Script
- Coarse Clock
- Acquisition Handle
- Transaction Unit Tests
- MySQL Binlog Syncer Config
- Stat Interface Composition
- pgx Command Result
- pgx Single Row
- MySQL Pool Config
- SQL Server Pool Config
- ClickHouse Pool Config
- Idle Connection Expiry
- Resizable Capability
- Stat Projection

## God Nodes (most connected - your core abstractions)
1. `gpool - Agent & Developer Profiles` - 52 edges
2. `session` - 40 edges
3. `newPool()` - 34 edges
4. `Proxy` - 30 edges
5. `newTestCore()` - 30 edges
6. `Pool Internals` - 29 edges
7. `NewPool()` - 28 edges
8. `Postgres` - 27 edges
9. `Rows` - 25 edges
10. `pgEventStream` - 25 edges

## Surprising Connections (you probably didn't know these)
- `An Unknown User Runs the Full Exchange Against a Decoy Verifier` --semantically_similar_to--> `Errors Are Classified by Code, Never by Message Text`  [AMBIGUOUS] [semantically similar]
  .serena/memories/proxy.md → AGENTS.md
- `One Interface Per File, Rule of Thumb 7 Methods` --semantically_similar_to--> `Interface Segregation: 7 Methods, Assembled by Composition`  [INFERRED] [semantically similar]
  .junie/guidelines.md → AGENTS.md
- `Post-Task Cleanup and Knowledge Update` --semantically_similar_to--> `Post-Task Maintenance Order`  [INFERRED] [semantically similar]
  .junie/guidelines.md → AGENTS.md
- `Capture Mode 'all update old', Not 'all'` --semantically_similar_to--> `REPLICA IDENTITY Rules`  [INFERRED] [semantically similar]
  .serena/memories/cdc_mssql.md → AGENTS.md
- `Clone the GTID set: BinlogSyncer retains and mutates it` --semantically_similar_to--> `cdc.Event`  [INFERRED] [semantically similar]
  .serena/memories/cdc_mysql.md → README.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Guards Against A Test That Passes By Not Running** — _serena_memories_testing_missing_dsn_fails, _serena_memories_testing_collect_closes_the_stream, _serena_memories_testing_unit_tests_cannot_see_driver_state, changelog_cdc_sentinel_coverage [INFERRED 0.85]
- **CI Is Five Jobs Over Six Modules** — _github_workflows_ci_check, _github_workflows_ci_unit, _github_workflows_ci_integration, _github_workflows_ci_vuln, _github_workflows_ci_consumer [EXTRACTED 1.00]
- **The Resume Contract Is Position, Transaction, Timestamp And SubscribeFrom** — _serena_memories_cdc_event_position, _serena_memories_cdc_event_transaction, _serena_memories_cdc_event_timestamp, _serena_memories_cdc_subscribefrom, _serena_memories_cdc_resume_contract [EXTRACTED 1.00]
- **Cross-Application Pooling Needs a Process, and Proves the Engine Is Vendor-Agnostic** — _serena_memories_proxy_cross_application_bound, _serena_memories_proxy_generality_proof, examples_gpoolproxy_readme_vendor_agnostic_proof, readme_cross_application_pooling, changelog_pkg_pooling_added, _serena_memories_architecture_gpoolproxy [INFERRED 0.85]
- **Token Channel Permit Set Replacing the Counting Semaphore** — _serena_memories_scale_semaphore_weighted_bottleneck, _serena_memories_scale_token_channel_permits, _serena_memories_pool_permits, _junie_plans_gpool_lib_init_token_channel_replacement, _serena_memories_scale_dropped_x_sync [INFERRED 0.85]
- **The Release Lineage From v0.1.0 To v1.0.1** — changelog_v0_1_0, changelog_v0_3_0, changelog_v0_4_0, changelog_v0_5_0, changelog_v1_0_0, changelog_v1_0_1 [EXTRACTED 1.00]
- **Documented As Supported While Never Having Been Run** — changelog_fixed_mariadb_never_run, changelog_fixed_mssql_never_run, readme_what_has_been_tested, _github_workflows_ci_integration [EXTRACTED 1.00]

## Communities (84 total, 7 thin omitted)

### Community 0 - "PostgreSQL Integration Fixtures"
Cohesion: 0.06
Nodes (79): CopyRows, sliceRows, Pool, T, scratchTable(), TestCopyFromLoadsRows(), TestCopyFromRollsBackOnSourceError(), TestCopyFromValidatesTheRequest() (+71 more)

### Community 1 - "PostgreSQL CDC Subscriber"
Cohesion: 0.07
Nodes (52): Postgres, math, firstErr(), Config, T, longSlotName(), TestClosedSubscriberRefusesWork(), TestConfigDefaults() (+44 more)

### Community 2 - "CDC Design Notes"
Cohesion: 0.04
Nodes (76): SQL Externalization to .sql + go:embed, Compile-Time Interface Proofs, vendors/mssql/cdc: a Package Inside the Pool Vendor's Module, catchUp Advances A Quiet Slot, CDC Internals, checkResumable Refuses A Silent Rewind, ErrNoCDCSupport, flushed Is What Releases WAL (+68 more)

### Community 3 - "PostgreSQL Logical Decoding Stream"
Cohesion: 0.05
Nodes (51): BackendMessage, Event, Op, pendingEvent, pgEventStream, transaction, DeleteMessage, fakeStream (+43 more)

### Community 4 - "SQL Server CDC Polling"
Cohesion: 0.06
Nodes (40): captureInstance, change, sqlEventStream, SQLServer, Config, Duration, describe(), Time (+32 more)

### Community 5 - "Vendor Factory And Godoc Examples"
Cohesion: 0.07
Nodes (58): BenchmarkGpoolAcquireRelease(), BenchmarkGpoolQueryIterator(), BenchmarkGpoolQueryRow(), BenchmarkGpoolQueryRowStress(), BenchmarkGpoolResetQuery(), B, Config, Pool (+50 more)

### Community 6 - "Pooling Core Unit Tests"
Cohesion: 0.09
Nodes (50): New(), Bool, Config, Context, Int32, Mutex, T, newTestCore() (+42 more)

### Community 7 - "database/sql Pool Adapter"
Cohesion: 0.06
Nodes (19): pgRows, Context, Stmt, Context, newConnWrapper(), Connector, connWrapper, Context (+11 more)

### Community 8 - "Batch And Bulk Copy Mocks"
Cohesion: 0.05
Nodes (23): CopyFromSource, FieldDescription, Batch, BatchQuery, BatchResults, Identifier, LargeObjects, T (+15 more)

### Community 9 - "Value Conversion And Scanning"
Cohesion: 0.08
Nodes (43): reflect, errArity(), Bool, connWrapper, Seq, Value, newRows(), scanInto() (+35 more)

### Community 10 - "Engine And Multi-Database Tests"
Cohesion: 0.08
Nodes (35): Stream, Subscriber, TableManager, net/url, Engine, databaseNamed(), Config, T (+27 more)

### Community 11 - "Scale And Footprint Notes"
Cohesion: 0.07
Nodes (48): No Memory Leaks, Bounded Statement and Description Caches, Dropped golang.org/x/sync, Step 12: Scale and Footprint, Token Channel Replaces Counting Semaphore, ActiveConnections Is an Exact Count, Per-Connection Caches Are the Memory Cost, maintain: the One Background Goroutine (+40 more)

### Community 12 - "CDC Mocks And Fixtures"
Cohesion: 0.09
Nodes (25): EventStream, github.com/gsoultan/gpool/vendors/mssql/cdc, fakePool, fakeSubscriber, Context, Int32, Stat, collect() (+17 more)

### Community 13 - "Module Dependency Graph"
Cohesion: 0.06
Nodes (45): filippo/io/edwards25519, github.com/andybalholm/brotli, github.com/cespare/xxhash/v2, github.com/clickhouse/ch/go, github.com/clickhouse/clickhouse/go/v2, github.com/coreos/go/semver, github.com/go/faster/city, github.com/go/faster/errors (+37 more)

### Community 14 - "PostgreSQL Pool And Driver"
Cohesion: 0.07
Nodes (29): newConnWrapper(), closeConn(), Config, ConnConfig, Context, Pool, init(), newFromConfig() (+21 more)

### Community 15 - "Cross-Application Proxy Notes"
Cohesion: 0.07
Nodes (42): examples/gpoolproxy as a Separate Module, emit Keeps Keepalives Flowing While Blocked, CDC Cannot Be Pooled, Cancellation Keys Are Per Session and Unguessable, A Library Cannot Bound Connections Across Applications, s.expect Swallows the Injected ParseComplete, The Generality Proof for pkg/pooling, gpoolproxy - Cross-Application Pooling (+34 more)

### Community 16 - "CDC Package Imports"
Cohesion: 0.10
Nodes (22): database/sql, encoding/hex, fmt, github.com/go/mysql/org/go/mysql/mysql, github.com/go/mysql/org/go/mysql/replication, github.com/go/sql/driver/mysql, github.com/gsoultan/gpool/pkg/gpool/cdc, github.com/gsoultan/gpool/vendors/mssql (+14 more)

### Community 17 - "Core Package Imports"
Cohesion: 0.09
Nodes (11): ReplicationManager, context, database/sql/driver, github.com/gsoultan/gpool/pkg/gpool, github.com/gsoultan/gpool/pkg/pooling, github.com/jackc/pgx/v5, sync/atomic, Batcher (+3 more)

### Community 18 - "Junie Agent Guidelines"
Cohesion: 0.07
Nodes (39): No AI Co-Authorship Trailers, Mandatory Copyright Header, Modern Go 1.26 Syntax, Graphify Knowledge Graph, Gpool Project Guidelines (.junie), Hierarchical Discovery Before a Level 4 Full Read, Layered Architecture Pattern, Obsidian Vault as Agentic Second Brain (+31 more)

### Community 19 - "SQL Server Pool Tests"
Cohesion: 0.11
Nodes (34): github.com/gsoultan/gpool/pkg/sqldriver, github.com/microsoft/go/mssqldb, Config, dsn(), Config, Pool, T, newPool() (+26 more)

### Community 20 - "MySQL Integration And Failure Tests"
Cohesion: 0.15
Nodes (34): github.com/gsoultan/gpool/vendors/mysql, target, target, init(), T, killConnections(), TestPoolRecoversAfterConnectionsAreKilled(), TestPoolReplacesItsOnlyConnectionWhenKilled() (+26 more)

### Community 21 - "Project Invariants"
Cohesion: 0.08
Nodes (35): The run Defer Order Is Load-Bearing, Vendor Registry (Only Process-Global State), Every Drain Is Bounded, Validate and Default Config at Construction, Idempotent Teardown, No Callback Invoked Under Its Own Lock, No Panic Reaches the Caller, Invariants (Memory) (+27 more)

### Community 22 - "Proxy Listener And Userlist"
Cohesion: 0.08
Nodes (21): Addr, clientTLS(), Bool, CancelFunc, Config, Context, Mutex, Stat (+13 more)

### Community 23 - "Testing And CI"
Cohesion: 0.09
Nodes (34): CI Workflow, Format And Vet Job, Installable From The Module Proxy Job, Fail If The Databases Were Never Reachable, gofmt Ignores graphify-out, Every Module Vetted And Tested Separately, testdbs.sh Is The One Database Definition, Unit Tests Job (+26 more)

### Community 24 - "SCRAM Authentication"
Cohesion: 0.14
Nodes (26): field(), splitGS2(), exchange(), T, parseServerFirst(), TestSCRAMAcceptsTheRightPassword(), TestSCRAMRejectsAlteredChannelBinding(), TestSCRAMRejectsRequiredChannelBinding() (+18 more)

### Community 25 - "Event Shape And Releases"
Cohesion: 0.11
Nodes (30): Integration Job (Five Engines), VendorFactory Pattern, Vendor Registry and init() Self-Registration, Event.Position Is An Opaque String, Not A Number, Event.Timestamp Is The Source's Commit Time, Event.Transaction Groups One Commit, Clone the GTID set: BinlogSyncer retains and mutates it, MySQL values keep the binlog parser's native Go types (+22 more)

### Community 26 - "Pool Internals And Recovery"
Cohesion: 0.10
Nodes (30): EvictIdle, LISTEN Is Session State the Transaction Gate Cannot See, Driver.NeedsCleanup Exists to Keep Release Cheap, Only the Blocking Path Is Timed, BulkCopier, Batcher, Notifier, pgConn as a Type Parameter, Not an Interface, Pool Internals, recyclable(): the Release Gate (+22 more)

### Community 27 - "Proxy Session Startup"
Cohesion: 0.13
Nodes (9): newRelay(), cutCString(), CancelFunc, Context, Mutex, Reader, Writer, parameterOf() (+1 more)

### Community 28 - "ClickHouse Pool Tests"
Cohesion: 0.15
Nodes (25): Config, github.com/gsoultan/gpool/vendors/clickhouse, Options, Connector, Duration, Pool, New(), newFromConfig() (+17 more)

### Community 29 - "Test Database Script"
Cohesion: 0.14
Nodes (23): ALL_ENGINES, CH_PASSWORD, cmd_down(), cmd_env(), cmd_status(), cmd_up(), die(), dsn_of() (+15 more)

### Community 30 - "Benchmarks"
Cohesion: 0.12
Nodes (22): BenchmarkPgBouncer(), BenchmarkPgxPool(), BenchmarkPgxPoolStress(), BenchmarkStdlib(), BenchmarkStdlibStress(), B, benchmarkTarget(), BenchmarkThroughput() (+14 more)

### Community 31 - "MySQL CDC Subscriber"
Cohesion: 0.16
Nodes (9): MySQL, Position, BinlogStreamer, BinlogSyncer, Context, DB, GTIDSet, Mutex (+1 more)

### Community 32 - "MySQL CDC Integration Tests"
Cohesion: 0.30
Nodes (22): target, github.com/gsoultan/gpool/vendors/mysql/cdc, collect(), eachTarget(), fixture, Config, DB, Duration (+14 more)

### Community 33 - "Proxy Backend Driver"
Cohesion: 0.12
Nodes (11): Bool, Context, Reader, Writer, Config, Context, networkAddress(), Conn (+3 more)

### Community 34 - "Pooling Contract Notes"
Cohesion: 0.10
Nodes (25): sync.Pool Restricted to Non-Escaping Buffers, Zero-Allocation Hot Paths, CDC Control and Replication Connection Split, Integrated CDC via Logical Replication, Gpool Library Initialization Plan, Go 1.26 Iterator API, PgBouncer Replacement, Step 11: Production Hardening (+17 more)

### Community 35 - "Vendor Config And Errors"
Cohesion: 0.09
Nodes (7): errors, github.com/clickhouse/clickhouse/go/v2, math/rand/v2, runtime, Config, Duration, Config

### Community 36 - "sqldriver Pool Tests"
Cohesion: 0.22
Nodes (22): Config, New(), Config, Pool, T, newTestPool(), TestAbandonedTransactionIsUnwoundOnRelease(), TestAcquireAfterCloseFailsFast() (+14 more)

### Community 37 - "Library Scope"
Cohesion: 0.13
Nodes (22): Gpool Is a Library, Not a Service, The /internal Rule Is Inverted for gpool, CLI Proxy Mode, Step 4: Wire Proxy and CLI Removed, Interfaces in pkg/gpool, Implementations in pkg/vendors, Gpool Core Memory, Library-Only Go Module, Serena Memory Index (+14 more)

### Community 38 - "Fake database/sql Driver"
Cohesion: 0.13
Nodes (7): Bool, Context, NamedValue, Stmt, fakeDriverConn, fakeTx, TxOptions

### Community 39 - "MySQL Binlog Stream"
Cohesion: 0.14
Nodes (14): BinlogEvent, mysqlEventStream, EventType, opOf(), BinlogStreamer, BinlogSyncer, Bool, CancelFunc (+6 more)

### Community 40 - "Rows, Row And Batch Results"
Cohesion: 0.14
Nodes (8): Row, Rows, Bool, closeRows(), Context, batchResults, failedBatchResults, rowCursor

### Community 41 - "Pooling Core Engine"
Cohesion: 0.25
Nodes (3): C, Context, Core[C]

### Community 42 - "Proxy Integration Tests"
Cohesion: 0.29
Nodes (19): cachingURL(), connect(), Config, T, startProxy(), TestProxyBoundsBackendsAcrossIndependentClients(), TestProxyForgetsClosedStatements(), TestProxyIsolatesIdenticallyNamedStatements() (+11 more)

### Community 43 - "database/sql Vendor Notes"
Cohesion: 0.14
Nodes (19): SQL Server Runs Natively On x86-64 Runners, Adding a Vendor Needs Zero Edits to pkg/gpool, The Transaction Gate Is the Pooling Contract, ClickHouse: analytical column store, no general transactions, database/sql vendor: about a hundred lines, driver.Value is not a closed set, SQL Server: ordinal placeholders, lenient DSN parser, MySQL and MariaDB: one implementation, two names (+11 more)

### Community 44 - "Prepared Statement Replay"
Cohesion: 0.22
Nodes (9): bindName(), closeStatement(), cString(), cutBytes(), parseName(), statementNameOf(), targetName(), bytes (+1 more)

### Community 45 - "Rows And Row Unit Tests"
Cohesion: 0.29
Nodes (14): newRow(), connWrapper, newRows(), T, TestResultReportsRowsAffected(), TestRowReleaseWithoutScanClosesTheResultSet(), TestRowsAllClosesOnEarlyBreak(), TestRowScanAfterReleaseIsRefused() (+6 more)

### Community 46 - "Multi-Database Architecture"
Cohesion: 0.21
Nodes (14): Avoid Stuttering in Filenames and Symbols, Multi-Database Engine Pool Registry, Step 10: Multi-Database Support, Independent Replication Slot per Node, Pool(name) Naming Decision, Architecture, Engine: Named Registries of Pools and Subscribers, One Struct or Interface Per File; No Stuttering (+6 more)

### Community 47 - "pgx Rows Wrapper"
Cohesion: 0.16
Nodes (4): Field, Bool, Seq, pgRows

### Community 48 - "Fake Connector Fixtures"
Cohesion: 0.21
Nodes (6): Int32, Value, bareConn, fakeConnector, fakeRows, fakeStmt

### Community 49 - "MySQL Column Cache"
Cohesion: 0.24
Nodes (7): columns, TableMapEvent, Context, DB, Mutex, newColumns(), qualify()

### Community 50 - "Transaction Wrapper"
Cohesion: 0.23
Nodes (6): Tx, Bool, connWrapper, Context, newTx(), pgTx

### Community 51 - "Core Driver Interface"
Cohesion: 0.17
Nodes (10): Bool, CancelFunc, Config, Int32, Int64, Mutex, Once, Core (+2 more)

### Community 52 - "Proxy Entry Point"
Cohesion: 0.20
Nodes (10): Config, hash(), main(), parseFlags(), run(), flag, io/fs, os (+2 more)

### Community 53 - "Byte Relay"
Cohesion: 0.30
Nodes (6): endsTransactionUnit(), flushIfDrained(), Reader, Writer, namesStatement(), relay

### Community 54 - "Idle Connection Shards"
Cohesion: 0.27
Nodes (7): C, C, Int32, Mutex, idleConn, shard, shard[C]

### Community 55 - "Permit Accounting"
Cohesion: 0.26
Nodes (3): Context, Int32, permits

### Community 57 - "MySQL Table Filter"
Cohesion: 0.27
Nodes (5): filter, slices, RWMutex, newFilter(), normalize()

### Community 58 - "Proxy Package Imports"
Cohesion: 0.35
Nodes (6): bufio, crypto/tls, encoding/binary, github.com/jackc/pgx/v5/pgproto3, io, net

### Community 60 - "PgBouncer Stacking Tests"
Cohesion: 0.45
Nodes (10): Config, Pool, T, hammer(), newPooledPool(), pgBouncerURL(), TestPgBouncerStatDescribesTheProxyHop(), TestPgBouncerTransactionsAreAtomic() (+2 more)

### Community 61 - "LISTEN/NOTIFY Capability"
Cohesion: 0.29
Nodes (5): Notification, Notifier, Context, connWrapper, quoteIdentifier()

### Community 63 - "MySQL Position Tracker"
Cohesion: 0.31
Nodes (5): tracker, GTIDSet, newTracker(), position, GTIDSet

### Community 64 - "SQL Server CDC Config"
Cohesion: 0.22
Nodes (4): regexp, Config, Duration, changesSQL()

### Community 65 - "Bulk Copy Capability"
Cohesion: 0.31
Nodes (6): BulkCopier, CopyRequest, Context, connWrapper, Postgres, validateCopyRequest()

### Community 66 - "Permit Unit Tests"
Cohesion: 0.50
Nodes (8): newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate(), TestPermitsHoldTheBoundUnderContention(), TestPermitsSurvivesOverRelease()

### Community 67 - "MySQL Position Tests"
Cohesion: 0.44
Nodes (8): parsePosition(), T, TestFilterMatching(), TestFilterMutation(), TestParsePositionIsFlavourSpecific(), TestParsePositionRejectsAForeignPosition(), TestPositionDistinguishesItsTwoNotations(), TestPositionRoundTrips()

### Community 68 - "PostgreSQL Pool Config"
Cohesion: 0.29
Nodes (4): cacheCapacity(), ConnConfig, Duration, Config

### Community 69 - "pgx Transaction Methods"
Cohesion: 0.43
Nodes (3): Bool, Context, pgTx

### Community 70 - "Graph Reconciliation Script"
Cohesion: 0.43
Nodes (6): is_package(), main(), package_label(), Recover a readable import path from a synthesized package id.      The id is los, Return the extraction with package nodes added and unresolvable edges dropped., reconcile()

### Community 71 - "Coarse Clock"
Cohesion: 0.38
Nodes (4): Int64, Time, newCoarseClock(), coarseClock

### Community 72 - "Acquisition Handle"
Cohesion: 0.29
Nodes (3): C, Handle, Handle[C]

### Community 73 - "Transaction Unit Tests"
Cohesion: 0.57
Nodes (6): newTx(), T, TestTxCommitWithDeferredRollback(), TestTxRefusesUseAfterSettle(), TestTxRollbackWithDeferredRollback(), TestTxSettlesExactlyOnce()

### Community 74 - "MySQL Binlog Syncer Config"
Cohesion: 0.33
Nodes (5): BinlogSyncerConfig, newFromConfig(), Config, New(), splitHostPort()

### Community 75 - "Stat Interface Composition"
Cohesion: 0.33
Nodes (3): Acquisition, Occupancy, Stat

### Community 77 - "pgx Single Row"
Cohesion: 0.47
Nodes (3): Bool, connWrapper, pgRow

### Community 79 - "SQL Server Pool Config"
Cohesion: 0.50
Nodes (3): Connector, Duration, Config

### Community 80 - "ClickHouse Pool Config"
Cohesion: 0.50
Nodes (3): Config, Connector, Duration

### Community 81 - "Idle Connection Expiry"
Cohesion: 0.50
Nodes (3): Duration, Time, idleConn[C]

## Ambiguous Edges - Review These
- `CLI Proxy Mode` → `Gpool: A Go Connection Pooling & CDC Library`  [AMBIGUOUS]
  .junie/plans/gpool-lib-init.md · relation: conceptually_related_to
- `Gpool Core Memory` → `Supported Databases`  [AMBIGUOUS]
  .serena/memories/core.md · relation: conceptually_related_to
- `An Unknown User Runs the Full Exchange Against a Decoy Verifier` → `Errors Are Classified by Code, Never by Message Text`  [AMBIGUOUS]
  .serena/memories/proxy.md · relation: semantically_similar_to

## Knowledge Gaps
- **40 isolated node(s):** `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD`, `CH_PASSWORD`, `MSSQL_PASSWORD` (+35 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `CLI Proxy Mode` and `Gpool: A Go Connection Pooling & CDC Library`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Gpool Core Memory` and `Supported Databases`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `An Unknown User Runs the Full Exchange Against a Decoy Verifier` and `Errors Are Classified by Code, Never by Message Text`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._
- **Why does `Proxy` connect `Proxy Listener And Userlist` to `Proxy Backend Driver`, `Core Driver Interface`, `SCRAM Authentication`, `Proxy Package Imports`, `Proxy Session Startup`?**
  _High betweenness centrality (0.033) - this node is a cross-community bridge._
- **Why does `MySQL` connect `MySQL CDC Subscriber` to `MySQL Binlog Stream`, `MySQL Binlog Syncer Config`, `CDC Package Imports`, `MySQL Column Cache`, `MySQL Table Filter`?**
  _High betweenness centrality (0.032) - this node is a cross-community bridge._
- **Why does `session` connect `Proxy Session Startup` to `Proxy Backend Driver`, `Acquisition Handle`, `Prepared Statement Replay`, `Byte Relay`, `Proxy Listener And Userlist`, `Proxy Package Imports`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **What connects `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._