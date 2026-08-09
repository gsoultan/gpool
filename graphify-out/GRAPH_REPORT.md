# Graph Report - .  (2026-08-09)

## Corpus Check
- 184 files · ~138,890 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2178 nodes · 5124 edges · 93 communities (85 shown, 8 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 616 edges (avg confidence: 0.83)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- PostgreSQL Integration Fixtures
- PostgreSQL CDC Subscriber
- CDC Design Notes
- Engine And Multi-Database Tests
- PostgreSQL Logical Decoding Stream
- Pooling Core Unit Tests
- SQL Server CDC Polling
- database/sql Pool Adapter
- Batch And Bulk Copy Mocks
- Value Conversion And Scanning
- Project Invariants
- CDC Integration Fixtures
- Module Dependency Graph
- Junie Agent Guidelines
- Vendor Factory And Godoc Examples
- Testing And CI
- Core Package Imports
- Benchmarks
- CDC Package Imports
- Pool Internals And Lifecycle
- SCRAM Authentication
- SQL Server Pool Tests
- MySQL Integration And Failure Tests
- Library Scope And Plan
- Vendor Config And Imports
- Releases And Event Shape
- Proxy Session Startup
- Scale Benchmarks
- ClickHouse Pool Tests
- Test Database Script
- MySQL CDC Subscriber
- Proxy Listener And Warm-Up
- Pooling Core Engine
- Fake database/sql Driver
- sqldriver Pool Tests
- Proxy Integration Tests
- MySQL Binlog Stream
- Proxy Backend Driver
- Multi-Database Architecture
- Proxy Startup Parameters
- PostgreSQL Pool Tests
- database/sql Vendor Notes
- Per-Connection Memory
- Prepared Statement Bound
- pgx Rows Wrapper
- Rows And Row Unit Tests
- Stat Accessors
- Fake Connector Fixtures
- pgx Driver Adapter
- PostgreSQL Pool Facade
- Proxy Package Imports
- Byte Relay
- Prepared Statement Replay
- Proxy Connection Config
- Transaction Wrapper
- Core Driver Interface
- Cross-Application Pooling Notes
- MySQL Table Filter
- Command Result
- Idle Connection Shards
- Permit Accounting
- Benchmark Hygiene
- MySQL Column Cache
- Proxy Userlist And TLS
- Prepared Statement Bound Tests
- pgx Transaction Methods
- LISTEN/NOTIFY Capability
- Rows And Batch Results
- Permit Unit Tests
- Connection Wrapper
- Token Channel Permits
- MySQL Position Tracker
- SQL Server CDC Config
- Bulk Copy Capability
- MySQL Position Tests
- Proxy Entry Point
- Acquisition Handle
- Vendor-Agnostic Engine Release
- Bounded Statement Set
- Graph Reconciliation Script
- Coarse Clock
- Transaction Unit Tests
- Footprint At Scale
- MySQL Binlog Syncer Config
- Stat Interface Composition
- pgx Command Result
- pgx Single Row
- MySQL Pool Config
- SQL Server Pool Config
- Idle Connection Expiry
- Deferred Error Row
- Resizable Capability
- Stat Projection

## God Nodes (most connected - your core abstractions)
1. `gpool - Agent & Developer Profiles` - 52 edges
2. `session` - 41 edges
3. `newTestCore()` - 35 edges
4. `newPool()` - 34 edges
5. `Proxy` - 31 edges
6. `Pool Internals` - 30 edges
7. `NewPool()` - 29 edges
8. `Postgres` - 27 edges
9. `gpoolproxy - Cross-Application Pooling` - 27 edges
10. `backend` - 26 edges

## Surprising Connections (you probably didn't know these)
- `An Unknown User Runs the Full Exchange Against a Decoy Verifier` --semantically_similar_to--> `Errors Are Classified by Code, Never by Message Text`  [AMBIGUOUS] [semantically similar]
  .serena/memories/proxy.md → AGENTS.md
- `Clone the GTID set: BinlogSyncer retains and mutates it` --semantically_similar_to--> `cdc.Event`  [INFERRED] [semantically similar]
  .serena/memories/cdc_mysql.md → README.md
- `One Interface Per File, Rule of Thumb 7 Methods` --semantically_similar_to--> `Interface Segregation: 7 Methods, Assembled by Composition`  [INFERRED] [semantically similar]
  .junie/guidelines.md → AGENTS.md
- `Post-Task Cleanup and Knowledge Update` --semantically_similar_to--> `Post-Task Maintenance Order`  [INFERRED] [semantically similar]
  .junie/guidelines.md → AGENTS.md
- `Capture Mode 'all update old', Not 'all'` --semantically_similar_to--> `REPLICA IDENTITY Rules`  [INFERRED] [semantically similar]
  .serena/memories/cdc_mssql.md → AGENTS.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **The Same Failover Counted From Both Sides** — readme_lifecycle_alerting, _serena_memories_testing_recovery_contract, _serena_memories_pool_lifecycle_counters, _serena_memories_testing_failure_injection [EXTRACTED 1.00]
- **CI Is Five Jobs Over Six Modules** — _github_workflows_ci_check, _github_workflows_ci_unit, _github_workflows_ci_integration, _github_workflows_ci_vuln, _github_workflows_ci_consumer [EXTRACTED 1.00]
- **The Resume Contract Is Position, Transaction, Timestamp And SubscribeFrom** — _serena_memories_cdc_event_position, _serena_memories_cdc_event_transaction, _serena_memories_cdc_event_timestamp, _serena_memories_cdc_subscribefrom, _serena_memories_cdc_resume_contract [EXTRACTED 1.00]
- **Sets A Client Controls The Size Of** — _serena_memories_proxy_bounded_statements, _serena_memories_proxy_buffer_sizing, _serena_memories_proxy_cross_application_bound, agents_bound_what_multiplies [INFERRED 0.85]
- **Token Channel Permit Set Replacing the Counting Semaphore** — _serena_memories_scale_semaphore_weighted_bottleneck, _serena_memories_scale_token_channel_permits, _serena_memories_pool_permits, _junie_plans_gpool_lib_init_token_channel_replacement, _serena_memories_scale_dropped_x_sync [INFERRED 0.85]

## Communities (93 total, 8 thin omitted)

### Community 0 - "PostgreSQL Integration Fixtures"
Cohesion: 0.06
Nodes (81): github.com/gsoultan/gpool/pkg/vendors/postgres/pool, CopyRows, sliceRows, Pool, T, scratchTable(), TestCopyFromLoadsRows(), TestCopyFromRollsBackOnSourceError() (+73 more)

### Community 1 - "PostgreSQL CDC Subscriber"
Cohesion: 0.07
Nodes (53): Postgres, math, slices, firstErr(), Config, T, longSlotName(), TestClosedSubscriberRefusesWork() (+45 more)

### Community 2 - "CDC Design Notes"
Cohesion: 0.05
Nodes (75): SQL Externalization to .sql + go:embed, CDC Control and Replication Connection Split, Step 11: Production Hardening, vendors/mssql/cdc: a Package Inside the Pool Vendor's Module, catchUp Advances A Quiet Slot, CDC Internals, checkResumable Refuses A Silent Rewind, flushed Is What Releases WAL (+67 more)

### Community 3 - "Engine And Multi-Database Tests"
Cohesion: 0.06
Nodes (55): Stream, Subscriber, TableManager, github.com/gsoultan/gpool/vendors/mssql/cdc, net/url, Engine, databaseNamed(), Config (+47 more)

### Community 4 - "PostgreSQL Logical Decoding Stream"
Cohesion: 0.05
Nodes (51): BackendMessage, Event, Op, pendingEvent, pgEventStream, transaction, DeleteMessage, fakeStream (+43 more)

### Community 5 - "Pooling Core Unit Tests"
Cohesion: 0.07
Nodes (59): Lifecycle, New(), Bool, Config, Context, Int32, Mutex, T (+51 more)

### Community 6 - "SQL Server CDC Polling"
Cohesion: 0.06
Nodes (40): captureInstance, change, sqlEventStream, SQLServer, Config, Duration, describe(), Time (+32 more)

### Community 7 - "database/sql Pool Adapter"
Cohesion: 0.06
Nodes (19): pgRows, Context, Stmt, Context, newConnWrapper(), Connector, connWrapper, Context (+11 more)

### Community 8 - "Batch And Bulk Copy Mocks"
Cohesion: 0.05
Nodes (23): CopyFromSource, FieldDescription, Batch, BatchQuery, BatchResults, Identifier, LargeObjects, T (+15 more)

### Community 9 - "Value Conversion And Scanning"
Cohesion: 0.08
Nodes (43): reflect, errArity(), Bool, connWrapper, Seq, Value, newRows(), scanInto() (+35 more)

### Community 10 - "Project Invariants"
Cohesion: 0.06
Nodes (51): sync.Pool Restricted to Non-Escaping Buffers, Zero-Allocation Hot Paths, TDD Mandatory, emit Keeps Keepalives Flowing While Blocked, Every Drain Is Bounded, Validate and Default Config at Construction, Idempotent Teardown, No Callback Invoked Under Its Own Lock (+43 more)

### Community 11 - "CDC Integration Fixtures"
Cohesion: 0.11
Nodes (28): EventStream, target, github.com/gsoultan/gpool/vendors/mysql/cdc, fakePool, fakeSubscriber, Context, Int32, Stat (+20 more)

### Community 12 - "Module Dependency Graph"
Cohesion: 0.06
Nodes (45): filippo/io/edwards25519, github.com/andybalholm/brotli, github.com/cespare/xxhash/v2, github.com/clickhouse/ch/go, github.com/clickhouse/clickhouse/go/v2, github.com/coreos/go/semver, github.com/go/faster/city, github.com/go/faster/errors (+37 more)

### Community 13 - "Junie Agent Guidelines"
Cohesion: 0.06
Nodes (42): No AI Co-Authorship Trailers, Mandatory Copyright Header, Modern Go 1.26 Syntax, Graphify Knowledge Graph, Gpool Project Guidelines (.junie), Hierarchical Discovery Before a Level 4 Full Read, The /internal Rule Is Inverted for gpool, Layered Architecture Pattern (+34 more)

### Community 14 - "Vendor Factory And Godoc Examples"
Cohesion: 0.10
Nodes (39): github.com/gsoultan/gpool/pkg/vendors/postgres/cdc, log, PoolFactory, SubscriberFactory, Vendor, apply(), Example_resumingFromACheckpoint(), Example_slotAdministration() (+31 more)

### Community 15 - "Testing And CI"
Cohesion: 0.09
Nodes (39): CI Workflow, Format And Vet Job, Installable From The Module Proxy Job, Fail If The Databases Were Never Reachable, gofmt Ignores graphify-out, Integration Job (Five Engines), Every Module Vetted And Tested Separately, testdbs.sh Is The One Database Definition (+31 more)

### Community 16 - "Core Package Imports"
Cohesion: 0.11
Nodes (12): ReplicationManager, context, database/sql/driver, github.com/gsoultan/gpool/pkg/gpool, github.com/gsoultan/gpool/pkg/pooling, github.com/jackc/pgx/v5, iter, math/rand/v2 (+4 more)

### Community 17 - "Benchmarks"
Cohesion: 0.10
Nodes (33): BenchmarkPgBouncer(), BenchmarkPgxPool(), BenchmarkPgxPoolStress(), BenchmarkStdlib(), BenchmarkStdlibStress(), B, benchmarkTarget(), BenchmarkThroughput() (+25 more)

### Community 18 - "CDC Package Imports"
Cohesion: 0.11
Nodes (15): database/sql, encoding/hex, fmt, github.com/go/mysql/org/go/mysql/mysql, github.com/go/mysql/org/go/mysql/replication, github.com/go/sql/driver/mysql, github.com/gsoultan/gpool/pkg/gpool/cdc, github.com/jackc/pglogrepl (+7 more)

### Community 19 - "Pool Internals And Lifecycle"
Cohesion: 0.10
Nodes (35): A Lifetime Below clockResolution Is Invisible, Not Quick, Every destroy Carries A discard Reason, EvictIdle, whyUnusable Checks Ill Health First, Lifecycle Counters Say Why Connections Are Replaced, LISTEN Is Session State the Transaction Gate Cannot See, maintain: the One Background Goroutine, Driver.NeedsCleanup Exists to Keep Release Cheap (+27 more)

### Community 20 - "SCRAM Authentication"
Cohesion: 0.14
Nodes (27): hash(), field(), splitGS2(), exchange(), T, parseServerFirst(), TestSCRAMAcceptsTheRightPassword(), TestSCRAMRejectsAlteredChannelBinding() (+19 more)

### Community 21 - "SQL Server Pool Tests"
Cohesion: 0.13
Nodes (32): github.com/gsoultan/gpool/vendors/mssql, Config, dsn(), Config, Pool, T, newPool(), scratchTable() (+24 more)

### Community 22 - "MySQL Integration And Failure Tests"
Cohesion: 0.16
Nodes (33): github.com/gsoultan/gpool/vendors/mysql, target, target, T, killConnections(), TestPoolRecoversAfterConnectionsAreKilled(), TestPoolReplacesItsOnlyConnectionWhenKilled(), TestPoolSurvivesRepeatedKillsUnderLoad() (+25 more)

### Community 23 - "Library Scope And Plan"
Cohesion: 0.08
Nodes (34): Gpool Is a Library, Not a Service, Integrated CDC via Logical Replication, CLI Proxy Mode, Gpool Library Initialization Plan, Go 1.26 Iterator API, PgBouncer Replacement, Step 4: Wire Proxy and CLI Removed, Transaction-Mode Pooling Constraints (+26 more)

### Community 24 - "Vendor Config And Imports"
Cohesion: 0.07
Nodes (12): errors, github.com/clickhouse/clickhouse/go/v2, github.com/gsoultan/gpool/pkg/sqldriver, github.com/microsoft/go/mssqldb, runtime, time, Config, Config (+4 more)

### Community 25 - "Releases And Event Shape"
Cohesion: 0.11
Nodes (29): No Memory Leaks, Event.Position Is An Opaque String, Not A Number, Event.Timestamp Is The Source's Commit Time, Event.Transaction Groups One Commit, Clone the GTID set: BinlogSyncer retains and mutates it, MySQL values keep the binlog parser's native Go types, SHOW BINARY LOG STATUS replaces SHOW MASTER STATUS in 8.4, Tagged positions: gtid:<set> or file:<name>:<offset> (+21 more)

### Community 26 - "Proxy Session Startup"
Cohesion: 0.13
Nodes (9): newRelay(), cutCString(), CancelFunc, Context, Mutex, Reader, Writer, parameterOf() (+1 more)

### Community 27 - "Scale Benchmarks"
Cohesion: 0.15
Nodes (25): BenchmarkGpoolAcquireRelease(), BenchmarkGpoolQueryIterator(), BenchmarkGpoolQueryRow(), BenchmarkGpoolQueryRowStress(), BenchmarkGpoolResetQuery(), B, Config, Pool (+17 more)

### Community 28 - "ClickHouse Pool Tests"
Cohesion: 0.15
Nodes (25): Config, github.com/gsoultan/gpool/vendors/clickhouse, Options, Connector, Duration, Pool, New(), newFromConfig() (+17 more)

### Community 29 - "Test Database Script"
Cohesion: 0.14
Nodes (23): ALL_ENGINES, CH_PASSWORD, cmd_down(), cmd_env(), cmd_status(), cmd_up(), die(), dsn_of() (+15 more)

### Community 30 - "MySQL CDC Subscriber"
Cohesion: 0.16
Nodes (9): MySQL, Position, BinlogStreamer, BinlogSyncer, Context, DB, GTIDSet, Mutex (+1 more)

### Community 31 - "Proxy Listener And Warm-Up"
Cohesion: 0.12
Nodes (12): Addr, Bool, CancelFunc, Context, Mutex, Stat, newCancelKey(), newSession() (+4 more)

### Community 32 - "Pooling Core Engine"
Cohesion: 0.20
Nodes (5): C, Context, Time, Core[C], discard

### Community 33 - "Fake database/sql Driver"
Cohesion: 0.13
Nodes (7): Bool, Context, NamedValue, Stmt, fakeDriverConn, fakeTx, TxOptions

### Community 34 - "sqldriver Pool Tests"
Cohesion: 0.22
Nodes (22): Config, New(), Config, Pool, T, newTestPool(), TestAbandonedTransactionIsUnwoundOnRelease(), TestAcquireAfterCloseFailsFast() (+14 more)

### Community 35 - "Proxy Integration Tests"
Cohesion: 0.29
Nodes (21): cachingURL(), connect(), Config, T, startProxy(), startProxyLimited(), TestProxyBoundsBackendsAcrossIndependentClients(), TestProxyBoundsPreparedStatementsOnTheServer() (+13 more)

### Community 36 - "MySQL Binlog Stream"
Cohesion: 0.14
Nodes (14): BinlogEvent, mysqlEventStream, EventType, opOf(), BinlogStreamer, BinlogSyncer, Bool, CancelFunc (+6 more)

### Community 37 - "Proxy Backend Driver"
Cohesion: 0.15
Nodes (10): Bool, Context, Reader, Writer, Config, Context, networkAddress(), backend (+2 more)

### Community 38 - "Multi-Database Architecture"
Cohesion: 0.14
Nodes (20): Avoid Stuttering in Filenames and Symbols, Multi-Database Engine Pool Registry, Step 10: Multi-Database Support, Independent Replication Slot per Node, Pool(name) Naming Decision, Architecture, Compile-Time Interface Proofs, Engine: Named Registries of Pools and Subscribers (+12 more)

### Community 39 - "Proxy Startup Parameters"
Cohesion: 0.14
Nodes (18): examples/gpoolproxy as a Separate Module, Cancellation Keys Are Per Session and Unguessable, The Generality Proof for pkg/pooling, gpoolproxy - Cross-Application Pooling, Known Gaps, Deliberate, ParameterStatus Values Captured From a Real Backend, A Client That Connects Before The First Backend Gets No ParameterStatus, pending Counts Only Query, Sync and FunctionCall (+10 more)

### Community 40 - "PostgreSQL Pool Tests"
Cohesion: 0.22
Nodes (16): Pool, newFromConfig(), Config, New(), Config, T, newTestPool(), TestAcquireAfterCloseFailsFast() (+8 more)

### Community 41 - "database/sql Vendor Notes"
Cohesion: 0.17
Nodes (16): SQL Server Runs Natively On x86-64 Runners, The Transaction Gate Is the Pooling Contract, ClickHouse: analytical column store, no general transactions, database/sql vendor: about a hundred lines, driver.Value is not a closed set, SQL Server: ordinal placeholders, lenient DSN parser, MySQL and MariaDB: one implementation, two names, pkg/sqldriver pools driver.Conn, not *sql.DB (+8 more)

### Community 42 - "Per-Connection Memory"
Cohesion: 0.20
Nodes (15): Bounded Statement and Description Caches, ActiveConnections Is an Exact Count, Per-Connection Caches Are the Memory Cost, WaitingAcquires Is the Only Gauge Among the Counters, Cache Capacity Default 64, Profile-Driven Optimization, Statement Cache Was 57% of Heap, Profile Before Optimising, and Measure After (+7 more)

### Community 43 - "Prepared Statement Bound"
Cohesion: 0.25
Nodes (15): Both Statement Sets Are Bounded, Buffers Are Per Client, So They Are What Multiplies, Evicting From A Backend Sends A Real Close, s.expect Swallows the Injected ParseComplete, A Name Already on the Backend May Be Another Client's, The Default Is 512, Not PgBouncer’s 200, reconcile Replays Parse Onto Whichever Backend Comes Next, Bound What Multiplies (+7 more)

### Community 44 - "pgx Rows Wrapper"
Cohesion: 0.15
Nodes (5): Field, Bool, connWrapper, Seq, pgRows

### Community 45 - "Rows And Row Unit Tests"
Cohesion: 0.30
Nodes (14): newRow(), newRows(), T, TestErrorRowDefersTheError(), TestResultReportsRowsAffected(), TestRowReleaseWithoutScanClosesTheResultSet(), TestRowsAllClosesOnEarlyBreak(), TestRowScanAfterReleaseIsRefused() (+6 more)

### Community 47 - "Fake Connector Fixtures"
Cohesion: 0.21
Nodes (6): Int32, Value, bareConn, fakeConnector, fakeRows, fakeStmt

### Community 48 - "pgx Driver Adapter"
Cohesion: 0.30
Nodes (6): closeConn(), Config, ConnConfig, Context, pgConn, pgxDriver

### Community 49 - "PostgreSQL Pool Facade"
Cohesion: 0.20
Nodes (5): connWrapper, Context, Postgres, Stat, translate()

### Community 50 - "Proxy Package Imports"
Cohesion: 0.29
Nodes (7): bufio, crypto/tls, encoding/binary, github.com/jackc/pgx/v5/pgconn, github.com/jackc/pgx/v5/pgproto3, io, net

### Community 51 - "Byte Relay"
Cohesion: 0.27
Nodes (6): endsTransactionUnit(), flushIfDrained(), Reader, Writer, namesStatement(), relay

### Community 52 - "Prepared Statement Replay"
Cohesion: 0.33
Nodes (8): bindName(), closeStatement(), cString(), cutBytes(), parseName(), statementNameOf(), targetName(), bytes

### Community 53 - "Proxy Connection Config"
Cohesion: 0.17
Nodes (5): Conn, cacheCapacity(), ConnConfig, Duration, Config

### Community 54 - "Transaction Wrapper"
Cohesion: 0.23
Nodes (6): Tx, Bool, connWrapper, Context, newTx(), pgTx

### Community 55 - "Core Driver Interface"
Cohesion: 0.17
Nodes (10): Bool, CancelFunc, Config, Int32, Int64, Mutex, Once, Core (+2 more)

### Community 56 - "Cross-Application Pooling Notes"
Cohesion: 0.23
Nodes (12): A Library Cannot Bound Connections Across Applications, Interleave the Targets at Each Concurrency, PgBouncer's One-Thread Ceiling Is the Crossover, pump and relay: Two Goroutines Per Session, TestProxyBoundsBackendsAcrossIndependentClients, The Gap a Sidecar Pooler Fills, PgBouncer Is the More Efficient of the Two, and It Is Not Close, gpoolproxy (+4 more)

### Community 57 - "MySQL Table Filter"
Cohesion: 0.26
Nodes (5): filter, RWMutex, newFilter(), normalize(), qualify()

### Community 58 - "Command Result"
Cohesion: 0.18
Nodes (3): Result, failedBatchResults, pgResult

### Community 59 - "Idle Connection Shards"
Cohesion: 0.27
Nodes (7): C, C, Int32, Mutex, idleConn, shard, shard[C]

### Community 60 - "Permit Accounting"
Cohesion: 0.26
Nodes (3): Context, Int32, permits

### Community 61 - "Benchmark Hygiene"
Cohesion: 0.27
Nodes (11): Measured Against PgBouncer 1.25.2, Benchmark Hygiene, Match Capacity on Both Sides, 200k Iterations Before allocs/op Settles, Scale and Footprint, Benchmark Hygiene, A Benchmark Comparison Must Match Capacity on Both Sides, Iteration Count Changes What a Benchmark Reports (+3 more)

### Community 62 - "MySQL Column Cache"
Cohesion: 0.29
Nodes (6): columns, TableMapEvent, Context, DB, Mutex, newColumns()

### Community 63 - "Proxy Userlist And TLS"
Cohesion: 0.20
Nodes (10): clientTLS(), Config, NewProxy(), randomSecret(), TestUserlistAcceptsBothSecretForms(), TestUserlistRefusesWorldReadableFile(), checkSecretPermissions(), loadUserlist() (+2 more)

### Community 64 - "Prepared Statement Bound Tests"
Cohesion: 0.53
Nodes (10): newStatements(), T, parseMessage(), TestStatementsCopyWhatTheyRemember(), TestStatementsEvictTheLeastRecentlyUsed(), TestStatementsForget(), TestStatementsHoldTheirLimit(), TestStatementsIgnoreTheUnnamedStatement() (+2 more)

### Community 65 - "pgx Transaction Methods"
Cohesion: 0.27
Nodes (4): Row, Bool, Context, pgTx

### Community 66 - "LISTEN/NOTIFY Capability"
Cohesion: 0.29
Nodes (5): Notification, Notifier, Context, connWrapper, quoteIdentifier()

### Community 67 - "Rows And Batch Results"
Cohesion: 0.29
Nodes (5): Rows, Bool, closeRows(), batchResults, rowCursor

### Community 68 - "Permit Unit Tests"
Cohesion: 0.42
Nodes (8): newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate(), TestPermitsHoldTheBoundUnderContention(), TestPermitsSurvivesOverRelease()

### Community 70 - "Token Channel Permits"
Cohesion: 0.36
Nodes (9): Dropped golang.org/x/sync, Step 12: Scale and Footprint, Token Channel Replaces Counting Semaphore, permits: a chan struct{} Token Set, Dropped golang.org/x/sync Dependency, semaphore.Weighted Mutex Convoy, Token Channel Permits, Changed: Token Channel Replaced the Counting Semaphore (+1 more)

### Community 71 - "MySQL Position Tracker"
Cohesion: 0.31
Nodes (5): tracker, GTIDSet, newTracker(), position, GTIDSet

### Community 72 - "SQL Server CDC Config"
Cohesion: 0.22
Nodes (4): regexp, Config, Duration, changesSQL()

### Community 73 - "Bulk Copy Capability"
Cohesion: 0.31
Nodes (6): BulkCopier, CopyRequest, Context, connWrapper, Postgres, validateCopyRequest()

### Community 74 - "MySQL Position Tests"
Cohesion: 0.44
Nodes (8): parsePosition(), T, TestFilterMatching(), TestFilterMutation(), TestParsePositionIsFlavourSpecific(), TestParsePositionRejectsAForeignPosition(), TestPositionDistinguishesItsTwoNotations(), TestPositionRoundTrips()

### Community 75 - "Proxy Entry Point"
Cohesion: 0.32
Nodes (7): Config, main(), parseFlags(), run(), flag, os/signal, syscall

### Community 76 - "Acquisition Handle"
Cohesion: 0.25
Nodes (4): C, newConnWrapper(), Handle, Handle[C]

### Community 77 - "Vendor-Agnostic Engine Release"
Cohesion: 0.29
Nodes (7): Native vendor: implements pooling.Driver directly, Added: Four New Databases, Each Its Own Go Module, Added: examples/gpoolproxy, a PostgreSQL Pooler on the Engine, Changed: PostgreSQL Runs on the Shared Pooling Engine, pkg/pooling Extracted As The Vendor-Agnostic Engine, v0.4.0 (2026-08-07), pkg/pooling Is Genuinely Vendor-Agnostic

### Community 79 - "Graph Reconciliation Script"
Cohesion: 0.43
Nodes (6): is_package(), main(), package_label(), Recover a readable import path from a synthesized package id.      The id is los, Return the extraction with package nodes added and unresolvable edges dropped., reconcile()

### Community 80 - "Coarse Clock"
Cohesion: 0.38
Nodes (4): Int64, Time, newCoarseClock(), coarseClock

### Community 81 - "Transaction Unit Tests"
Cohesion: 0.57
Nodes (6): newTx(), T, TestTxCommitWithDeferredRollback(), TestTxRefusesUseAfterSettle(), TestTxRollbackWithDeferredRollback(), TestTxSettlesExactlyOnce()

### Community 82 - "Footprint At Scale"
Cohesion: 0.47
Nodes (6): Goroutine Cost Is One, Total, Two Readings of 5000 Connections, Cost Per Caller Must Be Zero, Footprint At Scale, Cost Per Caller Is Zero, Two Readings of '5,000 Connections'

### Community 83 - "MySQL Binlog Syncer Config"
Cohesion: 0.33
Nodes (5): BinlogSyncerConfig, newFromConfig(), Config, New(), splitHostPort()

### Community 84 - "Stat Interface Composition"
Cohesion: 0.33
Nodes (3): Acquisition, Occupancy, Stat

### Community 86 - "pgx Single Row"
Cohesion: 0.47
Nodes (3): Bool, connWrapper, pgRow

### Community 88 - "SQL Server Pool Config"
Cohesion: 0.50
Nodes (3): Connector, Duration, Config

### Community 89 - "Idle Connection Expiry"
Cohesion: 0.50
Nodes (3): Duration, Time, idleConn[C]

## Ambiguous Edges - Review These
- `Gpool: A Go Connection Pooling & CDC Library` → `CLI Proxy Mode`  [AMBIGUOUS]
  .junie/plans/gpool-lib-init.md · relation: conceptually_related_to
- `Supported Databases` → `Gpool Core Memory`  [AMBIGUOUS]
  .serena/memories/core.md · relation: conceptually_related_to
- `An Unknown User Runs the Full Exchange Against a Decoy Verifier` → `Errors Are Classified by Code, Never by Message Text`  [AMBIGUOUS]
  .serena/memories/proxy.md · relation: semantically_similar_to

## Knowledge Gaps
- **40 isolated node(s):** `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD`, `CH_PASSWORD`, `MSSQL_PASSWORD` (+35 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **8 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Gpool: A Go Connection Pooling & CDC Library` and `CLI Proxy Mode`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Supported Databases` and `Gpool Core Memory`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `An Unknown User Runs the Full Exchange Against a Decoy Verifier` and `Errors Are Classified by Code, Never by Message Text`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._
- **Why does `Core` connect `Core Driver Interface` to `Pooling Core Engine`, `Pooling Core Unit Tests`, `database/sql Pool Adapter`, `Acquisition Handle`, `Core Package Imports`, `PostgreSQL Pool Facade`, `Coarse Clock`, `Idle Connection Shards`, `Permit Accounting`, `Proxy Listener And Warm-Up`?**
  _High betweenness centrality (0.033) - this node is a cross-community bridge._
- **Why does `Proxy` connect `Proxy Listener And Warm-Up` to `Proxy Backend Driver`, `Proxy Package Imports`, `SCRAM Authentication`, `Proxy Connection Config`, `Core Driver Interface`, `Proxy Session Startup`, `Proxy Userlist And TLS`?**
  _High betweenness centrality (0.032) - this node is a cross-community bridge._
- **Why does `fakeTx` connect `Batch And Bulk Copy Mocks` to `Core Package Imports`, `Proxy Connection Config`?**
  _High betweenness centrality (0.024) - this node is a cross-community bridge._
- **What connects `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._