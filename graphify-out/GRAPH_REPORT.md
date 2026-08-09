# Graph Report - .  (2026-08-09)

## Corpus Check
- 181 files · ~135,360 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2148 nodes · 5032 edges · 88 communities (81 shown, 7 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 607 edges (avg confidence: 0.83)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- SQL Server CDC Polling
- CDC Design Notes
- PostgreSQL CDC Subscriber
- Proxy Tests And SCRAM
- Pooling Core Engine
- Project Charter And Guidelines
- Pooling Core Unit Tests
- database/sql Pool Adapter
- Batch And Bulk Copy Mocks
- Value Conversion And Scanning
- Engine And Multi-Database Tests
- Cross-Application Proxy Notes
- Module Dependency Graph
- Project Invariants
- Pool Internals And Recovery
- Scale And Footprint Notes
- Testing And CI
- Fake database/sql Driver
- Vendor Config And Imports
- SQL Server Pool Tests
- MySQL Integration And Failure Tests
- database/sql Vendor Architecture
- CDC Package Imports
- Proxy Session Startup
- Proxy Listener And Sessions
- PostgreSQL Replication Stream
- Proxy Package Imports
- Event Shape And Releases
- MySQL Binlog Stream
- Scale Benchmarks
- ClickHouse Pool Tests
- Vendor Factory Registry
- Test Database Script
- MySQL CDC Subscriber
- MySQL CDC Integration Tests
- Proxy Backend And Statement Replay
- Pool And Subscriber Mocks
- SQL Server CDC Integration Tests
- PostgreSQL CDC Integration Tests
- PostgreSQL Pool Integration Tests
- sqldriver Pool Tests
- Permit Accounting
- Core Package Imports
- Bulk Copy Tests
- Rows, Row And Batch Results
- PostgreSQL Pool Tests
- CDC Event Decoding
- Godoc Examples
- Rows And Row Unit Tests
- pgoutput Tuple Decoding
- Prepared Statement Parsing
- Soak Tests
- PostgreSQL Pool Facade
- pgx Driver Adapter
- pgx Rows Wrapper
- Decoder Unit Tests
- Byte Relay
- MySQL Column Cache
- Proxy Entry Point
- MySQL Table Filter
- Prepared Statement Bound Tests
- PgBouncer Stacking Tests
- Stat Accessors
- Multi-Database Architecture
- LISTEN/NOTIFY Capability
- Transaction Wrapper
- Connection Wrapper
- Allocation Discipline
- SQL Server CDC Config
- Bulk Copy Capability
- Failure Injection Tests
- PostgreSQL Config Tests
- MySQL Position Tests
- Proxy Throughput Benchmarks
- PostgreSQL Position Tests
- Command Result
- PostgreSQL Pool Config
- pgx Transaction Methods
- Graph Reconciliation Script
- Transaction Unit Tests
- MySQL Binlog Syncer Config
- Stat Interface Composition
- pgx Command Result
- pgx Single Row
- MySQL Pool Config
- SQL Server Pool Config
- Deferred Error Row
- Resizable Capability

## God Nodes (most connected - your core abstractions)
1. `gpool - Agent & Developer Profiles` - 52 edges
2. `session` - 41 edges
3. `newPool()` - 34 edges
4. `Proxy` - 30 edges
5. `newTestCore()` - 30 edges
6. `Pool Internals` - 29 edges
7. `NewPool()` - 28 edges
8. `Postgres` - 27 edges
9. `gpoolproxy - Cross-Application Pooling` - 27 edges
10. `backend` - 26 edges

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
- **Sets A Client Controls The Size Of** — _serena_memories_proxy_bounded_statements, _serena_memories_proxy_buffer_sizing, _serena_memories_proxy_cross_application_bound, agents_bound_what_multiplies [INFERRED 0.85]
- **CI Is Five Jobs Over Six Modules** — _github_workflows_ci_check, _github_workflows_ci_unit, _github_workflows_ci_integration, _github_workflows_ci_vuln, _github_workflows_ci_consumer [EXTRACTED 1.00]
- **The Resume Contract Is Position, Transaction, Timestamp And SubscribeFrom** — _serena_memories_cdc_event_position, _serena_memories_cdc_event_transaction, _serena_memories_cdc_event_timestamp, _serena_memories_cdc_subscribefrom, _serena_memories_cdc_resume_contract [EXTRACTED 1.00]
- **Token Channel Permit Set Replacing the Counting Semaphore** — _serena_memories_scale_semaphore_weighted_bottleneck, _serena_memories_scale_token_channel_permits, _serena_memories_pool_permits, _junie_plans_gpool_lib_init_token_channel_replacement, _serena_memories_scale_dropped_x_sync [INFERRED 0.85]

## Communities (88 total, 7 thin omitted)

### Community 0 - "SQL Server CDC Polling"
Cohesion: 0.06
Nodes (43): captureInstance, change, sqlEventStream, SQLServer, Config, Duration, asInt(), describe() (+35 more)

### Community 1 - "CDC Design Notes"
Cohesion: 0.05
Nodes (74): CDC Control and Replication Connection Split, Step 11: Production Hardening, Compile-Time Interface Proofs, vendors/mssql/cdc: a Package Inside the Pool Vendor's Module, catchUp Advances A Quiet Slot, CDC Internals, checkResumable Refuses A Silent Rewind, ErrNoCDCSupport (+66 more)

### Community 2 - "PostgreSQL CDC Subscriber"
Cohesion: 0.08
Nodes (46): Postgres, firstErr(), Config, T, longSlotName(), TestClosedSubscriberRefusesWork(), TestConfigDefaults(), TestConfigValidate() (+38 more)

### Community 3 - "Proxy Tests And SCRAM"
Cohesion: 0.07
Nodes (59): BenchmarkPgBouncer(), BenchmarkPgxPool(), BenchmarkPgxPoolStress(), BenchmarkStdlib(), BenchmarkStdlibStress(), B, cachingURL(), connect() (+51 more)

### Community 4 - "Pooling Core Engine"
Cohesion: 0.05
Nodes (31): Int64, Time, newCoarseClock(), Bool, C, CancelFunc, Config, Context (+23 more)

### Community 5 - "Project Charter And Guidelines"
Cohesion: 0.05
Nodes (61): No AI Co-Authorship Trailers, Mandatory Copyright Header, Modern Go 1.26 Syntax, Gpool Is a Library, Not a Service, Graphify Knowledge Graph, Gpool Project Guidelines (.junie), Hierarchical Discovery Before a Level 4 Full Read, The /internal Rule Is Inverted for gpool (+53 more)

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
Nodes (36): Stream, Subscriber, TableManager, net/url, Engine, databaseNamed(), Config, T (+28 more)

### Community 11 - "Cross-Application Proxy Notes"
Cohesion: 0.08
Nodes (44): Both Statement Sets Are Bounded, Buffers Are Per Client, So They Are What Multiplies, Cancellation Keys Are Per Session and Unguessable, A Library Cannot Bound Connections Across Applications, Evicting From A Backend Sends A Real Close, s.expect Swallows the Injected ParseComplete, gpoolproxy - Cross-Application Pooling, Interleave the Targets at Each Concurrency (+36 more)

### Community 12 - "Module Dependency Graph"
Cohesion: 0.06
Nodes (45): filippo/io/edwards25519, github.com/andybalholm/brotli, github.com/cespare/xxhash/v2, github.com/clickhouse/ch/go, github.com/clickhouse/clickhouse/go/v2, github.com/coreos/go/semver, github.com/go/faster/city, github.com/go/faster/errors (+37 more)

### Community 13 - "Project Invariants"
Cohesion: 0.07
Nodes (43): SQL Externalization to .sql + go:embed, The run Defer Order Is Load-Bearing, emit Keeps Keepalives Flowing While Blocked, captureInstancePattern Validation, slotNamePattern Validates What Is Interpolated, Control And Replication Connections Are Never Shared, Vendor Registry (Only Process-Global State), Every Drain Is Bounded (+35 more)

### Community 14 - "Pool Internals And Recovery"
Cohesion: 0.07
Nodes (43): Integrated CDC via Logical Replication, Gpool Library Initialization Plan, Go 1.26 Iterator API, PgBouncer Replacement, Transaction-Mode Pooling Constraints, EvictIdle, LISTEN Is Session State the Transaction Gate Cannot See, Driver.NeedsCleanup Exists to Keep Release Cheap (+35 more)

### Community 15 - "Scale And Footprint Notes"
Cohesion: 0.08
Nodes (42): No Memory Leaks, Bounded Statement and Description Caches, Dropped golang.org/x/sync, Step 12: Scale and Footprint, Token Channel Replaces Counting Semaphore, ActiveConnections Is an Exact Count, Per-Connection Caches Are the Memory Cost, maintain: the One Background Goroutine (+34 more)

### Community 16 - "Testing And CI"
Cohesion: 0.08
Nodes (40): CI Workflow, Format And Vet Job, Installable From The Module Proxy Job, Fail If The Databases Were Never Reachable, gofmt Ignores graphify-out, Integration Job (Five Engines), Every Module Vetted And Tested Separately, testdbs.sh Is The One Database Definition (+32 more)

### Community 17 - "Fake database/sql Driver"
Cohesion: 0.08
Nodes (14): Tx, Bool, Context, Int32, NamedValue, Stmt, Value, bareConn (+6 more)

### Community 18 - "Vendor Config And Imports"
Cohesion: 0.07
Nodes (14): database/sql/driver, errors, github.com/clickhouse/clickhouse/go/v2, github.com/gsoultan/gpool/pkg/sqldriver, github.com/microsoft/go/mssqldb, math/rand/v2, runtime, time (+6 more)

### Community 19 - "SQL Server Pool Tests"
Cohesion: 0.13
Nodes (32): github.com/gsoultan/gpool/vendors/mssql, Config, dsn(), Config, Pool, T, newPool(), scratchTable() (+24 more)

### Community 20 - "MySQL Integration And Failure Tests"
Cohesion: 0.16
Nodes (33): github.com/gsoultan/gpool/vendors/mysql, target, target, T, killConnections(), TestPoolRecoversAfterConnectionsAreKilled(), TestPoolReplacesItsOnlyConnectionWhenKilled(), TestPoolSurvivesRepeatedKillsUnderLoad() (+25 more)

### Community 21 - "database/sql Vendor Architecture"
Cohesion: 0.08
Nodes (32): SQL Server Runs Natively On x86-64 Runners, Avoid Stuttering in Filenames and Symbols, Architecture, Minimal Dependencies: pgx/v5 and pglogrepl, vendors/mysql/cdc: Its Own Module Nested in the Pool Vendor, One Struct or Interface Per File; No Stuttering, Registry Guarded by sync.RWMutex, Adding a Vendor Needs Zero Edits to pkg/gpool (+24 more)

### Community 22 - "CDC Package Imports"
Cohesion: 0.12
Nodes (12): database/sql, encoding/hex, fmt, github.com/go/mysql/org/go/mysql/mysql, github.com/go/mysql/org/go/mysql/replication, github.com/go/sql/driver/mysql, github.com/gsoultan/gpool/pkg/gpool/cdc, github.com/jackc/pglogrepl (+4 more)

### Community 23 - "Proxy Session Startup"
Cohesion: 0.12
Nodes (9): newRelay(), cutCString(), CancelFunc, Context, Mutex, Reader, Writer, parameterOf() (+1 more)

### Community 24 - "Proxy Listener And Sessions"
Cohesion: 0.10
Nodes (17): Addr, clientTLS(), Bool, CancelFunc, Config, Context, Mutex, Stat (+9 more)

### Community 25 - "PostgreSQL Replication Stream"
Cohesion: 0.12
Nodes (16): BackendMessage, pendingEvent, pgEventStream, advance(), Bool, CancelFunc, Config, Context (+8 more)

### Community 26 - "Proxy Package Imports"
Cohesion: 0.12
Nodes (13): ReplicationManager, bufio, context, crypto/tls, encoding/binary, github.com/gsoultan/gpool/pkg/pooling, github.com/jackc/pgx/v5/pgconn, github.com/jackc/pgx/v5/pgproto3 (+5 more)

### Community 27 - "Event Shape And Releases"
Cohesion: 0.12
Nodes (28): VendorFactory Pattern, Vendor Registry and init() Self-Registration, Event.Position Is An Opaque String, Not A Number, Event.Timestamp Is The Source's Commit Time, Event.Transaction Groups One Commit, Clone the GTID set: BinlogSyncer retains and mutates it, MySQL values keep the binlog parser's native Go types, SHOW BINARY LOG STATUS replaces SHOW MASTER STATUS in 8.4 (+20 more)

### Community 28 - "MySQL Binlog Stream"
Cohesion: 0.11
Nodes (17): BinlogEvent, mysqlEventStream, tracker, BinlogStreamer, BinlogSyncer, Bool, CancelFunc, Context (+9 more)

### Community 29 - "Scale Benchmarks"
Cohesion: 0.15
Nodes (25): BenchmarkGpoolAcquireRelease(), BenchmarkGpoolQueryIterator(), BenchmarkGpoolQueryRow(), BenchmarkGpoolQueryRowStress(), BenchmarkGpoolResetQuery(), B, Config, Pool (+17 more)

### Community 30 - "ClickHouse Pool Tests"
Cohesion: 0.15
Nodes (25): Config, github.com/gsoultan/gpool/vendors/clickhouse, Options, Connector, Duration, Pool, New(), newFromConfig() (+17 more)

### Community 31 - "Vendor Factory Registry"
Cohesion: 0.16
Nodes (25): PoolFactory, SubscriberFactory, Vendor, TestCDCRefusesASecondConsumerOnOneSlot(), Example_slotAdministration(), ExampleNewSubscriber(), NewSubscriber(), RegisterPool() (+17 more)

### Community 32 - "Test Database Script"
Cohesion: 0.14
Nodes (23): ALL_ENGINES, CH_PASSWORD, cmd_down(), cmd_env(), cmd_status(), cmd_up(), die(), dsn_of() (+15 more)

### Community 33 - "MySQL CDC Subscriber"
Cohesion: 0.16
Nodes (9): MySQL, Position, BinlogStreamer, BinlogSyncer, Context, DB, GTIDSet, Mutex (+1 more)

### Community 34 - "MySQL CDC Integration Tests"
Cohesion: 0.30
Nodes (22): target, github.com/gsoultan/gpool/vendors/mysql/cdc, collect(), eachTarget(), fixture, Config, DB, Duration (+14 more)

### Community 35 - "Proxy Backend And Statement Replay"
Cohesion: 0.14
Nodes (11): Bool, Context, Reader, Writer, Config, Context, networkAddress(), closeStatement() (+3 more)

### Community 36 - "Pool And Subscriber Mocks"
Cohesion: 0.12
Nodes (6): EventStream, fakePool, fakeSubscriber, Context, Int32, Stat

### Community 37 - "SQL Server CDC Integration Tests"
Cohesion: 0.25
Nodes (19): github.com/gsoultan/gpool/vendors/mssql/cdc, collect(), contains(), dsn(), fixture, Config, DB, Duration (+11 more)

### Community 38 - "PostgreSQL CDC Integration Tests"
Cohesion: 0.27
Nodes (19): collect(), emailsOf(), Config, Duration, Pool, T, newCDCFixture(), TestCDCCloseIsIdempotent() (+11 more)

### Community 39 - "PostgreSQL Pool Integration Tests"
Cohesion: 0.23
Nodes (22): connString(), Config, Pool, T, newPool(), TestPoolCloseIsSafeUnderLoad(), TestPoolDeferredCloseAlongsideIterator(), TestPoolIteratorReleasesTheConnection() (+14 more)

### Community 40 - "sqldriver Pool Tests"
Cohesion: 0.22
Nodes (22): Config, New(), Config, Pool, T, newTestPool(), TestAbandonedTransactionIsUnwoundOnRelease(), TestAcquireAfterCloseFailsFast() (+14 more)

### Community 41 - "Permit Accounting"
Cohesion: 0.17
Nodes (11): Context, Int32, newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate() (+3 more)

### Community 42 - "Core Package Imports"
Cohesion: 0.22
Nodes (4): github.com/gsoultan/gpool/pkg/gpool, github.com/jackc/pgx/v5, iter, sync/atomic

### Community 43 - "Bulk Copy Tests"
Cohesion: 0.19
Nodes (14): CopyRows, sliceRows, Pool, T, scratchTable(), TestCopyFromLoadsRows(), TestCopyFromRollsBackOnSourceError(), TestCopyFromValidatesTheRequest() (+6 more)

### Community 44 - "Rows, Row And Batch Results"
Cohesion: 0.16
Nodes (7): Row, Rows, Bool, closeRows(), batchResults, failedBatchResults, rowCursor

### Community 45 - "PostgreSQL Pool Tests"
Cohesion: 0.22
Nodes (16): Pool, newFromConfig(), Config, New(), Config, T, newTestPool(), TestAcquireAfterCloseFailsFast() (+8 more)

### Community 46 - "CDC Event Decoding"
Cohesion: 0.14
Nodes (11): Event, Op, EventType, fakeStream, Time, Seq, columnMap(), decodeRows() (+3 more)

### Community 47 - "Godoc Examples"
Cohesion: 0.19
Nodes (15): github.com/gsoultan/gpool/pkg/vendors/postgres/cdc, github.com/gsoultan/gpool/pkg/vendors/postgres/pool, log, apply(), Example_resumingFromACheckpoint(), ExampleConn_Begin(), ExampleEngine(), ExampleNewPool() (+7 more)

### Community 48 - "Rows And Row Unit Tests"
Cohesion: 0.27
Nodes (15): newRow(), connWrapper, newRows(), T, TestErrorRowDefersTheError(), TestResultReportsRowsAffected(), TestRowReleaseWithoutScanClosesTheResultSet(), TestRowsAllClosesOnEarlyBreak() (+7 more)

### Community 49 - "pgoutput Tuple Decoding"
Cohesion: 0.27
Nodes (13): transaction, DeleteMessage, InsertMessage, decodeDelete(), decodeInsert(), decodeTuple(), decodeUpdate(), RelationMessage (+5 more)

### Community 50 - "Prepared Statement Parsing"
Cohesion: 0.22
Nodes (9): bindName(), cString(), cutBytes(), parseName(), statementNameOf(), targetName(), bytes, statement (+1 more)

### Community 51 - "Soak Tests"
Cohesion: 0.33
Nodes (14): sample, Duration, Pool, T, Time, slotRetainedBytes(), soakDuration(), takeSample() (+6 more)

### Community 52 - "PostgreSQL Pool Facade"
Cohesion: 0.18
Nodes (6): newConnWrapper(), connWrapper, Context, Postgres, Stat, translate()

### Community 53 - "pgx Driver Adapter"
Cohesion: 0.27
Nodes (6): closeConn(), Config, ConnConfig, Context, pgConn, pgxDriver

### Community 54 - "pgx Rows Wrapper"
Cohesion: 0.16
Nodes (4): Field, Bool, Seq, pgRows

### Community 55 - "Decoder Unit Tests"
Cohesion: 0.29
Nodes (13): assertColumns(), assertHeader(), RelationMessage, T, TupleData, TestDecodedMapsAreNotShared(), TestDecodeOperations(), TestDecodeTuple() (+5 more)

### Community 56 - "Byte Relay"
Cohesion: 0.27
Nodes (6): endsTransactionUnit(), flushIfDrained(), Reader, Writer, namesStatement(), relay

### Community 57 - "MySQL Column Cache"
Cohesion: 0.27
Nodes (7): columns, TableMapEvent, Context, DB, Mutex, newColumns(), qualify()

### Community 58 - "Proxy Entry Point"
Cohesion: 0.20
Nodes (10): Config, hash(), main(), parseFlags(), run(), flag, io/fs, os (+2 more)

### Community 59 - "MySQL Table Filter"
Cohesion: 0.27
Nodes (4): filter, RWMutex, newFilter(), normalize()

### Community 60 - "Prepared Statement Bound Tests"
Cohesion: 0.53
Nodes (10): newStatements(), T, parseMessage(), TestStatementsCopyWhatTheyRemember(), TestStatementsEvictTheLeastRecentlyUsed(), TestStatementsForget(), TestStatementsHoldTheirLimit(), TestStatementsIgnoreTheUnnamedStatement() (+2 more)

### Community 61 - "PgBouncer Stacking Tests"
Cohesion: 0.45
Nodes (10): Config, Pool, T, hammer(), newPooledPool(), pgBouncerURL(), TestPgBouncerStatDescribesTheProxyHop(), TestPgBouncerTransactionsAreAtomic() (+2 more)

### Community 63 - "Multi-Database Architecture"
Cohesion: 0.31
Nodes (10): Multi-Database Engine Pool Registry, Step 10: Multi-Database Support, Independent Replication Slot per Node, Pool(name) Naming Decision, Engine: Named Registries of Pools and Subscribers, One Pool Per Database, Sharing Nothing, Multi-Database Tests, Several Databases Means Several Pools (+2 more)

### Community 64 - "LISTEN/NOTIFY Capability"
Cohesion: 0.29
Nodes (5): Notification, Notifier, Context, connWrapper, quoteIdentifier()

### Community 65 - "Transaction Wrapper"
Cohesion: 0.31
Nodes (5): Bool, connWrapper, Context, newTx(), pgTx

### Community 67 - "Allocation Discipline"
Cohesion: 0.28
Nodes (9): sync.Pool Restricted to Non-Escaping Buffers, Zero-Allocation Hot Paths, Never Recycle Caller-Owned Objects, Handle Is Returned by Value, The LRU Is Found By Scanning, Not Kept In A List, Allocation Is Cheap Next to a Network Round Trip, Never Recycle an Object Whose Lifetime User Code Controls, CPU and Memory Overhead (+1 more)

### Community 68 - "SQL Server CDC Config"
Cohesion: 0.22
Nodes (4): regexp, Config, Duration, changesSQL()

### Community 69 - "Bulk Copy Capability"
Cohesion: 0.31
Nodes (6): BulkCopier, CopyRequest, Context, connWrapper, Postgres, validateCopyRequest()

### Community 70 - "Failure Injection Tests"
Cohesion: 0.44
Nodes (9): Config, Pool, T, taggedPool(), terminate(), TestPoolRecoversAfterEveryBackendIsTerminated(), TestPoolReplacesItsOnlyConnectionWhenTerminated(), TestPoolSurvivesRepeatedTerminationUnderLoad() (+1 more)

### Community 71 - "PostgreSQL Config Tests"
Cohesion: 0.39
Nodes (8): T, TestConfigBoundsPerConnectionCaches(), TestConfigDefaultsGiveUsableCapacity(), TestConfigDefaultsPreserveExplicitValues(), TestConfigParseIsOrderIndependent(), TestConfigParseRejectsBadConnString(), TestConfigResetQuerySelectsACompatibleExecMode(), TestConfigValidate()

### Community 72 - "MySQL Position Tests"
Cohesion: 0.44
Nodes (8): parsePosition(), T, TestFilterMatching(), TestFilterMutation(), TestParsePositionIsFlavourSpecific(), TestParsePositionRejectsAForeignPosition(), TestPositionDistinguishesItsTwoNotations(), TestPositionRoundTrips()

### Community 73 - "Proxy Throughput Benchmarks"
Cohesion: 0.36
Nodes (6): benchmarkTarget(), BenchmarkThroughput(), B, Pool, warm(), strconv

### Community 74 - "PostgreSQL Position Tests"
Cohesion: 0.39
Nodes (7): math, testing, parsePosition(), T, TestParsePositionRejectsAForeignPosition(), TestParsePositionTreatsNoPositionAsTheSlotsChoice(), TestPositionRoundTrips()

### Community 76 - "PostgreSQL Pool Config"
Cohesion: 0.29
Nodes (4): cacheCapacity(), ConnConfig, Duration, Config

### Community 77 - "pgx Transaction Methods"
Cohesion: 0.43
Nodes (3): Bool, Context, pgTx

### Community 78 - "Graph Reconciliation Script"
Cohesion: 0.43
Nodes (6): is_package(), main(), package_label(), Recover a readable import path from a synthesized package id.      The id is los, Return the extraction with package nodes added and unresolvable edges dropped., reconcile()

### Community 79 - "Transaction Unit Tests"
Cohesion: 0.57
Nodes (6): newTx(), T, TestTxCommitWithDeferredRollback(), TestTxRefusesUseAfterSettle(), TestTxRollbackWithDeferredRollback(), TestTxSettlesExactlyOnce()

### Community 80 - "MySQL Binlog Syncer Config"
Cohesion: 0.33
Nodes (5): BinlogSyncerConfig, newFromConfig(), Config, New(), splitHostPort()

### Community 81 - "Stat Interface Composition"
Cohesion: 0.33
Nodes (3): Acquisition, Occupancy, Stat

### Community 83 - "pgx Single Row"
Cohesion: 0.47
Nodes (3): Bool, connWrapper, pgRow

### Community 85 - "SQL Server Pool Config"
Cohesion: 0.50
Nodes (3): Connector, Duration, Config

## Ambiguous Edges - Review These
- `An Unknown User Runs the Full Exchange Against a Decoy Verifier` → `Errors Are Classified by Code, Never by Message Text`  [AMBIGUOUS]
  .serena/memories/proxy.md · relation: semantically_similar_to
- `CLI Proxy Mode` → `Gpool: A Go Connection Pooling & CDC Library`  [AMBIGUOUS]
  .junie/plans/gpool-lib-init.md · relation: conceptually_related_to
- `Gpool Core Memory` → `Supported Databases`  [AMBIGUOUS]
  .serena/memories/core.md · relation: conceptually_related_to

## Knowledge Gaps
- **40 isolated node(s):** `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD`, `CH_PASSWORD`, `MSSQL_PASSWORD` (+35 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `An Unknown User Runs the Full Exchange Against a Decoy Verifier` and `Errors Are Classified by Code, Never by Message Text`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._
- **What is the exact relationship between `CLI Proxy Mode` and `Gpool: A Go Connection Pooling & CDC Library`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Gpool Core Memory` and `Supported Databases`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `Core` connect `Pooling Core Engine` to `Pooling Core Unit Tests`, `database/sql Pool Adapter`, `Permit Accounting`, `Vendor Config And Imports`, `PostgreSQL Pool Facade`, `Proxy Listener And Sessions`?**
  _High betweenness centrality (0.035) - this node is a cross-community bridge._
- **Why does `Proxy` connect `Proxy Listener And Sessions` to `Proxy Tests And SCRAM`, `Proxy Backend And Statement Replay`, `Pooling Core Engine`, `Proxy Session Startup`, `Proxy Package Imports`?**
  _High betweenness centrality (0.034) - this node is a cross-community bridge._
- **Why does `MySQL` connect `MySQL CDC Subscriber` to `MySQL Binlog Syncer Config`, `CDC Package Imports`, `MySQL Column Cache`, `MySQL Table Filter`, `MySQL Binlog Stream`?**
  _High betweenness centrality (0.033) - this node is a cross-community bridge._
- **What connects `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._