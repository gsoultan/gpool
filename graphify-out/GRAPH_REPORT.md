# Graph Report - .  (2026-08-09)

## Corpus Check
- 184 files · ~138,896 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2179 nodes · 5131 edges · 81 communities (72 shown, 9 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 616 edges (avg confidence: 0.83)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- PostgreSQL Integration Fixtures
- PostgreSQL CDC Subscriber
- CDC Design Notes
- PostgreSQL Logical Decoding Stream
- Pooling Core Unit Tests
- SQL Server CDC Polling
- Pooling Core Engine
- Project Invariants
- Project Charter And Guidelines
- Batch And Bulk Copy Mocks
- Value Conversion And Scanning
- Engine And Multi-Database Tests
- Cross-Application Proxy Notes
- Releases, Testing And CI
- Pool And Config Unit Tests
- Vendor Architecture
- Module Dependency Graph
- Vendor Factory And Godoc Examples
- Scale And Footprint Notes
- SCRAM Authentication
- MySQL CDC Integration Tests
- Core Package Imports
- Pool Internals And Lifecycle
- Proxy Listener And Warm-Up
- SQL Server Pool Tests
- MySQL Integration And Failure Tests
- Vendor Config And Imports
- Scale And Capability Benchmarks
- CDC Package Imports
- Proxy Session Startup
- ClickHouse Pool Tests
- Proxy And Stream Imports
- Test Database Script
- MySQL CDC Subscriber
- Proxy Backend And Statement Replay
- Pool And Subscriber Mocks
- Proxy Integration Tests
- SQL Server CDC Integration Tests
- database/sql Pool Adapter
- sqldriver Pool Tests
- Event Shape Notes
- MySQL Binlog Stream
- Permit Accounting
- Fake database/sql Driver
- Rows, Row And Batch Results
- Fake Connector Fixtures
- Rows And Row Unit Tests
- Prepared Statement Parsing
- Transaction Wrapper
- PostgreSQL Pool Facade
- pgx Rows Wrapper
- Stat Accessors
- pgx Driver Adapter
- Byte Relay
- Row Cursors
- MySQL Column Cache
- Prepared Statement Bound Tests
- PgBouncer Stacking Tests
- sqldriver Connection Wrapper
- Multi-Database Architecture
- Command Result
- LISTEN/NOTIFY Capability
- Connection Wrapper
- MySQL Position Tracker
- Proxy Entry Point
- SQL Server CDC Config
- Bulk Copy Capability
- Proxy Throughput Benchmarks
- Pool Interface
- pgx Transaction Methods
- Graph Reconciliation Script
- Transaction Unit Tests
- MySQL Binlog Syncer Config
- Stat Interface Composition
- pgx Single Row
- MySQL Pool Config
- Proxy Userlist
- SQL Server Pool Config
- pgx Command Result
- Deferred Error Row
- Resizable Capability

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
- **CI Is Five Jobs Over Six Modules** — _github_workflows_ci_check, _github_workflows_ci_unit, _github_workflows_ci_integration, _github_workflows_ci_vuln, _github_workflows_ci_consumer [EXTRACTED 1.00]
- **The Resume Contract Is Position, Transaction, Timestamp And SubscribeFrom** — _serena_memories_cdc_event_position, _serena_memories_cdc_event_transaction, _serena_memories_cdc_event_timestamp, _serena_memories_cdc_subscribefrom, _serena_memories_cdc_resume_contract [EXTRACTED 1.00]
- **Sets A Client Controls The Size Of** — _serena_memories_proxy_bounded_statements, _serena_memories_proxy_buffer_sizing, _serena_memories_proxy_cross_application_bound, agents_bound_what_multiplies [INFERRED 0.85]
- **Token Channel Permit Set Replacing the Counting Semaphore** — _serena_memories_scale_semaphore_weighted_bottleneck, _serena_memories_scale_token_channel_permits, _serena_memories_pool_permits, _junie_plans_gpool_lib_init_token_channel_replacement, _serena_memories_scale_dropped_x_sync [INFERRED 0.85]
- **The Same Failover Counted From Both Sides** — readme_lifecycle_alerting, _serena_memories_testing_recovery_contract, _serena_memories_pool_lifecycle_counters, _serena_memories_testing_failure_injection [EXTRACTED 1.00]

## Communities (81 total, 9 thin omitted)

### Community 0 - "PostgreSQL Integration Fixtures"
Cohesion: 0.06
Nodes (80): CopyRows, sliceRows, Pool, T, scratchTable(), TestCopyFromLoadsRows(), TestCopyFromRollsBackOnSourceError(), TestCopyFromValidatesTheRequest() (+72 more)

### Community 1 - "PostgreSQL CDC Subscriber"
Cohesion: 0.07
Nodes (52): Postgres, math, firstErr(), Config, T, longSlotName(), TestClosedSubscriberRefusesWork(), TestConfigDefaults() (+44 more)

### Community 2 - "CDC Design Notes"
Cohesion: 0.05
Nodes (73): SQL Externalization to .sql + go:embed, CDC Control and Replication Connection Split, Step 11: Production Hardening, Compile-Time Interface Proofs, vendors/mssql/cdc: a Package Inside the Pool Vendor's Module, catchUp Advances A Quiet Slot, CDC Internals, checkResumable Refuses A Silent Rewind (+65 more)

### Community 3 - "PostgreSQL Logical Decoding Stream"
Cohesion: 0.05
Nodes (51): BackendMessage, Event, Op, pendingEvent, pgEventStream, transaction, DeleteMessage, fakeStream (+43 more)

### Community 4 - "Pooling Core Unit Tests"
Cohesion: 0.07
Nodes (59): Lifecycle, New(), Bool, Config, Context, Int32, Mutex, T (+51 more)

### Community 5 - "SQL Server CDC Polling"
Cohesion: 0.06
Nodes (40): captureInstance, change, sqlEventStream, SQLServer, Config, Duration, describe(), Time (+32 more)

### Community 6 - "Pooling Core Engine"
Cohesion: 0.05
Nodes (33): Int64, Time, newCoarseClock(), Bool, C, CancelFunc, Config, Context (+25 more)

### Community 7 - "Project Invariants"
Cohesion: 0.05
Nodes (57): sync.Pool Restricted to Non-Escaping Buffers, Zero-Allocation Hot Paths, Integrated CDC via Logical Replication, Gpool Library Initialization Plan, Go 1.26 Iterator API, PgBouncer Replacement, Transaction-Mode Pooling Constraints, The run Defer Order Is Load-Bearing (+49 more)

### Community 8 - "Project Charter And Guidelines"
Cohesion: 0.05
Nodes (56): No AI Co-Authorship Trailers, Mandatory Copyright Header, Modern Go 1.26 Syntax, Gpool Is a Library, Not a Service, Graphify Knowledge Graph, Gpool Project Guidelines (.junie), Hierarchical Discovery Before a Level 4 Full Read, The /internal Rule Is Inverted for gpool (+48 more)

### Community 9 - "Batch And Bulk Copy Mocks"
Cohesion: 0.05
Nodes (23): CopyFromSource, FieldDescription, Batch, BatchQuery, BatchResults, Identifier, LargeObjects, T (+15 more)

### Community 10 - "Value Conversion And Scanning"
Cohesion: 0.08
Nodes (43): reflect, errArity(), Bool, connWrapper, Seq, Value, newRows(), scanInto() (+35 more)

### Community 11 - "Engine And Multi-Database Tests"
Cohesion: 0.08
Nodes (36): Stream, Subscriber, TableManager, net/url, Engine, databaseNamed(), Config, T (+28 more)

### Community 12 - "Cross-Application Proxy Notes"
Cohesion: 0.07
Nodes (48): emit Keeps Keepalives Flowing While Blocked, CDC Cannot Be Pooled, Both Statement Sets Are Bounded, Buffers Are Per Client, So They Are What Multiplies, Cancellation Keys Are Per Session and Unguessable, A Library Cannot Bound Connections Across Applications, Evicting From A Backend Sends A Real Close, s.expect Swallows the Injected ParseComplete (+40 more)

### Community 13 - "Releases, Testing And CI"
Cohesion: 0.07
Nodes (48): CI Workflow, Format And Vet Job, Installable From The Module Proxy Job, Fail If The Databases Were Never Reachable, gofmt Ignores graphify-out, Every Module Vetted And Tested Separately, testdbs.sh Is The One Database Definition, Unit Tests Job (+40 more)

### Community 14 - "Pool And Config Unit Tests"
Cohesion: 0.08
Nodes (42): BenchmarkPgBouncer(), BenchmarkPgxPool(), BenchmarkPgxPoolStress(), BenchmarkStdlib(), BenchmarkStdlibStress(), B, github.com/jackc/pgx/v5/pgxpool, github.com/jackc/pgx/v5/stdlib (+34 more)

### Community 15 - "Vendor Architecture"
Cohesion: 0.06
Nodes (45): Integration Job (Five Engines), SQL Server Runs Natively On x86-64 Runners, Avoid Stuttering in Filenames and Symbols, VendorFactory Pattern, Architecture, examples/gpoolproxy as a Separate Module, Minimal Dependencies: pgx/v5 and pglogrepl, vendors/mysql/cdc: Its Own Module Nested in the Pool Vendor (+37 more)

### Community 16 - "Module Dependency Graph"
Cohesion: 0.06
Nodes (45): filippo/io/edwards25519, github.com/andybalholm/brotli, github.com/cespare/xxhash/v2, github.com/clickhouse/ch/go, github.com/clickhouse/clickhouse/go/v2, github.com/coreos/go/semver, github.com/go/faster/city, github.com/go/faster/errors (+37 more)

### Community 17 - "Vendor Factory And Godoc Examples"
Cohesion: 0.10
Nodes (39): github.com/gsoultan/gpool/pkg/vendors/postgres/cdc, log, PoolFactory, SubscriberFactory, Vendor, apply(), Example_resumingFromACheckpoint(), Example_slotAdministration() (+31 more)

### Community 18 - "Scale And Footprint Notes"
Cohesion: 0.08
Nodes (41): Bounded Statement and Description Caches, Dropped golang.org/x/sync, Step 12: Scale and Footprint, Token Channel Replaces Counting Semaphore, ActiveConnections Is an Exact Count, Per-Connection Caches Are the Memory Cost, permits: a chan struct{} Token Set, WaitingAcquires Is the Only Gauge Among the Counters (+33 more)

### Community 19 - "SCRAM Authentication"
Cohesion: 0.11
Nodes (29): field(), splitGS2(), exchange(), T, parseServerFirst(), TestSCRAMAcceptsTheRightPassword(), TestSCRAMRejectsAlteredChannelBinding(), TestSCRAMRejectsRequiredChannelBinding() (+21 more)

### Community 20 - "MySQL CDC Integration Tests"
Cohesion: 0.17
Nodes (27): filter, target, github.com/gsoultan/gpool/vendors/mysql/cdc, slices, RWMutex, normalize(), qualify(), collect() (+19 more)

### Community 21 - "Core Package Imports"
Cohesion: 0.10
Nodes (9): ReplicationManager, context, database/sql/driver, github.com/gsoultan/gpool/pkg/gpool, github.com/gsoultan/gpool/pkg/pooling, github.com/jackc/pgx/v5, Batcher, Pool (+1 more)

### Community 22 - "Pool Internals And Lifecycle"
Cohesion: 0.10
Nodes (35): A Lifetime Below clockResolution Is Invisible, Not Quick, Every destroy Carries A discard Reason, EvictIdle, whyUnusable Checks Ill Health First, Lifecycle Counters Say Why Connections Are Replaced, LISTEN Is Session State the Transaction Gate Cannot See, maintain: the One Background Goroutine, Driver.NeedsCleanup Exists to Keep Release Cheap (+27 more)

### Community 23 - "Proxy Listener And Warm-Up"
Cohesion: 0.09
Nodes (17): Addr, clientTLS(), Bool, CancelFunc, Config, Context, Mutex, Stat (+9 more)

### Community 24 - "SQL Server Pool Tests"
Cohesion: 0.13
Nodes (32): github.com/gsoultan/gpool/vendors/mssql, Config, dsn(), Config, Pool, T, newPool(), scratchTable() (+24 more)

### Community 25 - "MySQL Integration And Failure Tests"
Cohesion: 0.16
Nodes (33): github.com/gsoultan/gpool/vendors/mysql, target, target, T, killConnections(), TestPoolRecoversAfterConnectionsAreKilled(), TestPoolReplacesItsOnlyConnectionWhenKilled(), TestPoolSurvivesRepeatedKillsUnderLoad() (+25 more)

### Community 26 - "Vendor Config And Imports"
Cohesion: 0.07
Nodes (12): errors, github.com/clickhouse/clickhouse/go/v2, github.com/gsoultan/gpool/pkg/sqldriver, github.com/microsoft/go/mssqldb, runtime, time, Config, Config (+4 more)

### Community 27 - "Scale And Capability Benchmarks"
Cohesion: 0.13
Nodes (26): BenchmarkGpoolAcquireRelease(), BenchmarkGpoolQueryIterator(), BenchmarkGpoolQueryRow(), BenchmarkGpoolQueryRowStress(), BenchmarkGpoolResetQuery(), B, Config, Pool (+18 more)

### Community 28 - "CDC Package Imports"
Cohesion: 0.13
Nodes (14): database/sql, encoding/hex, fmt, github.com/go/mysql/org/go/mysql/mysql, github.com/go/mysql/org/go/mysql/replication, github.com/go/sql/driver/mysql, github.com/gsoultan/gpool/pkg/gpool/cdc, io/fs (+6 more)

### Community 29 - "Proxy Session Startup"
Cohesion: 0.13
Nodes (9): newRelay(), cutCString(), CancelFunc, Context, Mutex, Reader, Writer, parameterOf() (+1 more)

### Community 30 - "ClickHouse Pool Tests"
Cohesion: 0.15
Nodes (25): Config, github.com/gsoultan/gpool/vendors/clickhouse, Options, Connector, Duration, Pool, New(), newFromConfig() (+17 more)

### Community 31 - "Proxy And Stream Imports"
Cohesion: 0.15
Nodes (12): bufio, crypto/tls, encoding/binary, github.com/jackc/pglogrepl, github.com/jackc/pgx/v5/pgconn, github.com/jackc/pgx/v5/pgproto3, io, iter (+4 more)

### Community 32 - "Test Database Script"
Cohesion: 0.14
Nodes (23): ALL_ENGINES, CH_PASSWORD, cmd_down(), cmd_env(), cmd_status(), cmd_up(), die(), dsn_of() (+15 more)

### Community 33 - "MySQL CDC Subscriber"
Cohesion: 0.16
Nodes (9): MySQL, Position, BinlogStreamer, BinlogSyncer, Context, DB, GTIDSet, Mutex (+1 more)

### Community 34 - "Proxy Backend And Statement Replay"
Cohesion: 0.15
Nodes (11): Bool, Context, Reader, Writer, Config, Context, networkAddress(), closeStatement() (+3 more)

### Community 35 - "Pool And Subscriber Mocks"
Cohesion: 0.13
Nodes (6): EventStream, fakePool, fakeSubscriber, Context, Int32, Stat

### Community 36 - "Proxy Integration Tests"
Cohesion: 0.26
Nodes (23): cachingURL(), connect(), Config, T, startProxy(), startProxyLimited(), TestProxyBoundsBackendsAcrossIndependentClients(), TestProxyBoundsPreparedStatementsOnTheServer() (+15 more)

### Community 37 - "SQL Server CDC Integration Tests"
Cohesion: 0.25
Nodes (19): github.com/gsoultan/gpool/vendors/mssql/cdc, collect(), contains(), dsn(), fixture, Config, DB, Duration (+11 more)

### Community 38 - "database/sql Pool Adapter"
Cohesion: 0.15
Nodes (9): Context, Stmt, newConnWrapper(), Connector, connWrapper, Context, translate(), adapter (+1 more)

### Community 39 - "sqldriver Pool Tests"
Cohesion: 0.22
Nodes (22): Config, New(), Config, Pool, T, newTestPool(), TestAbandonedTransactionIsUnwoundOnRelease(), TestAcquireAfterCloseFailsFast() (+14 more)

### Community 40 - "Event Shape Notes"
Cohesion: 0.13
Nodes (22): Event.Position Is An Opaque String, Not A Number, Event.Timestamp Is The Source's Commit Time, Event.Transaction Groups One Commit, orderChanges Merges Instances by (start_lsn, seqval), Clone the GTID set: BinlogSyncer retains and mutates it, MySQL values keep the binlog parser's native Go types, SHOW BINARY LOG STATUS replaces SHOW MASTER STATUS in 8.4, Tagged positions: gtid:<set> or file:<name>:<offset> (+14 more)

### Community 41 - "MySQL Binlog Stream"
Cohesion: 0.14
Nodes (14): BinlogEvent, mysqlEventStream, EventType, opOf(), BinlogStreamer, BinlogSyncer, Bool, CancelFunc (+6 more)

### Community 42 - "Permit Accounting"
Cohesion: 0.17
Nodes (11): Context, Int32, newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate() (+3 more)

### Community 43 - "Fake database/sql Driver"
Cohesion: 0.15
Nodes (6): Bool, Context, NamedValue, Stmt, fakeDriverConn, fakeTx

### Community 44 - "Rows, Row And Batch Results"
Cohesion: 0.16
Nodes (7): Row, Rows, Bool, closeRows(), batchResults, failedBatchResults, rowCursor

### Community 45 - "Fake Connector Fixtures"
Cohesion: 0.17
Nodes (6): Int32, Value, bareConn, fakeConnector, fakeRows, fakeStmt

### Community 46 - "Rows And Row Unit Tests"
Cohesion: 0.27
Nodes (15): newRow(), connWrapper, newRows(), T, TestErrorRowDefersTheError(), TestResultReportsRowsAffected(), TestRowReleaseWithoutScanClosesTheResultSet(), TestRowsAllClosesOnEarlyBreak() (+7 more)

### Community 47 - "Prepared Statement Parsing"
Cohesion: 0.22
Nodes (9): bindName(), cString(), cutBytes(), parseName(), statementNameOf(), targetName(), bytes, statement (+1 more)

### Community 48 - "Transaction Wrapper"
Cohesion: 0.20
Nodes (7): Tx, Bool, connWrapper, Context, newTx(), pgTx, TxOptions

### Community 49 - "PostgreSQL Pool Facade"
Cohesion: 0.18
Nodes (6): newConnWrapper(), connWrapper, Context, Postgres, Stat, translate()

### Community 50 - "pgx Rows Wrapper"
Cohesion: 0.16
Nodes (4): Field, Bool, Seq, pgRows

### Community 52 - "pgx Driver Adapter"
Cohesion: 0.30
Nodes (6): closeConn(), Config, ConnConfig, Context, pgConn, pgxDriver

### Community 53 - "Byte Relay"
Cohesion: 0.27
Nodes (6): endsTransactionUnit(), flushIfDrained(), Reader, Writer, namesStatement(), relay

### Community 54 - "Row Cursors"
Cohesion: 0.18
Nodes (6): pgRows, Bool, newRow(), errorRow, pgRow, rowCursor

### Community 55 - "MySQL Column Cache"
Cohesion: 0.29
Nodes (6): columns, TableMapEvent, Context, DB, Mutex, newColumns()

### Community 56 - "Prepared Statement Bound Tests"
Cohesion: 0.53
Nodes (10): newStatements(), T, parseMessage(), TestStatementsCopyWhatTheyRemember(), TestStatementsEvictTheLeastRecentlyUsed(), TestStatementsForget(), TestStatementsHoldTheirLimit(), TestStatementsIgnoreTheUnnamedStatement() (+2 more)

### Community 57 - "PgBouncer Stacking Tests"
Cohesion: 0.45
Nodes (10): Config, Pool, T, hammer(), newPooledPool(), pgBouncerURL(), TestPgBouncerStatDescribesTheProxyHop(), TestPgBouncerTransactionsAreAtomic() (+2 more)

### Community 59 - "Multi-Database Architecture"
Cohesion: 0.31
Nodes (10): Multi-Database Engine Pool Registry, Step 10: Multi-Database Support, Independent Replication Slot per Node, Pool(name) Naming Decision, Engine: Named Registries of Pools and Subscribers, One Pool Per Database, Sharing Nothing, Multi-Database Tests, Several Databases Means Several Pools (+2 more)

### Community 61 - "LISTEN/NOTIFY Capability"
Cohesion: 0.29
Nodes (5): Notification, Notifier, Context, connWrapper, quoteIdentifier()

### Community 63 - "MySQL Position Tracker"
Cohesion: 0.31
Nodes (5): tracker, GTIDSet, newTracker(), position, GTIDSet

### Community 64 - "Proxy Entry Point"
Cohesion: 0.28
Nodes (8): Config, hash(), main(), parseFlags(), run(), flag, os/signal, syscall

### Community 65 - "SQL Server CDC Config"
Cohesion: 0.22
Nodes (4): regexp, Config, Duration, changesSQL()

### Community 66 - "Bulk Copy Capability"
Cohesion: 0.31
Nodes (6): BulkCopier, CopyRequest, Context, connWrapper, Postgres, validateCopyRequest()

### Community 67 - "Proxy Throughput Benchmarks"
Cohesion: 0.36
Nodes (6): benchmarkTarget(), BenchmarkThroughput(), B, Pool, warm(), strconv

### Community 69 - "pgx Transaction Methods"
Cohesion: 0.43
Nodes (3): Bool, Context, pgTx

### Community 70 - "Graph Reconciliation Script"
Cohesion: 0.43
Nodes (6): is_package(), main(), package_label(), Recover a readable import path from a synthesized package id.      The id is los, Return the extraction with package nodes added and unresolvable edges dropped., reconcile()

### Community 71 - "Transaction Unit Tests"
Cohesion: 0.57
Nodes (6): newTx(), T, TestTxCommitWithDeferredRollback(), TestTxRefusesUseAfterSettle(), TestTxRollbackWithDeferredRollback(), TestTxSettlesExactlyOnce()

### Community 72 - "MySQL Binlog Syncer Config"
Cohesion: 0.33
Nodes (5): BinlogSyncerConfig, newFromConfig(), Config, New(), splitHostPort()

### Community 73 - "Stat Interface Composition"
Cohesion: 0.33
Nodes (3): Acquisition, Occupancy, Stat

### Community 74 - "pgx Single Row"
Cohesion: 0.47
Nodes (3): Bool, connWrapper, pgRow

### Community 76 - "Proxy Userlist"
Cohesion: 0.40
Nodes (4): checkSecretPermissions(), loadUserlist(), File, userlist

### Community 77 - "SQL Server Pool Config"
Cohesion: 0.50
Nodes (3): Connector, Duration, Config

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
- **9 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `CLI Proxy Mode` and `Gpool: A Go Connection Pooling & CDC Library`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Gpool Core Memory` and `Supported Databases`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `An Unknown User Runs the Full Exchange Against a Decoy Verifier` and `Errors Are Classified by Code, Never by Message Text`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._
- **Why does `Core` connect `Pooling Core Engine` to `Pooling Core Unit Tests`, `Pool Interface`, `Permit Accounting`, `PostgreSQL Pool Facade`, `Proxy Listener And Warm-Up`, `Proxy And Stream Imports`?**
  _High betweenness centrality (0.033) - this node is a cross-community bridge._
- **Why does `Proxy` connect `Proxy Listener And Warm-Up` to `Proxy Backend And Statement Replay`, `Pooling Core Engine`, `Proxy Userlist`, `SCRAM Authentication`, `Proxy Session Startup`, `Proxy And Stream Imports`?**
  _High betweenness centrality (0.032) - this node is a cross-community bridge._
- **Why does `fakeTx` connect `Batch And Bulk Copy Mocks` to `Proxy Listener And Warm-Up`, `Proxy And Stream Imports`?**
  _High betweenness centrality (0.024) - this node is a cross-community bridge._
- **What connects `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._