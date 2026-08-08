# Graph Report - .  (2026-08-08)

## Corpus Check
- 175 files · ~123,039 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2040 nodes · 4601 edges · 78 communities (68 shown, 10 thin omitted)
- Extraction: 86% EXTRACTED · 14% INFERRED · 0% AMBIGUOUS · INFERRED: 625 edges (avg confidence: 0.85)
- Token cost: 192,391 input · 0 output

## Community Hubs (Navigation)
- PostgreSQL CDC Subscriber
- SQL Server CDC Stream
- PostgreSQL Replication Stream
- Pooling Engine Core
- database/sql Row Scanning
- Pooling Core Tests
- gpool Pool Benchmarks
- PostgreSQL Batch and Copy
- Engine and CDC Interfaces
- Bulk Copy Capability Tests
- Comparative Pool Benchmarks
- Project History and Plans
- Junie Workflow Rules
- gpool Interface Fakes
- Module Dependency Graph
- PostgreSQL Pool Construction
- PostgreSQL Pool Interfaces
- CDC Internals Notes
- Proxy and PgBouncer Notes
- Project Invariants
- CDC Integration Tests
- database/sql Pool Tests
- SQL Server CDC Notes
- Proxy SCRAM Authentication
- SQL Server Vendor
- Pooling Configuration
- Pool Internals Notes
- Proxy Server Lifecycle
- database/sql Connection Wrapper
- Proxy Session Startup
- MySQL CDC Dependencies
- MySQL Pool Vendor
- database/sql Transactions
- Scale and Footprint Notes
- MySQL CDC Subscriber
- Proxy Backend Connection
- MySQL CDC Integration Tests
- CDC Position Semantics
- Test Database Script
- MySQL Binlog Stream
- PostgreSQL Row Results
- Permit Token Channel
- PostgreSQL Row Tests
- Hardening and Teardown Notes
- Proxy Integration Tests
- Proxy Command Line
- Benchmark Hygiene Notes
- Proxy Prepared Statements
- Allocation and Goroutine Budget
- PostgreSQL Rows Iterator
- PostgreSQL Transaction Tests
- MySQL Column Resolution
- ClickHouse Vendor
- Proxy Module Structure
- Proxy Message Relay
- Vendor Behaviour Notes
- MySQL Table Filter
- Pooling Statistics
- database/sql Conn Wrapper
- database/sql Tx Wrapper
- PostgreSQL Connection Handle
- MySQL Position Tracking
- PostgreSQL CDC Config
- database/sql Result
- PostgreSQL Config Tests
- MySQL Position Unit Tests
- PostgreSQL Pool Config
- Graph Reconcile Script
- MySQL Syncer Configuration
- Pool Statistics Interfaces
- PostgreSQL Command Result
- MySQL CDC Config
- Proxy Credential Files
- database/sql Pool Config
- PostgreSQL Deferred Error Row
- MySQL Schema Mismatch
- Runtime Capacity Control
- Native Vendor Note

## God Nodes (most connected - your core abstractions)
1. `gpool - Agent & Developer Profiles` - 52 edges
2. `Gpool: A Go Connection Pooling & CDC Library` - 44 edges
3. `session` - 40 edges
4. `newPool()` - 31 edges
5. `Proxy` - 30 edges
6. `newTestCore()` - 30 edges
7. `Pool Internals` - 29 edges
8. `Postgres` - 27 edges
9. `gpoolproxy - Cross-Application Pooling` - 26 edges
10. `Rows` - 25 edges

## Surprising Connections (you probably didn't know these)
- `Changed: Per-Connection Memory Cut Roughly 60%` --semantically_similar_to--> `Per-Connection Caches Are the Memory Cost`  [INFERRED] [semantically similar]
  CHANGELOG.md → .serena/memories/pool.md
- `CLI Proxy Mode` --conceptually_related_to--> `Gpool: A Go Connection Pooling & CDC Library`  [AMBIGUOUS]
  .junie/plans/gpool-lib-init.md → README.md
- `Post-Task Knowledge Update` --semantically_similar_to--> `Post-Task Maintenance Order`  [INFERRED] [semantically similar]
  .junie/agents.md → AGENTS.md
- `Post-Task Cleanup and Knowledge Update` --semantically_similar_to--> `Post-Task Maintenance Order`  [INFERRED] [semantically similar]
  .junie/guidelines.md → AGENTS.md
- `Changed: cdc.ReplicationManager Left cdc.Subscriber` --semantically_similar_to--> `ReplicationManager Is Optional, Reached by Type Assertion`  [INFERRED] [semantically similar]
  CHANGELOG.md → .serena/memories/cdc.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Token Channel Permit Set Replacing the Counting Semaphore** — _serena_memories_scale_semaphore_weighted_bottleneck, _serena_memories_scale_token_channel_permits, _serena_memories_pool_permits, _junie_plans_gpool_lib_init_token_channel_replacement, _serena_memories_scale_dropped_x_sync [INFERRED 0.85]
- **Opaque Position: the Contract Three Unlike Change Logs Share** — _serena_memories_cdc_event_position, _serena_memories_cdc_third_vendor_proof, _serena_memories_cdc_mssql_not_a_log_tail, _serena_memories_cdc_resume_contract, changelog_event_position_change, readme_positions_and_resuming [INFERRED 0.85]
- **The Release Gate Is What Makes Reuse Safe** — _serena_memories_pool_recyclable, _serena_memories_pool_transaction_gate, _serena_memories_pool_listen_gate, _serena_memories_proxy_release_condition, readme_release_gate, agents_pooling_contract [INFERRED 0.85]
- **Cross-Application Pooling Needs a Process, and Proves the Engine Is Vendor-Agnostic** — _serena_memories_proxy_cross_application_bound, _serena_memories_proxy_generality_proof, examples_gpoolproxy_readme_vendor_agnostic_proof, readme_cross_application_pooling, changelog_pkg_pooling_added, _serena_memories_architecture_gpoolproxy [INFERRED 0.85]

## Communities (78 total, 10 thin omitted)

### Community 0 - "PostgreSQL CDC Subscriber"
Cohesion: 0.07
Nodes (52): Postgres, math, firstErr(), Config, T, longSlotName(), TestClosedSubscriberRefusesWork(), TestConfigDefaults() (+44 more)

### Community 1 - "SQL Server CDC Stream"
Cohesion: 0.06
Nodes (43): captureInstance, change, sqlEventStream, SQLServer, Config, Duration, asInt(), describe() (+35 more)

### Community 2 - "PostgreSQL Replication Stream"
Cohesion: 0.05
Nodes (51): BackendMessage, Event, Op, pendingEvent, pgEventStream, DeleteMessage, fakeStream, InsertMessage (+43 more)

### Community 3 - "Pooling Engine Core"
Cohesion: 0.05
Nodes (31): Int64, Time, newCoarseClock(), Bool, C, CancelFunc, Config, Context (+23 more)

### Community 4 - "database/sql Row Scanning"
Cohesion: 0.06
Nodes (49): reflect, pgRows, errArity(), Bool, newRow(), Bool, connWrapper, Seq (+41 more)

### Community 5 - "Pooling Core Tests"
Cohesion: 0.09
Nodes (50): New(), Bool, Config, Context, Int32, Mutex, T, newTestCore() (+42 more)

### Community 6 - "gpool Pool Benchmarks"
Cohesion: 0.08
Nodes (49): BenchmarkGpoolAcquireRelease(), BenchmarkGpoolQueryIterator(), BenchmarkGpoolQueryRow(), BenchmarkGpoolQueryRowStress(), BenchmarkGpoolResetQuery(), B, Config, Pool (+41 more)

### Community 7 - "PostgreSQL Batch and Copy"
Cohesion: 0.05
Nodes (23): CopyFromSource, FieldDescription, Batch, BatchQuery, BatchResults, Identifier, LargeObjects, T (+15 more)

### Community 8 - "Engine and CDC Interfaces"
Cohesion: 0.08
Nodes (36): Stream, Subscriber, TableManager, net/url, Engine, databaseNamed(), Config, T (+28 more)

### Community 9 - "Bulk Copy Capability Tests"
Cohesion: 0.09
Nodes (40): BulkCopier, CopyRequest, CopyRows, sliceRows, Pool, T, scratchTable(), TestCopyFromLoadsRows() (+32 more)

### Community 10 - "Comparative Pool Benchmarks"
Cohesion: 0.09
Nodes (43): BenchmarkPgBouncer(), BenchmarkPgxPool(), BenchmarkPgxPoolStress(), BenchmarkStdlib(), BenchmarkStdlibStress(), B, benchmarkTarget(), BenchmarkThroughput() (+35 more)

### Community 11 - "Project History and Plans"
Cohesion: 0.06
Nodes (47): Avoid Stuttering in Filenames and Symbols, The /internal Rule Is Inverted for gpool, Integrated CDC via Logical Replication, Gpool Library Initialization Plan, Go 1.26 Iterator API, Multi-Database Engine Pool Registry, Step 10: Multi-Database Support, Independent Replication Slot per Node (+39 more)

### Community 12 - "Junie Workflow Rules"
Cohesion: 0.06
Nodes (46): No AI Co-Authorship Trailers, Mandatory Copyright Header, Modern Go 1.26 Syntax, Gpool Is a Library, Not a Service, Graphify Knowledge Graph, Gpool Project Guidelines (.junie), Hierarchical Discovery Before a Level 4 Full Read, Layered Architecture Pattern (+38 more)

### Community 13 - "gpool Interface Fakes"
Cohesion: 0.09
Nodes (23): EventStream, github.com/gsoultan/gpool/vendors/mssql/cdc, fakePool, fakeSubscriber, Context, Int32, Stat, collect() (+15 more)

### Community 14 - "Module Dependency Graph"
Cohesion: 0.06
Nodes (45): filippo/io/edwards25519, github.com/andybalholm/brotli, github.com/cespare/xxhash/v2, github.com/clickhouse/ch/go, github.com/clickhouse/clickhouse/go/v2, github.com/coreos/go/semver, github.com/go/faster/city, github.com/go/faster/errors (+37 more)

### Community 15 - "PostgreSQL Pool Construction"
Cohesion: 0.08
Nodes (27): newConnWrapper(), closeConn(), Config, ConnConfig, Context, Pool, newFromConfig(), Config (+19 more)

### Community 16 - "PostgreSQL Pool Interfaces"
Cohesion: 0.10
Nodes (11): ReplicationManager, context, database/sql/driver, github.com/gsoultan/gpool/pkg/gpool, github.com/gsoultan/gpool/pkg/pooling, github.com/jackc/pgx/v5, iter, sync/atomic (+3 more)

### Community 17 - "CDC Internals Notes"
Cohesion: 0.07
Nodes (39): SQL Externalization to .sql + go:embed, Compile-Time Interface Proofs, catchUp Advances the Position on Idle Keepalives, CDC Internals, run Defer Order Is Load-Bearing, ErrNoCDCSupport, flushed Is confirmed_flush_lsn, Set After yield Returns, captureInstancePattern Validation (+31 more)

### Community 18 - "Proxy and PgBouncer Notes"
Cohesion: 0.08
Nodes (37): PgBouncer Transaction Mode, PgBouncer Integration, examples/gpoolproxy as a Separate Module, emit Selects on the Keepalive Ticker, pgConn as a Type Parameter, Not an Interface, Cancellation Keys Are Per Session and Unguessable, A Library Cannot Bound Connections Across Applications, s.expect Swallows the Injected ParseComplete (+29 more)

### Community 19 - "Project Invariants"
Cohesion: 0.07
Nodes (37): Control and Replication Connections, Never Shared, Every Drain Is Bounded, Validate and Default Config at Construction, Confirm Only After the Work Is Done, Caller-Owned Maps and Slices Allocated Fresh, No Callback Invoked Under Its Own Lock, No Panic Reaches the Caller, Invariants (Memory) (+29 more)

### Community 20 - "CDC Integration Tests"
Cohesion: 0.15
Nodes (32): github.com/gsoultan/gpool/pkg/vendors/postgres/cdc, collect(), emailsOf(), Config, Duration, Pool, T, newCDCFixture() (+24 more)

### Community 21 - "database/sql Pool Tests"
Cohesion: 0.12
Nodes (27): Int32, Value, Config, New(), Config, Pool, T, newTestPool() (+19 more)

### Community 22 - "SQL Server CDC Notes"
Cohesion: 0.07
Nodes (35): vendors/mssql/cdc: a Package Inside the Pool Vendor's Module, Event.Timestamp Is the Source's Commit Time, AddTables Is Real DDL, Capture Mode 'all update old', Not 'all', Latency Is the Capture Job's, About Five Seconds, SQL Server CDC - Change Tables, A Package Inside the Pool Vendor's Module, Metadata Columns Recognised by Their __$ Prefix (+27 more)

### Community 23 - "Proxy SCRAM Authentication"
Cohesion: 0.14
Nodes (27): hash(), field(), splitGS2(), exchange(), T, parseServerFirst(), TestSCRAMAcceptsTheRightPassword(), TestSCRAMRejectsAlteredChannelBinding() (+19 more)

### Community 24 - "SQL Server Vendor"
Cohesion: 0.13
Nodes (32): github.com/gsoultan/gpool/vendors/mssql, Config, dsn(), Config, Pool, T, newPool(), scratchTable() (+24 more)

### Community 25 - "Pooling Configuration"
Cohesion: 0.08
Nodes (11): errors, github.com/microsoft/go/mssqldb, math/rand/v2, runtime, time, Config, Config, Duration (+3 more)

### Community 26 - "Pool Internals Notes"
Cohesion: 0.08
Nodes (30): SQL Server Agent Must Be Running, ActiveConnections Is an Exact Count, EvictIdle, LISTEN Is Session State the Transaction Gate Cannot See, Only the Blocking Path Is Timed, Pool Internals, recyclable(): the Release Gate, Core.SetMaxConns and gpool.Resizable (+22 more)

### Community 27 - "Proxy Server Lifecycle"
Cohesion: 0.09
Nodes (17): Addr, clientTLS(), Bool, CancelFunc, Config, Context, Mutex, Stat (+9 more)

### Community 28 - "database/sql Connection Wrapper"
Cohesion: 0.11
Nodes (11): Context, Stmt, newConnWrapper(), Connector, connWrapper, Context, Stat, translate() (+3 more)

### Community 29 - "Proxy Session Startup"
Cohesion: 0.13
Nodes (9): newRelay(), cutCString(), CancelFunc, Context, Mutex, Reader, Writer, parameterOf() (+1 more)

### Community 30 - "MySQL CDC Dependencies"
Cohesion: 0.16
Nodes (12): database/sql, encoding/hex, fmt, github.com/go/mysql/org/go/mysql/mysql, github.com/go/mysql/org/go/mysql/replication, github.com/go/sql/driver/mysql, github.com/gsoultan/gpool/pkg/gpool/cdc, github.com/jackc/pglogrepl (+4 more)

### Community 31 - "MySQL Pool Vendor"
Cohesion: 0.21
Nodes (26): github.com/gsoultan/gpool/vendors/mysql, target, Pool, newFromConfig(), eachTarget(), Config, Pool, T (+18 more)

### Community 32 - "database/sql Transactions"
Cohesion: 0.11
Nodes (9): Tx, Bool, Context, NamedValue, Stmt, bareConn, fakeDriverConn, fakeTx (+1 more)

### Community 33 - "Scale and Footprint Notes"
Cohesion: 0.12
Nodes (26): Bounded Statement and Description Caches, Dropped golang.org/x/sync, Step 12: Scale and Footprint, Token Channel Replaces Counting Semaphore, Minimal Dependencies: pgx/v5 and pglogrepl, go-mysql logging discarded (gpool does no logging), Per-Connection Caches Are the Memory Cost, Token Channel, Not a Counting Semaphore (+18 more)

### Community 34 - "MySQL CDC Subscriber"
Cohesion: 0.16
Nodes (9): MySQL, Position, BinlogStreamer, BinlogSyncer, Context, DB, GTIDSet, Mutex (+1 more)

### Community 35 - "Proxy Backend Connection"
Cohesion: 0.12
Nodes (11): Bool, Context, Reader, Writer, Config, Context, networkAddress(), Conn (+3 more)

### Community 36 - "MySQL CDC Integration Tests"
Cohesion: 0.30
Nodes (20): target, github.com/gsoultan/gpool/vendors/mysql/cdc, collect(), eachTarget(), fixture, Config, DB, Duration (+12 more)

### Community 37 - "CDC Position Semantics"
Cohesion: 0.10
Nodes (22): vendors/mysql/cdc: Its Own Module Nested in the Pool Vendor, checkResumable and ErrPositionBehindSlot, Event.Position Is an Opaque String, Not a Number, checkRetained and ErrPositionExpired, Subscribe Starts at the End; SubscribeFrom Resumes Inclusively, Clone the GTID set: BinlogSyncer retains and mutates it, resume advances only at commit (XID or literal COMMIT), SHOW BINARY LOG STATUS replaces SHOW MASTER STATUS in 8.4 (+14 more)

### Community 38 - "Test Database Script"
Cohesion: 0.16
Nodes (19): ALL_ENGINES, CH_PASSWORD, cmd_down(), cmd_env(), cmd_status(), cmd_up(), die(), dsn_of() (+11 more)

### Community 39 - "MySQL Binlog Stream"
Cohesion: 0.14
Nodes (14): BinlogEvent, mysqlEventStream, EventType, opOf(), BinlogStreamer, BinlogSyncer, Bool, CancelFunc (+6 more)

### Community 40 - "PostgreSQL Row Results"
Cohesion: 0.14
Nodes (8): Row, Rows, Bool, closeRows(), Context, batchResults, failedBatchResults, rowCursor

### Community 41 - "Permit Token Channel"
Cohesion: 0.17
Nodes (11): Context, Int32, newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate() (+3 more)

### Community 42 - "PostgreSQL Row Tests"
Cohesion: 0.19
Nodes (17): Bool, connWrapper, newRow(), newRows(), T, TestErrorRowDefersTheError(), TestResultReportsRowsAffected(), TestRowReleaseWithoutScanClosesTheResultSet() (+9 more)

### Community 43 - "Hardening and Teardown Notes"
Cohesion: 0.14
Nodes (20): sync.Pool Restricted to Non-Escaping Buffers, CDC Control and Replication Connection Split, Step 11: Production Hardening, Client-side table filter shared with the running stream, Table Management: Local List Updated Only After the Server Accepts, Idempotent Teardown, Never Recycle Caller-Owned Objects, Handle Is Returned by Value (+12 more)

### Community 44 - "Proxy Integration Tests"
Cohesion: 0.29
Nodes (19): cachingURL(), connect(), Config, T, startProxy(), TestProxyBoundsBackendsAcrossIndependentClients(), TestProxyForgetsClosedStatements(), TestProxyIsolatesIdenticallyNamedStatements() (+11 more)

### Community 45 - "Proxy Command Line"
Cohesion: 0.14
Nodes (12): Config, main(), parseFlags(), run(), flag, os/signal, syscall, Notification (+4 more)

### Community 46 - "Benchmark Hygiene Notes"
Cohesion: 0.15
Nodes (17): Integration Tests Live in integration/, Not tests/, Interleave the Targets at Each Concurrency, Benchmark Hygiene, Match Capacity on Both Sides, 200k Iterations Before allocs/op Settles, AllocsPerRun Panics Under t.Parallel, Benchmark Hygiene, Test Layout (+9 more)

### Community 47 - "Proxy Prepared Statements"
Cohesion: 0.22
Nodes (9): bindName(), closeStatement(), cString(), cutBytes(), parseName(), statementNameOf(), targetName(), bytes (+1 more)

### Community 48 - "Allocation and Goroutine Budget"
Cohesion: 0.16
Nodes (15): Zero-Allocation Hot Paths, No Memory Leaks, Stream Topology: One run Goroutine, Buffered Delivery, maintain: the One Background Goroutine, Goroutine Cost Is One, Total, Allocation Is Cheap Next to a Network Round Trip, Cost Per Caller Must Be Zero, Every Goroutine Has a Named Owner and a Termination Path (+7 more)

### Community 49 - "PostgreSQL Rows Iterator"
Cohesion: 0.15
Nodes (5): Field, Bool, connWrapper, Seq, pgRows

### Community 50 - "PostgreSQL Transaction Tests"
Cohesion: 0.24
Nodes (9): Bool, Context, newTx(), T, TestTxCommitWithDeferredRollback(), TestTxRefusesUseAfterSettle(), TestTxRollbackWithDeferredRollback(), TestTxSettlesExactlyOnce() (+1 more)

### Community 51 - "MySQL Column Resolution"
Cohesion: 0.24
Nodes (7): columns, TableMapEvent, Context, DB, Mutex, newColumns(), qualify()

### Community 52 - "ClickHouse Vendor"
Cohesion: 0.21
Nodes (10): Config, github.com/clickhouse/clickhouse/go/v2, github.com/gsoultan/gpool/pkg/sqldriver, Options, Connector, Duration, Pool, init() (+2 more)

### Community 53 - "Proxy Module Structure"
Cohesion: 0.32
Nodes (7): bufio, crypto/tls, encoding/binary, github.com/jackc/pgx/v5/pgconn, github.com/jackc/pgx/v5/pgproto3, io, net

### Community 54 - "Proxy Message Relay"
Cohesion: 0.30
Nodes (6): endsTransactionUnit(), flushIfDrained(), Reader, Writer, namesStatement(), relay

### Community 55 - "Vendor Behaviour Notes"
Cohesion: 0.20
Nodes (11): MySQL values keep the binlog parser's native Go types, The Transaction Gate Is the Pooling Contract, ClickHouse: analytical column store, no general transactions, database/sql vendor: about a hundred lines, driver.Value is not a closed set, SQL Server: ordinal placeholders, lenient DSN parser, MySQL and MariaDB: one implementation, two names, pkg/sqldriver pools driver.Conn, not *sql.DB (+3 more)

### Community 56 - "MySQL Table Filter"
Cohesion: 0.27
Nodes (5): filter, slices, RWMutex, newFilter(), normalize()

### Community 59 - "database/sql Tx Wrapper"
Cohesion: 0.31
Nodes (5): Bool, connWrapper, Context, newTx(), pgTx

### Community 61 - "MySQL Position Tracking"
Cohesion: 0.31
Nodes (5): tracker, GTIDSet, newTracker(), position, GTIDSet

### Community 62 - "PostgreSQL CDC Config"
Cohesion: 0.22
Nodes (4): regexp, Config, Duration, changesSQL()

### Community 64 - "PostgreSQL Config Tests"
Cohesion: 0.39
Nodes (8): T, TestConfigBoundsPerConnectionCaches(), TestConfigDefaultsGiveUsableCapacity(), TestConfigDefaultsPreserveExplicitValues(), TestConfigParseIsOrderIndependent(), TestConfigParseRejectsBadConnString(), TestConfigResetQuerySelectsACompatibleExecMode(), TestConfigValidate()

### Community 65 - "MySQL Position Unit Tests"
Cohesion: 0.44
Nodes (8): parsePosition(), T, TestFilterMatching(), TestFilterMutation(), TestParsePositionIsFlavourSpecific(), TestParsePositionRejectsAForeignPosition(), TestPositionDistinguishesItsTwoNotations(), TestPositionRoundTrips()

### Community 66 - "PostgreSQL Pool Config"
Cohesion: 0.29
Nodes (4): cacheCapacity(), ConnConfig, Duration, Config

### Community 67 - "Graph Reconcile Script"
Cohesion: 0.43
Nodes (6): is_package(), main(), package_label(), Recover a readable import path from a synthesized package id.      The id is los, Return the extraction with package nodes added and unresolvable edges dropped., reconcile()

### Community 68 - "MySQL Syncer Configuration"
Cohesion: 0.33
Nodes (5): BinlogSyncerConfig, newFromConfig(), Config, New(), splitHostPort()

### Community 69 - "Pool Statistics Interfaces"
Cohesion: 0.33
Nodes (3): Acquisition, Occupancy, Stat

### Community 72 - "Proxy Credential Files"
Cohesion: 0.40
Nodes (4): checkSecretPermissions(), loadUserlist(), File, userlist

### Community 73 - "database/sql Pool Config"
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
- **48 isolated node(s):** `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD`, `CH_PASSWORD`, `MSSQL_PASSWORD` (+43 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `CLI Proxy Mode` and `Gpool: A Go Connection Pooling & CDC Library`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Gpool Core Memory` and `Supported Databases`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `An Unknown User Runs the Full Exchange Against a Decoy Verifier` and `Errors Are Classified by Code, Never by Message Text`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._
- **Why does `session` connect `Proxy Session Startup` to `Proxy Backend Connection`, `Pooling Engine Core`, `Proxy Prepared Statements`, `Proxy Module Structure`, `Proxy Message Relay`, `Proxy Server Lifecycle`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `backend` connect `Proxy Backend Connection` to `Proxy Session Startup`, `Proxy Server Lifecycle`, `Proxy Module Structure`, `Proxy Prepared Statements`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **Why does `Core` connect `Pooling Engine Core` to `Pooling Core Tests`, `Permit Token Channel`, `PostgreSQL Pool Construction`, `Pooling Configuration`, `Proxy Server Lifecycle`, `database/sql Connection Wrapper`?**
  _High betweenness centrality (0.028) - this node is a cross-community bridge._
- **What connects `PREFIX`, `PG_PASSWORD`, `MY_PASSWORD` to the rest of the system?**
  _48 weakly-connected nodes found - possible documentation gaps or missing edges._