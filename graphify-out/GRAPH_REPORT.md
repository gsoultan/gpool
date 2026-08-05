# Graph Report - .  (2026-08-05)

## Corpus Check
- 90 files · ~51,101 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 899 nodes · 1921 edges · 46 communities (43 shown, 3 thin omitted)
- Extraction: 83% EXTRACTED · 17% INFERRED · 0% AMBIGUOUS · INFERRED: 325 edges (avg confidence: 0.84)
- Token cost: 120,848 input · 0 output

## Community Hubs (Navigation)
- CDC Stream Reader Loop
- CDC Subscriber and Publication Management
- Batch and Copy Types
- Pool Core and Connection Lifecycle
- Baseline Benchmarks
- Engine Pool and Subscriber Registry
- Copy Rows and Capability Tests
- Core Interface Test Doubles
- Concurrency and Lifecycle Invariants
- Gpool Benchmarks
- CDC Interfaces and Integration Tests
- Scale and Footprint
- Pool Lifecycle Unit Tests
- Testing Doctrine and Benchmark Hygiene
- CDC Internals and LSN Tracking
- CDC Replication Semantics
- Removed Proxy and Library-Only Shape
- Permit Token Channel
- Package Architecture and Conventions
- Pooling Contract and Release Gate
- Row and Batch Results
- Vendor Registry and Factory
- Core Package Imports
- Exec Result and Transaction
- CDC Config Unit Tests
- Rows and Row Unit Tests
- Interface Design and Role Profiles
- Rows Iterator
- CDC Package Imports
- Knowledge Tooling
- Connection Wrapper
- Superseded Template Rules
- Serena Memory Conventions
- LISTEN and NOTIFY
- Pool Statistics
- Multi-Database Support
- Bulk Copy
- Module Dependencies
- Junie Guidelines
- Command Tag Result
- Pool Config Parsing
- CDC Config Validation
- Vendor Registration
- Single Row Result
- Deferred Error Row
- Batcher Interface

## God Nodes (most connected - your core abstractions)
1. `newPool()` - 30 edges
2. `Postgres` - 30 edges
3. `pgEventStream` - 25 edges
4. `Postgres` - 24 edges
5. `idleConn` - 23 edges
6. `NewEngine()` - 18 edges
7. `Engine` - 16 edges
8. `NewPool()` - 16 edges
9. `newTestPool()` - 16 edges
10. `Pool` - 15 edges

## Surprising Connections (you probably didn't know these)
- `Permit Token Channel (Capacity)` --semantically_similar_to--> `Bounded Queues, Caps and Deadlines (DoS Resistance)`  [INFERRED] [semantically similar]
  .serena/memories/pool.md → AGENTS.md
- `Serena Persistent Memory` --references--> `Gpool Core`  [INFERRED]
  .junie/agents.md → .serena/memories/core.md
- `Avoid Stuttering` --conceptually_related_to--> `Package Conventions`  [INFERRED]
  .junie/agents.md → .serena/memories/architecture.md
- `Transaction-Mode Pooling Constraints` --conceptually_related_to--> `ResetQuery Couples to Query Exec Mode`  [INFERRED]
  .junie/plans/gpool-lib-init.md → .serena/memories/pool.md
- `Bound What Multiplies` --semantically_similar_to--> `Profile: Security Architect`  [INFERRED] [semantically similar]
  .serena/memories/scale.md → AGENTS.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **The Pooling Contract: Clean State for the Next Caller** — _serena_memories_pool_recyclable_gate, _serena_memories_pool_transaction_gate, _serena_memories_pool_listen_unlisten_gate, readme_recyclable_release_gate, readme_unlisten_cleanup, agents_recyclable_gate, agents_session_state_needs_its_own_gate, readme_reset_query [INFERRED 0.85]
- **Optional Capabilities Kept Off Pool and Conn** — readme_optional_capabilities, readme_bulk_copier, readme_batcher, readme_notifier, agents_optional_capabilities, agents_interface_segregation, _serena_memories_pool_optional_capabilities [INFERRED 0.85]
- **Stat: Occupancy plus Acquisition as a Sizing Signal** — readme_stat_composition, readme_acquisition_counters, readme_only_waits_are_timed, agents_stat_composition, _serena_memories_pool_statistics, _serena_memories_pool_blocking_path_timing [INFERRED 0.85]

## Communities (46 total, 3 thin omitted)

### Community 0 - "CDC Stream Reader Loop"
Cohesion: 0.06
Nodes (42): BackendMessage, Event, Op, pgEventStream, DeleteMessage, fakeStream, InsertMessage, Seq (+34 more)

### Community 1 - "CDC Subscriber and Publication Management"
Cohesion: 0.10
Nodes (34): Postgres, slices, strings, dedupe(), difference(), hasSQLState(), intersection(), isDuplicateObject() (+26 more)

### Community 2 - "Batch and Copy Types"
Cohesion: 0.06
Nodes (23): CopyFromSource, FieldDescription, Batch, BatchQuery, BatchResults, Identifier, LargeObjects, T (+15 more)

### Community 3 - "Pool Core and Connection Lifecycle"
Cohesion: 0.08
Nodes (19): Conn, Int64, Duration, Time, closeConn(), Bool, CancelFunc, Config (+11 more)

### Community 4 - "Baseline Benchmarks"
Cohesion: 0.08
Nodes (41): BenchmarkPgBouncer(), BenchmarkPgxPool(), BenchmarkPgxPoolStress(), BenchmarkStdlib(), BenchmarkStdlibStress(), B, database/sql, errors (+33 more)

### Community 5 - "Engine Pool and Subscriber Registry"
Cohesion: 0.10
Nodes (32): net/url, Engine, Pool, databaseNamed(), Config, T, multiDBEngine(), provisionDatabases() (+24 more)

### Community 6 - "Copy Rows and Capability Tests"
Cohesion: 0.13
Nodes (31): CopyRows, sliceRows, T, scratchTable(), TestCopyFromLoadsRows(), TestCopyFromRollsBackOnSourceError(), TestCopyFromValidatesTheRequest(), TestListenDoesNotLeakToTheNextCaller() (+23 more)

### Community 7 - "Core Interface Test Doubles"
Cohesion: 0.09
Nodes (8): EventStream, Acquisition, fakePool, fakeSubscriber, Occupancy, Stat, Context, Int32

### Community 8 - "Concurrency and Lifecycle Invariants"
Cohesion: 0.11
Nodes (28): run() Defer Order, Every Drain Is Bounded, Validate and Default Config at Construction, No Callback Invoked Under Its Own Lock, No Panic Reaches the Caller, Invariants (Memory), One Goroutine Owns a Connection, Only the Blocking Path Is Timed (+20 more)

### Community 9 - "Gpool Benchmarks"
Cohesion: 0.16
Nodes (24): BenchmarkGpoolAcquireRelease(), BenchmarkGpoolQueryIterator(), BenchmarkGpoolQueryRow(), BenchmarkGpoolQueryRowStress(), BenchmarkGpoolResetQuery(), B, Config, TB (+16 more)

### Community 10 - "CDC Interfaces and Integration Tests"
Cohesion: 0.18
Nodes (16): ReplicationManager, Stream, Subscriber, TableManager, collect(), Config, Duration, T (+8 more)

### Community 11 - "Scale and Footprint"
Cohesion: 0.13
Nodes (22): sync.WaitGroup.Go from Stdlib, Bounded Statement and Description Caches, Dropped golang.org/x/sync, Step 12: Scale and Footprint, Token Channel Replaces Counting Semaphore, Bound What Multiplies, Cache Capacity Default 64, DisableCache (+14 more)

### Community 12 - "Pool Lifecycle Unit Tests"
Cohesion: 0.22
Nodes (21): newConnWrapper(), Config, Postgres, T, newTestPool(), TestAcquireAfterCloseFailsFast(), TestCloseIsIdempotent(), TestConnReleaseIsIdempotent() (+13 more)

### Community 13 - "Testing Doctrine and Benchmark Hygiene"
Cohesion: 0.13
Nodes (21): Testing Standards, Step 11: Production Hardening, Never Recycle Caller-Owned Objects, Rows vs Row Ownership, Benchmark Hygiene, Match Capacity on Both Sides, 200k Iterations Before allocs/op Settles, AllocsPerRun Panics Under t.Parallel (+13 more)

### Community 14 - "CDC Internals and LSN Tracking"
Cohesion: 0.11
Nodes (21): advance() CAS Loop, catchUp (idle WAL release), CDC Internals Memory, Control Connection, emit (keepalive-aware handoff), LSN Position Tracking (received/lastPushed/flushed), quoteQualifiedName, Replication Connection (+13 more)

### Community 15 - "CDC Replication Semantics"
Cohesion: 0.12
Nodes (19): CDC Control and Replication Connection Split, Integrated CDC via Logical Replication, mem:cdc, Confirm Only After the Work Is Done, Caller-Owned Maps and Slices Allocated Fresh, CDC Cannot Be Pooled, CDC Fixtures Drop Their Slot, Logical Replication Cannot Be Pooled (+11 more)

### Community 16 - "Removed Proxy and Library-Only Shape"
Cohesion: 0.12
Nodes (18): AGENTS.md Wins Over These Guidelines, Project Shape: Gpool Is a Library, CLI Proxy Mode, Gpool Library Initialization Plan, Go 1.26 Iterator API, PgBouncer Replacement, Step 4: Wire Proxy and CLI Removed, Transaction-Mode Pooling Constraints (+10 more)

### Community 17 - "Permit Token Channel"
Cohesion: 0.21
Nodes (10): Context, newPermits(), T, TestPermitsAcquireHonoursCancellation(), TestPermitsBoundConcurrency(), TestPermitsDrain(), TestPermitsFastPathDoesNotAllocate(), TestPermitsHoldTheBoundUnderContention() (+2 more)

### Community 18 - "Package Architecture and Conventions"
Cohesion: 0.16
Nodes (17): Inverted /internal Layout, File and Folder Readability Limits, VendorFactory Pattern, Architecture, Compile-Time Interface Proofs, Package Conventions, Interface Segregation, Minimal Dependencies (+9 more)

### Community 19 - "Pooling Contract and Release Gate"
Cohesion: 0.23
Nodes (17): Classify Errors by SQLSTATE, LISTEN / UNLISTEN * Gate, Pooling Mode Is Usage, Not Config, recyclable() Release Gate, ResetQuery Couples to Query Exec Mode, Transaction Gate Is the Pooling Contract, Profile: Database Architect, Profile: Network Architect (+9 more)

### Community 20 - "Row and Batch Results"
Cohesion: 0.16
Nodes (7): Row, Rows, Bool, closeRows(), batchResults, failedBatchResults, rowCursor

### Community 21 - "Vendor Registry and Factory"
Cohesion: 0.35
Nodes (15): PoolFactory, SubscriberFactory, Vendor, NewPool(), NewSubscriber(), RegisterPool(), RegisterSubscriber(), T (+7 more)

### Community 22 - "Core Package Imports"
Cohesion: 0.30
Nodes (6): context, github.com/gsoultan/gpool/pkg/gpool, github.com/jackc/pgx/v5, iter, math/rand/v2, sync/atomic

### Community 23 - "Exec Result and Transaction"
Cohesion: 0.19
Nodes (5): Result, Tx, Bool, Context, pgTx

### Community 24 - "CDC Config Unit Tests"
Cohesion: 0.31
Nodes (14): firstErr(), Config, T, longSlotName(), TestClosedSubscriberRefusesWork(), TestConfigDefaults(), TestConfigValidate(), TestIsTrackingAndGetTablesAreIsolated() (+6 more)

### Community 25 - "Rows and Row Unit Tests"
Cohesion: 0.30
Nodes (14): newRow(), newRows(), T, TestErrorRowDefersTheError(), TestResultReportsRowsAffected(), TestRowReleaseWithoutScanClosesTheResultSet(), TestRowsAllClosesOnEarlyBreak(), TestRowScanAfterReleaseIsRefused() (+6 more)

### Community 26 - "Interface Design and Role Profiles"
Cohesion: 0.24
Nodes (14): Optional Capabilities (Memory), Interface Segregation by Composition, Optional Capabilities Reached by Type Assertion, Seven Role Profiles, One Pool per Database, Sharing Nothing, Profile: Software Architect, gpool.Stat = Occupancy + Acquisition, Vendor Self-Registration Registry (+6 more)

### Community 27 - "Rows Iterator"
Cohesion: 0.16
Nodes (5): Field, Bool, connWrapper, Seq, pgRows

### Community 28 - "CDC Package Imports"
Cohesion: 0.23
Nodes (7): github.com/gsoultan/gpool/pkg/gpool/cdc, github.com/jackc/pglogrepl, github.com/jackc/pgx/v5/pgproto3, sync, time, init(), newFromConfig()

### Community 29 - "Knowledge Tooling"
Cohesion: 0.20
Nodes (11): Graphify Knowledge Graph, Mandatory Workflow Rules, Obsidian Agentic Second Brain, rtk Token Optimization, Serena Persistent Memory, Hierarchy of Reading (Levels 0-4), RTK - Rust Token Killer, sqz (Compression and Dedup) (+3 more)

### Community 30 - "Connection Wrapper"
Cohesion: 0.36
Nodes (4): Bool, Context, connWrapper, Postgres

### Community 31 - "Superseded Template Rules"
Cohesion: 0.22
Nodes (10): Integration Tests Moved to integration/, Layered Architecture Pattern, No Build Target, SQL Externalization Not Applicable, Superseded Service Template Rules, Restricted sync.Pool Usage, mem:invariants, Test Layout (+2 more)

### Community 32 - "Serena Memory Conventions"
Cohesion: 0.22
Nodes (10): Gpool Core, mem:memory_maintenance, Memory Add/Update Threshold, Memory Discovery Model, mem: Reference Convention, Dense Agent Note Style, Serena Project Configuration (gpool), graphify_reconcile.py Reconciliation Step (+2 more)

### Community 33 - "LISTEN and NOTIFY"
Cohesion: 0.29
Nodes (5): Notification, Notifier, Context, connWrapper, quoteIdentifier()

### Community 35 - "Multi-Database Support"
Cohesion: 0.33
Nodes (9): Multi-Database Engine Pool Registry, Step 10: Multi-Database Support, Independent Replication Slot per Node, Pool(name) Naming Decision, DefaultPool and DefaultSubscriber Resolution, Engine Named Pool and Subscriber Registry, integration/multidb_test.go, Engine Facade (+1 more)

### Community 36 - "Bulk Copy"
Cohesion: 0.31
Nodes (6): BulkCopier, CopyRequest, Context, connWrapper, Postgres, validateCopyRequest()

### Community 37 - "Module Dependencies"
Cohesion: 0.22
Nodes (9): github.com/gsoultan/gpool, github.com/jackc/pgio, github.com/jackc/pglogrepl, github.com/jackc/pgpassfile, github.com/jackc/pgservicefile, github.com/jackc/pgx/v5, github.com/jackc/puddle/v2, golang/org/x/sync (+1 more)

### Community 38 - "Junie Guidelines"
Cohesion: 0.25
Nodes (8): Avoid Stuttering, PgBouncer Transaction Mode, Avoid Stuttering Rule, Interface Segregation Principle, Junie Guidelines, No Memory Leaks Rule, PgBouncer Best Practices Reference, Post-Task Cleanup and Maintenance

### Community 39 - "Command Tag Result"
Cohesion: 0.25
Nodes (3): github.com/jackc/pgx/v5/pgconn, CommandTag, pgResult

### Community 40 - "Pool Config Parsing"
Cohesion: 0.32
Nodes (4): cacheCapacity(), ConnConfig, Duration, Config

### Community 41 - "CDC Config Validation"
Cohesion: 0.33
Nodes (3): Config, regexp, Duration

### Community 42 - "Vendor Registration"
Cohesion: 0.33
Nodes (3): fmt, init(), newFromConfig()

### Community 43 - "Single Row Result"
Cohesion: 0.47
Nodes (3): Bool, connWrapper, pgRow

## Ambiguous Edges - Review These
- `Minimal Dependencies` → `Dropped golang.org/x/sync Dependency`  [AMBIGUOUS]
  .serena/memories/architecture.md · relation: conceptually_related_to

## Knowledge Gaps
- **26 isolated node(s):** `Batcher`, `BulkCopier`, `Notifier`, `connWrapper`, `Postgres` (+21 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Minimal Dependencies` and `Dropped golang.org/x/sync Dependency`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `Postgres` connect `Pool Core and Connection Lifecycle` to `Permit Token Channel`, `Core Package Imports`, `Core Interface Test Doubles`?**
  _High betweenness centrality (0.044) - this node is a cross-community bridge._
- **Why does `fakeTx` connect `Batch and Copy Types` to `Pool Core and Connection Lifecycle`, `Command Tag Result`?**
  _High betweenness centrality (0.040) - this node is a cross-community bridge._
- **Why does `pgEventStream` connect `CDC Stream Reader Loop` to `CDC Subscriber and Publication Management`, `CDC Package Imports`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **Are the 13 inferred relationships involving `newPool()` (e.g. with `TestCopyFromLoadsRows()` and `TestCopyFromRollsBackOnSourceError()`) actually correct?**
  _`newPool()` has 13 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Batcher`, `BulkCopier`, `Notifier` to the rest of the system?**
  _26 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `CDC Stream Reader Loop` be split into smaller, more focused modules?**
  _Cohesion score 0.06398730830248546 - nodes in this community are weakly interconnected._