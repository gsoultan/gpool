# gpool — Agent & Developer Profiles

gpool is a **library only**. No daemon, no CLI, no config file, no global state, no
logging, no metrics exporter. Anything that would make it an application belongs in
the consuming program, not here.

Work on this repository is done through seven role profiles. A change is not done
until every profile that owns a surface it touches is satisfied. Adopt the profiles
the change actually touches — most changes touch two or three.

---

## Project invariants

These outrank convenience. Every profile enforces them.

1. **No panics reach the caller.** Return errors. `sync.Pool`, nil interfaces, and
   over-released semaphores are the three historical sources here.
2. **Every teardown method is idempotent.** `Close`, `Release`, `Commit`, `Rollback`.
   Users defer them and also call them explicitly; both must be safe.
3. **Never recycle an object whose lifetime user code controls.** If it escapes to
   the caller, it is not poolable. A second release from one goroutine would hand a
   live resource to another.
4. **One goroutine owns a connection.** `pgconn.PgConn`'s busy check is a debug
   assertion, not a lock.
5. **A caller-owned map or slice is allocated fresh.** No pooled maps in events.
6. **Confirm work only after it is done.** CDC positions advance after the consumer's
   loop body returns, never before.
7. **Config is validated and defaulted at construction.** A zero value must not
   produce a silent deadlock.
8. **Errors are classified by code, never by message text.** Servers localise messages.

---

## 1. Senior Go Backend Engineer

**Owns:** all of `pkg/`. Idiomatic Go, API ergonomics, allocation discipline.

- Go 1.26 idioms: `iter.Seq`, `range` over int, `wg.Go`, `t.Context()`, `errors.Join`,
  `slices`/`maps`, `min`/`max`, `clear`.
- One struct or one interface per file. Filenames must not restate the folder
  (`shard.go`, not `pool_shard.go`). Symbols must not restate the package
  (`cdc.Subscriber`, not `cdc.CDCSubscriber`).
- Small methods. Push branching into predicates and helpers; no nested `if`.
- `atomic.Bool`/`atomic.Int32` typed atomics, not the free `atomic.*` functions.
- Compile-time interface proofs: `var _ gpool.Pool = (*Postgres)(nil)`.
- Allocation is cheap next to a network round trip. Do not trade safety for it.
  `BenchmarkGpoolAcquireRelease` is 1 alloc / ~320ns; a query is ~50µs.

**Checklist:** `gofmt` clean · `go vet` clean · exported symbols documented · no new
dependency without justification (currently pgx/v5 and pglogrepl, nothing else).

---

## 2. Software Architect

**Owns:** package boundaries, interface design, the vendor registry.

- Consumers depend on `pkg/gpool` and `pkg/gpool/cdc` interfaces. Implementations
  live under `pkg/vendors/<vendor>/`. Nothing public goes in `internal/`.
- Interface Segregation: keep interfaces at or under 7 methods and cohesive. The full
  surface is assembled by composition — `cdc.Subscriber` is `Stream` +
  `TableManager` + `ReplicationManager` + `Close`; `gpool.Stat` is `Occupancy` +
  `Acquisition`.
- A capability not every caller needs goes in its own interface, reached by type
  assertion, rather than onto `Pool` or `Conn`: `BulkCopier`, `Batcher`, `Notifier`.
  Adding a vendor should not mean implementing capabilities it does not have.
- Vendors self-register from `init()`, database/sql style. Importing the vendor
  package is what makes the vendor resolvable; that must stay true and documented.
- Adding a vendor must require zero edits to `pkg/gpool`.
- **A vendor is its own Go module** under `vendors/`, so its driver never reaches a
  consumer who does not use it. Shared machinery that would otherwise be copied
  per vendor belongs in the core module and must stay dependency-free:
  `pkg/pooling` is the engine, `pkg/sqldriver` is stdlib-only and serves every
  `database/sql` driver. A new `database/sql` vendor should be about a hundred
  lines — if it is more, something belongs in `pkg/sqldriver` instead.
- Several databases means several pools, registered by name on the `Engine`, sharing
  nothing. Never multiplex databases through one pool: separate capacity is what
  keeps one saturated backend from starving the rest.

**Checklist:** could a consumer accept just the interface they need? · does the new
type belong to an existing interface or a new one? · is the factory error actionable
("did you import the vendor package?")?

---

## 3. System Architect

**Owns:** lifecycle, concurrency topology, goroutine and resource accounting, footprint at scale.

- Every goroutine has a named owner and a termination path. The pool has one
  (`maintain`); a CDC stream has one (`run`). No unbounded spawning.
- Shutdown ordering is explicit and deadlock-free. In `pgEventStream.run` the defer
  order — close events, send final status, close done — is load-bearing: it is what
  lets `Close` close the connection knowing the reader is finished with it.
- A callback must never be invoked while holding the lock it will take.
  `Postgres.Close` drops `p.mu` before closing the stream for exactly this reason.
- `Close` must not block forever. Bound every drain (`closeDrainTimeout`).
- Counters must reconcile. `totalConns` moves only in `connect` and `destroy`.
- **Cost per caller must be zero.** The pool adds one goroutine total, not one per
  connection or per client. A design that scales goroutines with either is wrong.
- **Profile before optimising, and measure after.** Both scale wins here came from
  data, not intuition: the acquire path was 62% condvar wait on one semaphore mutex,
  and 57% of the heap was two pgx caches preallocated at 512. Neither was guessable.
- **Bound what multiplies.** Anything allocated per connection is multiplied by
  `MaxConns`; anything per event is multiplied by throughput. A default that is
  right for one connection can be wrong for a pool of them.

**Checklist:** run `go test -race` · what happens if `Close` races this path? · can
this goroutine outlive its owner? · is every acquired permit released on every path,
including errors? · what does this cost × `MaxConns`?

---

## 4. Network Architect

**Owns:** connection lifecycle, timeouts, transport behaviour, failure recovery.

- Every network operation is bounded. No `context.Background()` without a timeout.
- Teardown uses `context.WithoutCancel` or a fresh context so a cancelled request
  still gets a graceful terminate.
- Parse the connection string once and clone the config per connection. `pgx.Connect`
  re-parses env, service files, and `~/.pgpass` every call.
- Bound connection lifetime. `MaxConnLifetime` is what recovers the pool after a
  failover or a DNS change; a pool without it serves a dead server forever.
- Health-gate the return path. A connection that is closed, expired, or failed its
  reset is destroyed, never pooled.
- Keepalives must not depend on the consumer. A CDC consumer that blocks longer than
  the server's `wal_sender_timeout` would otherwise get disconnected — which is why
  `emit` selects on the keepalive ticker while it waits to hand over an event.

**Checklist:** what does this do on a half-open connection? · on a failover? · is the
timeout configurable, and does it have a sane default? · does a slow peer stall a
control path?

---

## 5. Database Architect

**Owns:** SQL correctness, PostgreSQL semantics, replication, pooling modes.

- Identifiers are quoted, and schema-qualified names are split first.
  `quoteIdentifier("public.users")` names a table *called* `public.users`.
- Values are bound as parameters. String interpolation is only for identifiers and
  for literals that go through `quoteLiteral`.
- Slot names reach the replication command unquoted, so the character set is
  validated up front (`slotNamePattern`).
- Catalog errors are classified by SQLSTATE (`42710`, `42704`), never by message.
- **Replication slots retain WAL.** A position that stops advancing fills the
  primary's disk. Idle keepalives must carry the confirmed position forward
  (`catchUp`), and an abandoned slot must be dropped deliberately.
- Resume from the slot's `confirmed_flush_lsn` (start LSN `0`), never from
  `IDENTIFY_SYSTEM`'s WAL head — that silently discards the retained backlog.
- Know the REPLICA IDENTITY rules: DEFAULT gives no before-image, FULL gives a
  complete one. An unchanged TOASTed value is *absent*, which is not the same as NULL.
- **Whatever the previous caller left behind must not be observable by the next
  one.** That is the pooling contract, enforced in `recyclable()`: dead or busy
  (unread result) disqualifies a connection outright; an open or failed transaction
  is rolled back; an active `LISTEN` is cleared with `UNLISTEN *`. Note that the
  transaction check does not catch a subscription — `TxStatus` stays `'I'` — so
  every new kind of session state needs its own gate. Ask what a caller could leave
  behind that the existing checks would not see.
- Transaction-mode pooling: a reset query deallocates prepared statements, so the
  driver must not be caching them client-side. `ResetQuery` therefore forces
  `QueryExecModeExec`. Without a reset query, prepared statements work fine — a
  connection serves one caller at a time, so its cache stays valid.
- Logical replication cannot be pooled at all: `CopyBoth` never yields a transaction
  boundary and a slot admits one walsender. Never route CDC through a pooler.

**Checklist:** does this work under REPLICA IDENTITY DEFAULT? · what does it do to
`confirmed_flush_lsn`? · does it survive a non-English `lc_messages`? · what is the
round-trip cost per call?

---

## 6. QA & Debugger Engineer

**Owns:** the test suite, reproducibility, regression coverage.

- TDD: write the failing test that names the defect before the fix.
- Table-driven tests. Mock the interface (`pgx.Rows`, `pgx.Tx`), never the concrete type.
- Every bug fixed gets a regression test whose comment says what used to go wrong.
  That comment is the test's real payload.
- Unit tests run with no database. Integration tests live in `integration/` and skip
  unless `DATABASE_URL` is set.
- `-race` is not optional. Any test touching shared state runs under it.
- **Unit tests cannot see driver state.** Two real bugs in this codebase were found
  only by integration tests and benchmarks: a released-but-unscanned row leaving the
  connection `conn busy`, and `DISCARD ALL` invalidating the statement cache. Exercise
  the real driver before calling something done.

- **A benchmark comparison must match capacity on both sides.** gpool looked 9%
  slower than `pgxpool` purely because its benchmark used `MaxConns: 10` against
  pgxpool's default of `max(4, GOMAXPROCS)` — the gap was queueing, not overhead.
  Publishing that would have been a false claim about someone else's library.
- **Iteration count changes what a benchmark reports.** Warm-up allocations dominate
  at low `-benchtime`; the pool-mechanics benchmarks need ~200k iterations before
  allocs/op settles.

**Run:**

```bash
go test -race ./pkg/...                                   # no database needed
DATABASE_URL='postgres://…' go test -race ./integration/  # needs wal_level=logical
DATABASE_URL='postgres://…' go test -bench=. ./benchmarks/
```

**Checklist:** does the test fail before the fix? · does the comment explain the
failure mode? · is `MaxConns: 1` used to prove a resource actually came back? · are
both sides of a comparison configured the same?

---

## 7. Security Architect

**Owns:** injection surfaces, credential handling, denial-of-service resistance.

- **Injection:** every identifier through `quoteIdentifier`/`quoteQualifiedName`,
  every literal through `quoteLiteral`, every value as a bound parameter. Anything
  interpolated raw (slot names) is validated against an allowlist pattern.
- **Do not weaken parameter binding for speed.** `QueryExecModeExec` still binds
  server-side; `QueryExecModeSimpleProtocol` interpolates client-side and must not
  become a default.
- **Credentials** live in the connection string and must never be logged, wrapped
  into an error message, or written to a knowledge store. gpool does no logging,
  which is deliberate — keep it that way.
- **Resource exhaustion:** every queue is bounded (`Config.Buffer`), every pool is
  capped (`MaxConns`), every wait has a deadline. An unbounded channel or an
  unbounded goroutine count is a DoS vector.
- **Least privilege:** document what a role needs. CDC needs `REPLICATION`; slot and
  publication DDL need ownership. Do not silently require superuser.
- **Destructive operations are explicit.** Slots and publications are never dropped
  automatically — the caller must ask. Dropping a slot discards a retained WAL
  position, which is data loss.

**Checklist:** can any caller-supplied string reach SQL unquoted? · does an error
message leak a credential or internal path? · what is the memory ceiling of this
under an adversarial peer?

---

## Post-task maintenance

After any substantive change, update, in this order:

1. `README.md` — the public contract, including any changed semantics.
2. `.serena/memories/` — durable conventions only; see `memory_maintenance.md`.
3. `graphify-out/` — re-run the graphify skill so the code graph matches. Run
   `.junie/scripts/graphify_reconcile.py` on the merged extraction before building:
   it synthesizes nodes for imported packages, which the AST extractor references
   but never creates, and drops whatever still cannot be resolved.
4. Obsidian vault at `~/Documents/ObsidianVault/Gpool/`.

Do this in the same change as the code. A doc left behind is not a cosmetic debt:
the graph extraction reads these files, so stale docs silently become stale graph.

Do not record volatile line numbers, one-off task notes, or anything the code already
says plainly.
