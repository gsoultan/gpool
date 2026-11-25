# Pool internals


> The session, statement cache, cursor, batch and COPY code described below
> moved to caduceus (`caduceus.Conn`, `caduceus.Rows`, `BatchResults`,
> `CopyFrom`); the behaviour is unchanged. gpool keeps the adapter (`session`,
> `bindable`, `copySource`) and everything about pooling.

The PostgreSQL vendor runs on `pkg/pooling` like every other vendor. What is left
here is what is genuinely PostgreSQL-specific: dialling with `wire`, judging a connection
without I/O, and cleaning one up. Everything below about capacity, sharding, the
clock and the reaper now describes the *engine*, reached through `Core[*pgConn]`.

`pgConn` is the vendor's connection type — `{session *session; listening bool}`.
Being a type parameter rather than an interface is what lets the vendor keep its
own per-connection bookkeeping without a side table.

## Two costs the retarget exposed

Both were caught by benchmarking the retarget rather than assuming it was free.

- **`Driver.NeedsCleanup` exists to keep release cheap.** The engine originally
  built a `context.WithTimeout` on every release to bound `Recyclable`. That is
  four allocations and a runtime timer — it took the acquire path from 198 to 889
  ns/op. A connection returned with nothing on it now pays none of it.
- **`Handle` is returned by value**, so a vendor stores it inline in its wrapper
  and pays one allocation for the pair rather than two. That is why `released` is
  a plain word with CAS rather than an `atomic.Bool`: `atomic.Bool` carries a
  `noCopy` marker that makes returning a `Handle` a vet error. Range over handles
  by index, never by value.

After both, the retarget is performance-neutral: 200-217 ns/op against 198.3
before, one allocation either way.

`pkg/vendors/postgres/pool`.

## Capacity

`permits` (`permit.go`), a `chan struct{}` token set. A permit is held for the whole
time a connection is checked out, so `total <= MaxConns` holds structurally: a
connection is only created while its acquirer holds a permit, and only pooled while one
is being released. `warmUp` takes a permit for the same reason.

Not a counting semaphore — that cost a global mutex convoy and 3 allocations per
contended acquire at high client concurrency. See `mem:scale`.

**The set is resizable** (`Core.SetMaxConns`, exposed to library users as
`gpool.Resizable`). The channel is allocated once at `MaxConnsLimit` and never
replaced — swapping a channel out from under parked waiters would strand them — and
sizing it generously costs nothing, since a `struct{}` element has no backing array at
any capacity. `limit` holds the effective ceiling.

Shrinking is the interesting half. It cannot reclaim a permit a caller is holding, and
waiting for one would make a resize block on user code, so the shortfall becomes `debt`
and the next `release` calls swallow their tokens instead of returning them. Two things
must both happen or the ceiling is only half enforced:

- the permit debt bounds concurrent *checkouts*, and
- `Core.release` destroys a connection when `totalConns > maxConns`, which bounds
  *connections to the server*.

Missing the second one keeps every surplus connection open while correctly refusing new
checkouts — caught by `TestSetMaxConns_ShrinkIsNonBlockingAndDeferred`, not by reading.

`Close` drains `maxConns` tokens, not `cap(tokens)`: draining the channel's headroom
waits for tokens that were never issued and costs the full `closeDrainTimeout`.

`EvictIdle` closes every idle connection via `shard.takeIf`. Checked-out ones are judged
on release. It exists because only the caller knows its connections have gone stale —
role change, rotated credential, failover that kept the address.

## Sharding

16 shards, cache-line padded. Probe start is `rand.UintN(shardCount)` from
`math/rand/v2` — that is per-M and lock-free. A shared `atomic.AddUint64` round-robin
counter puts every Acquire in the process on one contended cache line, defeating the
striping.

`shard.count` is an `atomic.Int32` mirroring `len(conns)`, maintained under `mu` but
read without it. Lets Acquire skip empty shards with an atomic load instead of a lock
round trip, and makes `Stat` lock-free.

`shard.takeIf(predicate)` — the shard stores; the pool decides what is stale.

## Lifecycle

- `closed atomic.Bool` + `closeOnce`. Acquire re-checks after the semaphore wait.
- One background goroutine (`maintain`): `warmUp` immediately, then reap + warm on
  `HealthCheckPeriod`. Exits on `bgCtx`; `Close` waits on `bgDone`.
- `Close` drains checked-out connections by acquiring all permits with
  `closeDrainTimeout`, then destroys idle. Anything still out is destroyed by
  whoever releases it, since `release` sees `closed`.
- `release` → `recyclable()` gate, in order: dead → destroy; `session.busy()`
  (an exchange left open: unread result, mid-protocol, unsalvageable) → destroy; `TxStatus() != 'I'` →
  `ROLLBACK`, destroy only if that fails; `idleConn.listening` → `UNLISTEN *`,
  destroy if that fails; failed reset → destroy; lifetime expired → destroy.
  Idle expiry is *not* checked on release (it was in use until now).
- A `LISTEN` is session state the tx gate does not see: `TxStatus` stays `'I'`.
  `connWrapper.Listen` sets `idleConn.listening` *before* running the statement,
  because a LISTEN that fails mid-flight may still have registered.
- **The transaction gate is the pooling contract.** Without it a caller who forgets
  to commit hands the next caller their locks and snapshot, or a poisoned session
  rejecting everything with `25P02`. Rolling back is one round trip; pgxpool
  destroys the connection instead, which costs a full reconnect.
- **`Begin` sends nothing; caduceus holds the BEGIN and sends it with the first
  statement.** So the gate reads caduceus's `TxStatus()`, never the wire's: a
  transaction begun and not yet sent is idle on the wire, and returned as idle its
  BEGIN would go out in front of the next caller's statement. A BEGIN the server
  refused reads as `'E'` there, and caduceus runs nothing on that connection,
  through the Tx or around it, until the transaction is settled or unwound
  (`ErrTxNotBegun`). A plain BEGIN is rarely refused; a first statement the server
  cannot parse is not a refusal, caduceus re-sends the two apart and the
  transaction is open and failed.
- **The gate unwinds through `caduceus.Conn.Unwind`, not by sending `ROLLBACK`
  itself.** Unwind rolls back, costs nothing when the BEGIN never went, and spends
  every `caduceus.Tx` begun on the connection. `sessionTx` maps caduceus's
  sentinels onto this package's (`inOurWords`).
- **A `pgTx` ends with the acquisition it began on (`lease`).** The gate runs only
  when there is something to unwind, so a transaction the caller ended in SQL
  leaves its `caduceus.Tx` live through the release. `pgTx` therefore asks its
  `pooling.Handle` whether the connection has been released before every use, and
  `pkg/sqldriver` asks `connWrapper.live()`. Anything else a caller can keep that
  reaches a connection needs the same check.
- `totalConns` moves only in `connect` (+1) and `destroy` (-1).

## Statistics

`gpool.Stat` composes `Occupancy` (total/idle/active/max) and `Acquisition`
(acquire count, wait duration, empty and cancelled counts, plus `WaitingAcquires`).
Occupancy alone cannot distinguish a pool that is busy from one that is too small;
`EmptyAcquireCount` against `AcquireCount` is the pressure signal.

`ActiveConnections` is an exact count of checked-out connections, not `total - idle`.
The derived form samples two counters independently, so a connection the warm-up had
created but not yet pushed to a shard counted as active — and a consumer ranking pools
by load multiplies by that number.

`WaitingAcquires` is the only gauge among the counters. `EmptyAcquireCount` says the
pool has been short at some point since start-up; it cannot say whether it is short now,
which is what a dashboard and anything driving `SetMaxConns` actually need.

Only the blocking path is timed — `permits.tryAcquire()` vs `permits.wait()` exist
to separate them. A clock read on the fast path would cost a meaningful fraction of
its ~250ns.

## Lifecycle counters say *why* connections are replaced

`gpool.Lifecycle` — `ExpiredConnections`, `UnhealthyConnections`,
`EvictedConnections` — asserted on the value `Stat()` returns, not on the pool.

The question it answers is the one the acquisition counters cannot: a climbing
`EmptyAcquireCount` means either a pool that is too small or a pool dialling
replacements as fast as its connections die, and raising `MaxConns` fixes the
first while making the second worse.

Counted in `pkg/pooling`, so every vendor has it with no vendor code. Every
`destroy` carries a `discard` reason (`discard.go`) and every one is counted, so
the four reasons add up to every connection ever closed — a reason that went
unrecorded would show as connections vanishing from the accounting.
`discardClosed` is counted and reported to nobody: shutdown is not churn, and it
would put a step in every consumer's graph at exit.

`whyUnusable` checks ill health **first**, so a connection both dead and past its
lifetime counts as dead. Reporting it as a healthy recycle would hide the exact
signal these exist to surface.

Proven against a real server: four terminated backends are counted as four
unhealthy, which is the same number the recovery contract states as failed
queries — see `mem:testing`.

**A lifetime shorter than `clockResolution` (100ms) is invisible, not merely
quick.** Age is judged against the cached clock, so nothing ever reads a time
later than the one the connection was stamped with. A test wanting to observe
expiry needs tens of milliseconds and the reaper on.

## Optional capabilities

`BulkCopier` (Pool + Conn), `Batcher` (Conn), `Notifier` (Conn), `Resizable`
(Pool), `Lifecycle` (Stat). Separate interfaces reached by type assertion, so
`Pool`/`Conn`/`Stat` stay within the ISP method limit.
`CopyFrom` is caduceus's and takes two round trips whatever the row count. The
first learns the column types: Parse + Describe of a SELECT over the columns. The
second carries the COPY command as a simple Query (what libpq sends, and what
PgBouncer tracks correctly; a COPY through the extended protocol made PgBouncer
1.22.0 drop the server connection), the rows in 64 KiB CopyData chunks, and
CopyDone. Outside a transaction caduceus wraps the two in a BEGIN ... COMMIT of
its own, pipelined with them, so the table lock is held from the describe to the
COPY and the types described are the types written to; inside the caller's
transaction it adds nothing. Rows go in COPY's binary format when every column
type has a binary encoding (`codec.BinaryInput`), in text otherwise. A value with
no binary form (a string for an int column) is not refused: the rest of the stream
goes as a second, text COPY in the same transaction, so the copy is still all or
nothing. Never cache the described types across calls: a stale type in binary is
silent corruption, not an error. A source error sends CopyFail, and ROLLBACK for
a transaction caduceus began.
`SendBatch` pipelines behind one Sync (one implicit transaction, as
with pgx): one round trip when every statement is cached, two when some must be
described first.

## Per-connection caches are the memory cost

`Config.StatementCacheCapacity` bounds the session's LRU of named statements
(`gpool_<base36>`), nothing preallocated. Default 64. An eviction queues a Close sent
ahead of the next Parse, so it costs no round trip. With the statement cache off
(`DisableCache`, or a `ResetQuery`) nothing is kept on the server: caduceus parses
a statement into the unnamed statement in the same Sync that binds it, because a
pooler in transaction mode may give two Syncs different backends, and a Bind of
an unnamed statement parsed in an earlier Sync ran another client's statement
(measured: 4,393 of 4,800 answers wrong through PgBouncer). `DescriptionCacheCapacity`
bounds what the connection remembers of such statements instead, which makes a
known one a single round trip; the columns are compared with the server's on
every execution and a reset clears it. Under pgx the two caches were 57% of the heap
(`mem:scale`); a pooled connection is now ~11 KiB against pgxpool's ~82. `cacheCapacity` resolves the default itself so
`parse()` gives the same answer whether or not `withDefaults()` ran first.

## Pooling mode is usage, not config

No mode switch. Pool-level `Query`/`Exec`/`QueryRow` = statement pooling;
`Acquire`→`Begin`→`Commit`→`Release` = transaction pooling; holding a connection =
session pooling. Prepared statements work in the first two by default: a connection
serves one caller at a time, so its per-connection cache stays valid across reuse.
Session usage needs `ResetQuery`, which trades the cache away (below).

## CDC cannot be pooled

`replication=database` + `CopyBoth` after `START_REPLICATION` + one active walsender
per slot = inherently session-scoped, no transaction boundary to release at.
PgBouncer does not proxy replication connections. The subscriber's *control*
connection is ordinary and uses `wire.QueryText` (unnamed statements), so it holds
no named prepared statements and is pooler-safe.

## ResetQuery couples to query exec mode

`DISCARD ALL` deallocates the server's prepared statements. A cache would keep
binding the dropped names → `SQLSTATE 26000` on the connection's next use.
`Config.parse` therefore sets `settings.statements = 0` when `ResetQuery != ""`: every
query parses the unnamed statement, parameters still bound server-side; after the
reset `forgetStatements` clears anything cached. Never interpolate values
client-side. `Exec` with no arguments uses the simple protocol (several statements
allowed); with arguments it binds.

Found by benchmark, not by unit test. Cost is a full extra round trip per release
(~2x per-query latency), so ResetQuery stays empty by default.

## Rows vs Row ownership

`Rows`/`Row` from a *pool-level* call own the connection and release it on close.
From a *conn-level* or *tx-level* call they own nothing.

`pgRow` wraps a `resultSet` (a `cursor`), so a caller who declines to read can still
close it; returning that connection unclosed pooled it mid-query (`conn busy`, found
by integration test under pgx). `queryRow` allocates `pgRow` and its `cursor`
together (`cursorRow`). A cursor streams: each `Next` reads one DataRow whose values
alias the read buffer.

## The driver underneath

`session` (one per connection) runs every exchange between `begin` and `finish`;
one left open is what `busy()` reports. `begin` takes a `wire.Guard` from
`conn.Watch(ctx)`: a ctx deadline becomes the socket deadline, a cancellable ctx
gets a `context.AfterFunc` that moves the deadline into the past. `Guard.Done`
turns a timeout into the ctx's error, sends a CancelRequest from a goroutine, and
closes the connection, because its stream position is unknown. A Background ctx
costs nothing.

`wire.awaitExpiry`: the socket notices a deadline before the ctx timer fires, so an
exchange used to fail with `DeadlineExceeded` while `ctx.Err()` was still nil, and a
caller checking `ctx.Err()` to tell its own cancellation from a failure counted the
end of its run as errors. Found by `TestSoakSustainedQueryLoad` (449 "failures").
Done now waits for the ctx once the deadline has passed.

`WaitForNotification` is the one read that may end without harm: on an idle
connection only asynchronous messages arrive and the buffer consumes whole
messages, so it sets a read deadline and does not abort.
