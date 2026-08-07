# Pool internals

The PostgreSQL vendor runs on `pkg/pooling` like every other vendor. What is left
here is what is genuinely PostgreSQL-specific: dialling pgx, judging a connection
without I/O, and cleaning one up. Everything below about capacity, sharding, the
clock and the reaper now describes the *engine*, reached through `Core[*pgConn]`.

`pgConn` is the vendor's connection type — `{conn *pgx.Conn; listening bool}`.
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
- `release` → `recyclable()` gate, in order: dead → destroy; `PgConn().IsBusy()`
  (unread result, mid-protocol, unsalvageable) → destroy; `TxStatus() != 'I'` →
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

## Optional capabilities

`BulkCopier` (Pool + Conn), `Batcher` (Conn), `Notifier` (Conn). Separate interfaces
reached by type assertion, so `Pool`/`Conn` stay within the ISP method limit.
`gpool.CopyRows` is structurally identical to `pgx.CopyFromSource`, so the source
passes straight through with no adapter.

## Per-connection caches are the memory cost

`Config.StatementCacheCapacity` / `DescriptionCacheCapacity` bound pgx's two
per-connection LRU caches, which it preallocates at 512 each. Default 64. Was 57% of
the pool's heap; see `mem:scale`. `cacheCapacity` resolves the default itself so
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
connection is ordinary and uses `ExecParams` (unnamed statements), so it holds no
named prepared statements and is pooler-safe.

## ResetQuery couples to query exec mode

`DISCARD ALL` deallocates the server's prepared statements. pgx's default
`QueryExecModeCacheStatement` keeps referencing the dropped names → `SQLSTATE 26000`
on the connection's next use. `Config.parse` therefore forces
`pgx.QueryExecModeExec` when `ResetQuery != ""` — unnamed statement, parameters still
bound server-side. Do **not** reach for `QueryExecModeSimpleProtocol`, which
interpolates client-side.

Found by benchmark, not by unit test. Cost is a full extra round trip per release
(~2x per-query latency), so ResetQuery stays empty by default.

## Rows vs Row ownership

`Rows`/`Row` from a *pool-level* call own the connection and release it on close.
From a *conn-level* or *tx-level* call they own nothing.

`pgRow` wraps `pgx.Rows`, not `pgx.Row`. `pgx.Row` exposes only `Scan` and closes the
result as a side effect, so a caller who declines to read would have no way to close
it — and returning that connection pooled it mid-query (`conn busy`). Found by
integration test.
