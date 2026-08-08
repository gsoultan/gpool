# Gpool: A Go Connection Pooling & CDC Library

Gpool is a Go 1.26-native **library** for connection pooling — PostgreSQL, MySQL, MariaDB,
SQL Server and ClickHouse — with Change Data Capture for PostgreSQL, MySQL and SQL Server. It is designed to be embedded in your application or composed into another library — there
is no daemon, no CLI, no config file, no logger, and no process-global state.

## 📦 Installation

```bash
go get github.com/gsoultan/gpool@v0.2.0
```

While the major version is `0` the API may change in a minor release, so pin an
exact version. See [CHANGELOG.md](CHANGELOG.md) for the versioning policy and what
`v1.0.0` will require.

## 🧱 Architecture

Gpool separates **interfaces** from **vendor implementations**, so consumers depend only on
behaviour:

| Package | Contents |
| :--- | :--- |
| `pkg/gpool` | `Pool`, `Conn`, `Tx`, `Rows`, `Row`, `Result`, `Stat`, `Engine`, vendor registry, and the optional capabilities `BulkCopier`, `Batcher`, `Notifier`, `Resizable` |
| `pkg/gpool/cdc` | `Subscriber` = `Stream` + `TableManager` + `Close`, plus `EventStream`, `Event`, `Position`, and the optional capability `ReplicationManager` |
| `pkg/pooling` | The pooling engine: capacity, lock-striped idle buckets, reaper, lifecycle, statistics |
| `pkg/sqldriver` | Shared pooling for any `database/sql` driver (standard library only) |
| `pkg/vendors/postgres/pool` | PostgreSQL pool implementation (`pgx/v5`) |
| `pkg/vendors/postgres/cdc` | PostgreSQL logical-replication implementation (`pglogrepl`) |
| `vendors/mysql/cdc` | MySQL and MariaDB binary-log implementation (`go-mysql`), its own module |

### Supported databases

| Database | Module | Driver | CDC |
| :--- | :--- | :--- | :--- |
| PostgreSQL | `github.com/gsoultan/gpool` | `pgx/v5` | ✅ logical replication |
| MySQL | `github.com/gsoultan/gpool/vendors/mysql` | `go-sql-driver/mysql` | ✅ binary log |
| MariaDB | `github.com/gsoultan/gpool/vendors/mysql` | `go-sql-driver/mysql` | ✅ binary log |
| SQL Server | `github.com/gsoultan/gpool/vendors/mssql` | `microsoft/go-mssqldb` | ✅ change tables |
| ClickHouse | `github.com/gsoultan/gpool/vendors/clickhouse` | `clickhouse-go/v2` | — |

MySQL CDC is a module of its own again, nested inside the pool vendor at
`github.com/gsoultan/gpool/vendors/mysql/cdc`: reading a binary log pulls in the
TiDB parser and thirty-odd other modules, and someone who only wants a
connection pool should not download them. The pool vendor resolves 11 modules,
the CDC one 47.

### What has actually been tested

Every row below was run against a real server, not asserted. `.junie/scripts/testdbs.sh`
brings all five up with Apple's `container`; the suites skip themselves when a DSN
is unset, so an untested vendor reports as skipped rather than as passing.

| Database | Pool | CDC | Verified against |
| :--- | :--- | :--- | :--- |
| PostgreSQL | ✅ | ✅ | 17.10, `wal_level=logical` |
| MySQL | ✅ | ✅ | 8.4.11, `binlog_format=ROW`, GTID on, `binlog_row_metadata=MINIMAL` |
| MariaDB | ✅ | ✅ | 11.8.8, `binlog_format=ROW` |
| SQL Server | ✅ | ✅ | 2022 (16.0.4265.3) |
| ClickHouse | ✅ | — | 24.10.2.80 |

ClickHouse has **no CDC implementation**; the dash means absent, not untested — it
has no change log to read. `gpool.NewSubscriber` reports that with `ErrNoCDCSupport` rather than
suggesting an import that would not help.

MySQL's CDC is exercised under `binlog_row_metadata=MINIMAL` deliberately — the
default, and the harder path, because column names then have to come from
`information_schema` rather than from the log.

Each non-PostgreSQL vendor is **its own Go module**. That is deliberate: a consumer
using only PostgreSQL never downloads the MySQL driver, and `govulncheck` never flags a
CVE in a driver you do not import. The cost is a `go get` per vendor.

```bash
go get github.com/gsoultan/gpool                       # PostgreSQL pool + CDC, and the interfaces
go get github.com/gsoultan/gpool/vendors/mysql         # MySQL and MariaDB pool
go get github.com/gsoultan/gpool/vendors/mysql/cdc     # MySQL and MariaDB CDC (binary log)
go get github.com/gsoultan/gpool/vendors/mssql         # SQL Server
go get github.com/gsoultan/gpool/vendors/clickhouse    # ClickHouse
```

The weight this saves is real: the core resolves 20 modules, while ClickHouse alone
brings 89. A PostgreSQL consumer pays for none of them.

`pkg/pooling` and `pkg/sqldriver` live in the core module and depend on nothing beyond
the standard library, which is what keeps each vendor module thin.

Vendors self-register through `init()`, in the style of `database/sql` drivers. Importing the
vendor package is what makes `gpool.Postgres` resolvable:

```go
import (
    "github.com/gsoultan/gpool/pkg/gpool"

    // Registers the "postgres" pool factory.
    postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)
```

If you only need the interfaces — for example to accept a `gpool.Pool` in your own library's API —
import `pkg/gpool` alone and let the caller choose the vendor. The CDC interfaces are segregated,
so a consumer that only reads changes can depend on `cdc.Stream` rather than the full
`cdc.Subscriber`.

## 🛠️ Pool Usage

```go
import (
    "github.com/gsoultan/gpool/pkg/gpool"
    postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
    ConnString: "postgres://user:pass@localhost:5432/dbname",
    MaxConns:   50,
    MinConns:   5,
})
if err != nil {
    return err
}
defer pool.Close()

rows, err := pool.Query(ctx, "SELECT id, name FROM users WHERE active = $1", true)
if err != nil {
    return err
}
defer rows.Close() // safe: All() also closes, and Close is idempotent

for row := range rows.All() {
    var id int
    var name string
    if err := row.Scan(&id, &name); err != nil {
        return err
    }
}
return rows.Err()
```

Explicit acquisition and transactions:

```go
conn, err := pool.Acquire(ctx)
if err != nil {
    return err
}
defer conn.Release()

tx, err := conn.Begin(ctx)
if err != nil {
    return err
}
defer func() { _ = tx.Rollback(ctx) }() // returns ErrTxClosed after a commit; safe to ignore

if _, err := tx.Exec(ctx, "UPDATE users SET active = $1 WHERE id = $2", false, id); err != nil {
    return err
}
return tx.Commit(ctx)
```

### Lifecycle guarantees

Every teardown method is idempotent, because the idiomatic Go patterns call them more than once:

| Call | Repeat behaviour |
| :--- | :--- |
| `Pool.Close` | No-op |
| `Conn.Release` | No-op; later use returns `ErrConnReleased` |
| `Rows.Close` | No-op; later use returns `ErrRowsClosed` |
| `Row.Scan` / `Row.Release` | Either settles the row; the other becomes a no-op |
| `Tx.Commit` / `Tx.Rollback` | Whichever runs second returns `ErrTxClosed` |
| `Engine.Close` | Replays the first result |

`Rows` from `Pool.Query` and `Row` from `Pool.QueryRow` own the connection and return it when they
are closed. For `QueryRow` that means calling **either** `Scan` or `Release` — if you decide not to
read the result, `Release` still closes the query and frees the connection.

The `Row` yielded by `Rows.All()` is a cursor over the current row, valid only until the loop
advances. Scan what you need inside the loop body.

### Pool configuration

| Field | Default | Notes |
| :--- | :--- | :--- |
| `ConnString` | required | Parsed once at construction, then cloned per connection |
| `MaxConns` | `max(4, GOMAXPROCS)` | Hard cap; `Acquire` blocks at the limit |
| `MinConns` | `0` | Kept warm by the background reaper |
| `MaxConnLifetime` | `1h` | What lets the pool recover from a failover; negative disables |
| `MaxConnIdleTime` | `30m` | Negative disables |
| `HealthCheckPeriod` | `1m` | Reaper interval; negative disables |
| `ResetQuery` | `""` | See below |
| `ResetQueryTimeout` | `5s` | |
| `ConnectTimeout` | `10s` | Used when the caller's context has no earlier deadline |
| `StatementCacheCapacity` | `64` | Per-connection prepared-statement cache; `DisableCache` turns it off |
| `DescriptionCacheCapacity` | `64` | Per-connection statement-description cache |
| `BeforeConnect` | `nil` | Adjust the config per connection, e.g. credential rotation |
| `AfterConnect` | `nil` | Register types or run `SET`; an error discards the connection |

A connection that is closed, has outlived its bounds, or fails its reset query is destroyed rather
than returned to the pool.

The two cache capacities are the largest per-connection memory cost. pgx defaults both to 512 and
preallocates each one's map at that size, which measured as 57% of the pool's entire heap. gpool
defaults them to 64 — within 10% of the floor while still caching a typical service's query set.
Raise it if your query variety is wide; lower it if connection count matters more. See
[Footprint at scale](#-footprint-at-scale).

### Pooling modes and prepared statements

gpool has no pooling-mode switch. The mode is decided by **how long you hold a connection**, which
makes the PgBouncer equivalents a matter of usage rather than configuration:

| PgBouncer mode | gpool equivalent | Prepared statements |
| :--- | :--- | :--- |
| statement | `pool.Query` / `pool.Exec` / `pool.QueryRow` — acquired and released per call | ✅ cached per connection |
| transaction | `Acquire` → `Begin` → `Commit` → `Release` | ✅ cached per connection |
| session | `Acquire` once, hold it, `Release` at the end — plus `ResetQuery: "DISCARD ALL"` | ❌ traded away, see below |

**Prepared statements work by default.** gpool caches them per connection
(`pgx.QueryExecModeCacheStatement`), and because a connection is only ever handed to one caller at a
time, the cache stays valid across reuse. Nothing about pooling by itself invalidates it — this is
the advantage of pooling in-process over a wire proxy, where a client's statement names follow it
onto whichever backend it lands on.

**What returns a connection to a clean state.** On release gpool refuses to recycle a connection
that is closed or mid-protocol with an unread result, and cleans up two kinds of leftover state:

| Leftover | What gpool does | Why it matters |
| :--- | :--- | :--- |
| Open or failed transaction | `ROLLBACK` | The next caller would inherit the locks and snapshot, or a session rejecting everything with `SQLSTATE 25P02` |
| Active `LISTEN` | `UNLISTEN *` | The next caller would silently receive notifications it never subscribed to |

Both cost one round trip and only on connections that actually need them — versus a full reconnect
if the connection were simply discarded. A cleanup that fails means the state is unknown, so that
connection is destroyed rather than reused.

**`ResetQuery` is the session-mode option, and it costs you prepared statements.**
`DISCARD ALL` deallocates the server's prepared statements, so gpool switches the connection to
`pgx.QueryExecModeExec` — parameters still bound server-side, but nothing cached by name. Without
that switch the driver's cache goes stale and the next query fails with `SQLSTATE 26000`. It also
costs a full extra round trip on every release, roughly doubling per-query latency.

Set `ResetQuery` only when callers hold connections long enough to accumulate session state
(`SET`, `LISTEN`, temp tables, `WITH HOLD` cursors). For statement- and transaction-scoped usage
the rollback gate above is already sufficient, and leaving `ResetQuery` empty is both faster and
keeps prepared statements. `BeforeConnect` can override the execution mode if your reset query
preserves them.

## ⚡ CDC Streaming

Two vendors implement change data capture behind one interface: PostgreSQL over
logical replication, MySQL and MariaDB over the binary log. Both are reached
through the same factory, and a vendor that has a pool but no CDC says so with
`ErrNoCDCSupport` rather than suggesting an import that would not help.

### PostgreSQL

```go
import (
    "github.com/gsoultan/gpool/pkg/gpool"
    postgrescdc "github.com/gsoultan/gpool/pkg/vendors/postgres/cdc"
)

subscriber, err := gpool.NewSubscriber(gpool.Postgres, postgrescdc.Config{
    ConnString:        "postgres://user:pass@localhost:5432/dbname",
    SlotName:          "gpool_slot",
    PublicationName:   "gpool_pub",
    Tables:            []string{"public.users"},
    CreateSlot:        true,
    CreatePublication: true,
})
if err != nil {
    return err
}
defer subscriber.Close()

stream, err := subscriber.Subscribe(ctx)
if err != nil {
    return err
}
defer stream.Close()

for event := range stream.All() {
    fmt.Printf("%s %s.%s at=%s after=%v\n",
        event.Op, event.Schema, event.Table, event.Position, event.After)
}
return stream.Err()
```

Requires `wal_level=logical` and a role with the `REPLICATION` attribute. Creating the publication
additionally needs ownership of the tables.

> **CDC cannot be pooled, and does not use prepared statements.** A replication stream opens with
> `replication=database`, enters `CopyBoth` after `START_REPLICATION`, and stays there for the life
> of the subscription — there is no transaction boundary at which a connection could be handed
> back, and a slot admits one active walsender at a time. It is inherently session-scoped, and
> PgBouncer does not proxy replication connections at all. Point the subscriber at the database
> directly, not at a pooler.
>
> The subscriber's *control* connection — publication DDL and `pg_publication_tables` lookups — is
> an ordinary connection issuing discrete statements with unnamed parameter binding, so it holds no
> server-side prepared statements and is safe through a pooler in transaction mode.

### MySQL and MariaDB

```go
import (
    "github.com/gsoultan/gpool/pkg/gpool"
    mysqlpool "github.com/gsoultan/gpool/vendors/mysql"
    mysqlcdc "github.com/gsoultan/gpool/vendors/mysql/cdc"
)

subscriber, err := gpool.NewSubscriber(mysqlpool.MySQL, mysqlcdc.Config{
    DSN:      "repl:pass@tcp(localhost:3306)/app",
    ServerID: 1001,                        // must be unique among the source's replicas
    Tables:   []string{"app.users"},       // empty captures every table
})
if err != nil {
    return err
}
defer subscriber.Close()

// Subscribe() starts at the end of the log. Resume from what you last stored.
stream, err := subscriber.SubscribeFrom(ctx, checkpoint)
if err != nil {
    return err
}
defer stream.Close()

for event := range stream.All() {
    process(event)
    save(event.Position)   // durably, before treating the change as done
}
return stream.Err()
```

Both imports are needed: the CDC package registers the subscriber, the pool package names the
vendor. `mysqlpool.MariaDB` works the same way.

Requires `binlog_format=ROW` and an account with `REPLICATION SLAVE`, plus `SELECT` on
`information_schema` to resolve column names. It does **not** need `SUPER`.

> **Set `binlog_row_metadata=FULL` if you can.** A binlog row is a list of values with no names
> attached. Under `FULL` the names travel in the log itself, which is correct by construction even
> for rows written before a later `ALTER TABLE`. Under the default `MINIMAL` they are read from
> `information_schema`, which describes the table *now* — and a column count that disagrees is
> reported as `ErrSchemaMismatch` rather than handing you values under names that may not be theirs.

> **`ServerID` has no default, deliberately.** The source treats a subscriber as a replica, and two
> consumers sharing an ID make it disconnect one of them repeatedly without explaining why.

### SQL Server

```go
import (
    "github.com/gsoultan/gpool/pkg/gpool"
    mssqlpool "github.com/gsoultan/gpool/vendors/mssql"
    mssqlcdc "github.com/gsoultan/gpool/vendors/mssql/cdc"
)

subscriber, err := gpool.NewSubscriber(mssqlpool.SQLServer, mssqlcdc.Config{
    DSN:    "sqlserver://user:pass@host:1433?database=app",
    Tables: []string{"dbo.users"},
})
if err != nil {
    return err
}
defer subscriber.Close()

// Capture is server-side DDL here, not a client filter: this enables it.
if err := subscriber.AddTables(ctx, "dbo.users"); err != nil {
    return err
}

stream, err := subscriber.SubscribeFrom(ctx, checkpoint)
```

CDC lives in the pool vendor's module because it needs no dependency the pool does
not already have. Requires a database with `sys.sp_cdc_enable_db` run on it —
not `master`, which cannot have CDC — and **SQL Server Agent running**, because
the capture job is what fills the change tables. Without the Agent,
`sp_cdc_enable_table` succeeds, the change tables are created, and they stay
empty forever, which looks exactly like an idle database.

> **Changes arrive on the capture job's schedule**, roughly five seconds by
> default, not as transactions commit. `PollInterval` controls how often gpool
> reads the change tables; it cannot make a source that captures slowly deliver
> quickly.

### Positions and resuming

Every event carries a `Position` — an opaque, vendor-defined marker. Record it,
hand it back to `SubscribeFrom`, and the stream resumes:

```go
for event := range stream.All() {
    process(event)
    checkpoint = event.Position   // persist this
}
...
stream, err := subscriber.SubscribeFrom(ctx, checkpoint)
```

Resuming starts **at or before** the change the position came from, never after
it. A resumed stream may repeat what it already delivered but never skips, which
is what carries at-least-once delivery across a restart — so a consumer that must
not process a change twice needs its own idempotency.

The difference between vendors is not cosmetic and loses data if assumed away:

| | PostgreSQL | MySQL | SQL Server |
| :--- | :--- | :--- | :--- |
| how changes are read | tail the WAL | tail the binlog | **poll change tables** |
| who records your position | the server, in the slot | nobody | nobody |
| `Subscribe()` with no position | resumes from the slot, losing nothing | starts at the **end of the log** | starts at the **end of the log** |
| resuming needs client state | no | **yes** | **yes** |
| falling behind costs | the primary's disk fills | logs expire, **changes lost** | cleanup job runs, **changes lost** |
| delivery latency | as it commits | as it commits | **the capture job's schedule** |
| what a position looks like | `0/1A2B3C4D` | `gtid:3E11FA47-…:1-5` | `0x0000002B00000582001C` |

Positions are opaque and vendor-specific: never compare two from different vendors, and do not
assume they sort lexically. Passing one vendor's position to another's `SubscribeFrom` is refused
rather than coerced.

PostgreSQL additionally refuses `SubscribeFrom` with a position behind the slot's
confirmed position (`ErrPositionBehindSlot`), because the server would silently
clamp to the slot and serve a stream with a hole in it.

### Delivery semantics

Delivery is **at-least-once**, on both vendors — but what backs it differs.

On PostgreSQL the stream confirms a position to the server only after the iterator body for that
event has returned, so a crash mid-processing replays rather than loses. The corollary is that a
consumer which hands work to another goroutine and returns immediately has confirmed work it has
not done: either finish processing inside the loop body, or record `Event.Position` yourself.

On MySQL there is nothing to confirm — the source keeps no per-consumer state — so your stored
`Event.Position` is the *only* record of progress. Store it before treating a change as processed.

> **Replication slots retain WAL.** A PostgreSQL consumer that stops draining grows the primary's
> disk usage until it fills. Slots are never dropped automatically; drop one deliberately when you
> are done with it. During idle periods the stream advances its confirmed position from the
> server's keepalives, so a quiet publication does not pin WAL.
>
> **Binary logs expire.** A MySQL consumer that falls behind `binlog_expire_logs_seconds` loses the
> changes outright — the opposite failure mode, and the quieter one.

### Event shape

```go
type Event struct {
    Op       Op                // OpInsert, OpUpdate, OpDelete
    Schema   string
    Table    string
    Position Position          // opaque; record it and pass it to SubscribeFrom
    Before   map[string]any    // Update and Delete
    After    map[string]any    // Insert and Update
}
```

- `Before` and `After` are allocated per event and owned by you. They are safe to retain and to
  send to another goroutine.
- **Value types differ by vendor.** PostgreSQL delivers every value as a `string`, exactly as
  `pgoutput` transmits it in text format — the replication stream does not carry the destination Go
  type. MySQL delivers the binlog parser's native types (`int64`, `float64`, `string`, `time.Time`),
  with byte slices copied into strings.
- A column **absent** from the map was not transmitted. On PostgreSQL that means either
  `REPLICA IDENTITY` does not cover it or it is an unchanged TOASTed value. That is distinct from a
  key present with a `nil` value, which is a real SQL `NULL`. Writing an absent column as `NULL`
  would blank it on replay.
- On PostgreSQL `Before` is `nil` under `REPLICA IDENTITY DEFAULT` unless the key changed; use
  `ALTER TABLE ... REPLICA IDENTITY FULL` for a complete before-image. MySQL's `ROW` format carries
  the full before-image by default.

### Table management

```go
err := subscriber.AddTables(ctx, "public.orders", "public.products")
err  = subscriber.RemoveTables(ctx, "public.products")
err  = subscriber.SyncTables(ctx, "public.users", "public.orders") // reconcile to an exact list

tracking := subscriber.IsTracking("public.users")
tables   := subscriber.GetTables()
ok, err  := subscriber.VerifyTable(ctx, "public.users")
```

Both vendors apply these to a live stream, but they mean different things underneath.

On PostgreSQL they are `ALTER PUBLICATION` against the server, run on a separate control connection
so they are safe while a stream is live. The local tracking list is updated only after the server
accepts the change, so `IsTracking` never reports a table the publication does not have, and
`VerifyTable` queries `pg_publication_tables` for the real answer.

On MySQL there is no subscription to alter, so the filter is applied by the consumer — the whole
binary log crosses the network either way, and an empty table list captures everything.
`VerifyTable` can only confirm the table exists.

### Slot administration is optional

`cdc.Subscriber` is `Stream` + `TableManager` + `Close`. Replication slots and
publications are PostgreSQL's model — MySQL has no equivalent at all — so they
live in a separate interface reached by type assertion, the same way `BulkCopier`
and `Notifier` do:

```go
if slots, ok := subscriber.(cdc.ReplicationManager); ok {
    err := slots.CreateSlot(ctx, "manual_slot")        // no-op if it exists
    err  = slots.DropSlot(ctx, "manual_slot")          // no-op if it does not
    err  = slots.CreatePublication(ctx, "manual_pub", "public.t1", "public.t2")
    err  = slots.DropPublication(ctx, "manual_pub")
}
```

A failed assertion means "this engine has no such objects", not an error.

### CDC configuration

PostgreSQL (`postgrescdc.Config`):

| Field | Default | Notes |
| :--- | :--- | :--- |
| `ConnString` | required | The `replication` parameter is added internally where needed |
| `SlotName` | required | Must match `[a-z0-9_]{1,63}` — PostgreSQL's own rule |
| `PublicationName` | required | |
| `Tables` | required with `CreatePublication` | |
| `StartLSN` | `0` | Resume from the slot's confirmed position; `SubscribeFrom` overrides it |
| `StandbyInterval` | `10s` | Must stay well under the server's `wal_sender_timeout` |
| `Buffer` | `256` | Read-ahead depth in events |

MySQL and MariaDB (`mysqlcdc.Config`):

| Field | Default | Notes |
| :--- | :--- | :--- |
| `DSN` | required | go-sql-driver format; must be `tcp` |
| `ServerID` | required | Unique across every replica and CDC consumer of the source |
| `Tables` | all tables | `schema.table`; filtered client-side |
| `Flavor` | `mysql` | Or `mariadb` — the two write GTIDs differently |
| `HeartbeatPeriod` | `30s` | How often an idle source is asked to prove it is alive |
| `ReadTimeout` | `90s` | Must exceed `HeartbeatPeriod` |
| `Buffer` | `256` | Read-ahead depth in events |

Only one stream may be open per subscriber; a second `Subscribe` returns `ErrAlreadySubscribed`.

## 🧩 Engine

`Engine` is an optional facade that owns any number of named pools and CDC subscribers, so a host
application can shut everything down through a single `Close`. Both constructor arguments may be nil.

```go
engine := gpool.NewEngine(pool, subscriber) // registered as gpool.DefaultPool / DefaultSubscriber

engine.AddSubscriber("analytics", analyticsSubscriber)
analytics := engine.Subscriber("analytics")
names := engine.Subscribers()
err := engine.RemoveSubscriber("analytics") // closes it

defer engine.Close() // closes every subscriber and every pool, joining errors
```

`Close` is idempotent and one component failing to close does not stop the others. `AddPool` and
`AddSubscriber` replace an existing registration *without* closing the displaced one — the caller
may still be holding it.

## 🔀 Running Behind PgBouncer

gpool replaces PgBouncer, but plenty of deployments cannot remove it — a managed
service that only exposes a pooled endpoint, or a database already fronted by one.
Stacking them works; the one thing to get right is prepared statements.

| PgBouncer | gpool default | `StatementCacheCapacity: DisableCache` |
| :--- | :--- | :--- |
| `session` mode | ✅ | ✅ |
| `transaction` mode, `max_prepared_statements > 0` | ✅ | ✅ |
| `transaction` mode, `max_prepared_statements = 0` | ❌ `42P05` / `26000` | ✅ |

PgBouncer gained prepared-statement tracking in **1.21** and enables it by default
from **1.24** (`max_prepared_statements = 200`), so a current PgBouncer works with
gpool untouched. Against an older one, or one with the setting off, gpool's cached
statement names collide as clients move between backends — measured as
`SQLSTATE 42P05: prepared statement "stmtcache_…" already exists`.

The configuration that works against every version and mode:

```go
pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
    ConnString:             "postgres://user:pass@pgbouncer:6432/app",
    MaxConns:               25,
    StatementCacheCapacity: postgrespool.DisableCache, // safe behind any pooler
})
```

Three further things to know:

- **Leave `ResetQuery` empty.** PgBouncer already runs `server_reset_query` itself;
  adding gpool's own is a second round trip that buys nothing.
- **`Notifier` will not work** through transaction mode. A `LISTEN` needs a stable
  session, which is precisely what transaction pooling refuses to give.
- **CDC must bypass PgBouncer entirely.** It does not proxy replication
  connections. Point `postgrescdc.Config.ConnString` at the database directly.

`Stat()` still describes gpool's own connections — which, stacked this way, are
connections to the *proxy*. PgBouncer's server-side pool is reported by its own
`SHOW POOLS`, not here. Worth knowing before labelling a dashboard "database
connections".

`integration/pgbouncer_test.go` covers all of this and reports which regime it
observed; run it with `PGBOUNCER_URL` set.

## 🏛️ Cross-application pooling

There is one thing gpool cannot do as a library. An in-process pool bounds *its
own* connections; it cannot see the other applications. Forty services holding
twenty-five connections each still open a thousand, and no amount of library
design fixes that — bounding connections across processes needs a process.

`examples/gpoolproxy` is that process, built on the same `pkg/pooling` engine
every vendor runs on: a PostgreSQL wire-protocol pooler in about a thousand
lines, of which the vendor half is five methods. It is an example rather than a
product, and its tests prove the property:

```
60 client connections across 12 applications ran on at most 4 PostgreSQL backends
```

Measured against PgBouncer 1.25.2 — both in transaction mode, both pooling 16
server connections, both accepting 3,000 clients, same PostgreSQL, load
generator on the same network, median of three interleaved runs:

| clients | direct | PgBouncer | gpoolproxy |
| ---: | ---: | ---: | ---: |
| 8 | 52,864 | 26,854 | 23,239 |
| 32 | 115,960 | 34,698 | 51,792 |
| 128 | 116,002 | 31,046 | **58,532** |
| 1024 | *exceeds max_connections* | 29,079 | **54,226** |
| 3000 | — | 26,973 | **50,742** |

PgBouncer is the more efficient of the two and it is not close — roughly half the
CPU per query, and 6 KiB per client against 37 KiB. What it cannot do is use a
second core: it runs one thread, so one core is its ceiling on any hardware,
while gpoolproxy was measured at 140% of a core under the same load. That is the
whole of the throughput difference. See `examples/gpoolproxy/README.md` for the
method and the caveats.

## 🐬 MySQL and MariaDB

MariaDB speaks the MySQL wire protocol, so one implementation serves both; the two
vendor names exist so calling code says which database it actually targets.

```go
import (
    "github.com/gsoultan/gpool/pkg/gpool"
    "github.com/gsoultan/gpool/vendors/mysql"
)

pool, err := mysql.New(mysql.Config{
    // parseTime=true is worth setting: without it DATE and DATETIME columns
    // arrive as raw bytes rather than time.Time.
    DSN:      "user:pass@tcp(localhost:3306)/app?parseTime=true",
    MaxConns: 50,
    MinConns: 5,
})
if err != nil {
    return err
}
defer pool.Close()
```

Or through the registry, exactly as with PostgreSQL:

```go
pool, err := gpool.NewPool(mysql.MariaDB, mysql.Config{DSN: dsn, MaxConns: 50})
```

Everything else — `Query`, `QueryRow`, `Exec`, `Acquire`, transactions, `Stat`, the
release gate, the `MaxConns` ceiling — behaves identically to PostgreSQL, because it
is the same engine underneath. Two differences worth knowing:

- **`Result` carries `LastInsertID`.** MySQL has one and PostgreSQL does not, so it is
  reached by type assertion rather than sitting on `gpool.Result`:
  ```go
  if withID, ok := result.(interface{ LastInsertID() (int64, bool) }); ok {
      id, present := withID.LastInsertID()
  }
  ```
- **`Rows.FieldDescriptions` reports only column names.** A `database/sql` driver
  exposes nothing else, so the type fields are left zero rather than invented.

Connections are pooled at the `driver.Conn` level rather than by wrapping `*sql.DB`.
Wrapping `*sql.DB` would mean `database/sql` does the pooling and none of gpool's
guarantees would apply — no release gate, no acquisition metrics, no shared behaviour
with the native vendors.

> A transaction the caller abandons is rolled back on release. `go-sql-driver`'s own
> `ResetSession` does **not** do this, so without the gate the next caller inherits the
> uncommitted work — verified by removing the gate and watching the row survive.

## 🏢 SQL Server and ClickHouse

Both follow the same shape as MySQL — a DSN plus the pooling knobs, registered under
their own vendor name.

```go
import "github.com/gsoultan/gpool/vendors/mssql"

pool, err := mssql.New(mssql.Config{
    // The ADO form, server=host;user id=...;, is accepted too.
    DSN:      "sqlserver://user:pass@localhost:1433?database=app",
    MaxConns: 50,
})
```

SQL Server uses `@p1`-style ordinal placeholders rather than `?` or `$1`.

```go
import "github.com/gsoultan/gpool/vendors/clickhouse"

pool, err := clickhouse.New(clickhouse.Config{
    DSN:      "clickhouse://default:pass@localhost:9000/analytics",
    MaxConns: 8, // sized to what the cluster can run at once, not to client concurrency
})
```

ClickHouse is an analytical column store, so two things differ from the
transactional engines:

- **Transactions are not generally available.** `Begin` fails unless the server has
  experimental transaction support enabled. gpool reports the server's refusal rather
  than papering over it, and the connection stays usable afterwards.
- **Insert in batches.** One row per `INSERT` is pathological against a column store;
  use one statement carrying many rows.
- **Size the pool to the server.** ClickHouse spends real memory per concurrent query,
  so a large pool against a small cluster trades queueing in your process for pressure
  on the cluster.

`Options` is available for what a DSN cannot express — compression, TLS, several hosts
for failover:

```go
clickhouse.Config{Options: &clickhousego.Options{ /* ... */ }, MaxConns: 8}
```

## 🗄️ Multiple Databases

Reaching several databases from one process means one pool per database, selected by name. Nothing
is shared between them: separate connection strings, separate capacity, separate lifecycles. A
database that saturates or fails cannot starve another.

```go
engine := gpool.NewEngine(nil, nil)

for name, connString := range map[string]string{
    "primary":   "postgres://user:pass@host-a:5432/app",
    "analytics": "postgres://user:pass@host-b:5432/warehouse",
} {
    pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
        ConnString: connString,
        MaxConns:   25, // capacity is per database
    })
    if err != nil {
        return err
    }
    engine.AddPool(name, pool)
}
defer engine.Close() // closes every pool

// Route by name.
var total int
err := engine.Pool("analytics").
    QueryRow(ctx, "SELECT count(*) FROM events").Scan(&total)

names := engine.Pools()        // ["primary", "analytics"]
engine.RemovePool("analytics") // unregisters and closes it
```

`engine.Pool()` with no argument returns the pool registered under `gpool.DefaultPool`, so
single-database code needs no changes.

The same works for CDC: register one subscriber per database with `AddSubscriber`. Each holds its
own replication slot, which is the correct topology — sharing a slot between consumers loses data.

## 🧰 Optional Capabilities

Bulk copy, batching, and LISTEN/NOTIFY are exposed as separate interfaces rather than piled onto
`Pool` and `Conn`, so neither grows past what every caller needs. Reach them with a type assertion.

**`BulkCopier`** — load many rows in one pass over the COPY protocol, far faster than the equivalent
INSERTs. Available on both `Pool` and `Conn`. COPY is atomic: a source that fails part-way leaves
nothing behind.

```go
copier, ok := pool.(gpool.BulkCopier)
if !ok {
    return errors.New("vendor does not support bulk copy")
}

copied, err := copier.CopyFrom(ctx, gpool.CopyRequest{
    Table:   []string{"public", "users"}, // parts, so a dotted name is still addressable
    Columns: []string{"id", "email"},
    Rows: gpool.CopyFromSlice(len(users), func(i int) ([]any, error) {
        return []any{users[i].ID, users[i].Email}, nil
    }),
})
```

`CopyRows` is a cursor, not a slice, so you can stream a dataset larger than memory.

**`Batcher`** — pipeline statements so N of them cost roughly one round trip instead of N. Available
on `Conn` only: the result reader stays valid just while the connection is held.

```go
batch := &gpool.Batch{}
batch.Queue("INSERT INTO events (kind) VALUES ($1)", "click")
batch.Queue("INSERT INTO events (kind) VALUES ($1)", "view")
batch.Queue("SELECT count(*) FROM events")

results := conn.(gpool.Batcher).SendBatch(ctx, batch)
defer results.Close() // required: unread replies would desynchronise the connection

for range 2 {
    if _, err := results.Exec(); err != nil {
        return err
    }
}
var total int
if err := results.QueryRow().Scan(&total); err != nil {
    return err
}
return results.Close()
```

**`Notifier`** — LISTEN/NOTIFY, on `Conn` only. A subscription belongs to the session, so it only
means anything on a connection you continue to hold; a pool-level `Listen` would register on
whichever connection served the call and then hand it to someone else. gpool clears the
subscription on release, so it cannot leak onward.

```go
conn, _ := pool.Acquire(ctx)
defer conn.Release()

listener := conn.(gpool.Notifier)
if err := listener.Listen(ctx, "events"); err != nil {
    return err
}
for {
    notification, err := listener.WaitForNotification(ctx)
    if err != nil {
        return err
    }
    handle(notification.Channel, notification.Payload)
}
```

## 📊 Observability

Gpool exports no metrics and starts no HTTP server. Poll `Pool.Stat()` — it is lock-free — and feed
it to whatever collector your application already uses.

`Stat` composes two interfaces, so a consumer that only graphs utilisation can depend on
`Occupancy` alone:

```go
stat := pool.Stat()

// Occupancy: a point-in-time view.
stat.TotalConnections()
stat.IdleConnections()
stat.ActiveConnections()
stat.MaxConnections()   // so a dashboard can show utilisation without being told the config

// Acquisition: cumulative counters.
stat.AcquireCount()          // successful acquisitions
stat.AcquireDuration()       // total time callers spent waiting
stat.EmptyAcquireCount()     // acquisitions that had to wait
stat.CanceledAcquireCount()  // acquisitions whose context ended first
```

Occupancy alone cannot answer the question that actually matters when sizing a pool — *is `MaxConns`
too low?* — because a pool that is permanently full looks identical to one that is merely busy. The
acquisition counters separate them:

| Signal | Reading |
| :--- | :--- |
| `EmptyAcquireCount` near zero | Callers never wait; the pool is large enough |
| `EmptyAcquireCount` tracking `AcquireCount` | Every caller queues; raise `MaxConns` |
| `AcquireDuration / EmptyAcquireCount` rising | Waits are getting longer, not just more frequent |
| `CanceledAcquireCount` rising | Callers are timing out before they get a connection |

Only waits are timed. An acquisition served immediately contributes nothing to `AcquireDuration`,
which keeps the uncontended path free of a clock read it does not need.

## 🚨 Errors

All sentinels are comparable with `errors.Is`.

`pkg/gpool`: `ErrVendorNotRegistered`, `ErrNilFactory`, `ErrEmptyVendor`.

`pkg/vendors/postgres/pool`: `ErrPoolClosed`, `ErrConnReleased`, `ErrTxClosed`, `ErrRowsClosed`,
`ErrInvalidConfig`.

`pkg/vendors/postgres/cdc`: `ErrClosed`, `ErrAlreadySubscribed`, `ErrNoTables`, `ErrInvalidConfig`.

## 🔌 Adding a Vendor

Implement `gpool.Pool` (and optionally `cdc.Subscriber`) and register a factory from your package's
`init()`:

```go
func init() {
    _ = gpool.RegisterPool(gpool.Vendor("mysql"), func(config any) (gpool.Pool, error) {
        cfg, ok := config.(Config)
        if !ok {
            return nil, fmt.Errorf("%w: expected %T, got %T", ErrInvalidConfig, Config{}, config)
        }
        return New(cfg)
    })
}
```

The registry is mutex-guarded, so registration is safe to interleave with `NewPool`.

## 🧪 Testing

Unit tests need no database:

```bash
go test -race ./pkg/...
```

Integration tests and benchmarks need a server with logical replication enabled:

```bash
podman run -d --rm --name gpool-test \
  -e POSTGRES_PASSWORD=postgres -p 55432:5432 \
  docker.io/library/postgres:17-alpine \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10

export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:55432/postgres'
go test -race ./integration/
go test -bench=. ./benchmarks/
```

## 📉 Footprint at Scale

A pooler's job is to absorb far more callers than it holds connections, so those two numbers are
measured separately. Apple M5 Pro, PostgreSQL 17 over loopback, `GOMAXPROCS=15`.

**Per connection.** Measured by `TestMemoryPerPooledConnection`, which holds every connection open
and reads steady-state heap:

| `StatementCacheCapacity` | KiB / connection | Projected at 5,000 connections |
| :--- | ---: | ---: |
| `DisableCache` | 25.8 | 126 MiB |
| 16 | 26.1 | 128 MiB |
| **64 (default)** | **28.5** | **139 MiB** |
| 128 | 36.2 | 177 MiB |
| 512 (pgx's default) | 71.2 | 348 MiB |

**Per caller: nothing.** The pool adds exactly one goroutine — the background maintainer —
regardless of connection count or client concurrency, and `Close` reclaims it. Verified by
`TestGoroutineCostAtScale`.

Note the two readings of "5,000 connections". Five thousand *callers* over a bounded pool is the
normal shape and costs `MaxConns × 28.5 KiB` — about **3 MiB for a 100-connection pool**. Five
thousand *actual backend connections* is the 139 MiB row above, and is usually the wrong design:
PostgreSQL itself allocates several MiB of backend memory per connection, so the server is the
binding constraint long before gpool is.

## 📈 Benchmarks

`benchmarks/` compares gpool against `pgxpool`, `database/sql`, and PgBouncer on the same workload,
with pool capacity matched so the comparison measures the driver rather than queueing.

**Query path** — `-benchtime=20000x`, default parallelism, capacity `max(4, GOMAXPROCS)` on both:

| Benchmark | ns/op |
| :--- | ---: |
| `PgxPool` | 45,956 – 47,229 |
| `Stdlib` | 46,882 – 48,973 |
| `GpoolQueryRow` | 46,393 – 47,096 |

A query is dominated by the network round trip, so all three land in the same band across repeated
runs. gpool adds no measurable overhead to the query itself.

**Pool mechanics** — what gpool actually controls, `-benchtime=200000x`:

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `GpoolAcquireRelease` (default parallelism) | 257 | 48 | 1 |
| `ScaleAcquireRelease` (**5,000 callers**, 16 conns) | 574 | 65 | 1 |
| `ScaleAcquireRelease` (**5,000 callers**, 64 conns) | 744 | 52 | 1 |
| `ScaleAcquireRelease` (**5,000 callers**, 256 conns) | 537 | 52 | 1 |
| `ScaleStatUnderLoad` (scraping under traffic) | 30 | 19 | 1 |

The single allocation is the per-acquisition handle, which is deliberately not pooled — recycling
an object whose lifetime the caller controls is what made double-`Release` corrupt the pool. `Stat`
is lock-free, so a metrics scrape does not serialise against acquisition.

**`ResetQuery` costs a full extra round trip**: `GpoolResetQuery` measures ~103,000 ns/op against
~47,000 without it. Enable it only when session isolation is required.

Rerun on your own hardware before quoting any of this; loopback flatters every number here.

## 🛡️ License

Copyright (c) 2026 Gembit Soultan Shirazi. All rights reserved.
Licensed under the MIT License.
