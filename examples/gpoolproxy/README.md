# gpoolproxy

A PostgreSQL connection pooler built on gpool, in about a thousand lines.

gpool is a library, and there is exactly one thing a library cannot do: bound
connections *across* applications. Forty services holding twenty-five connections
each still open a thousand, and no in-process pool can see the other
thirty-nine. That is the gap a sidecar pooler fills, and this is what filling it
with gpool looks like.

It is an example, not a product. Read it to see how the pooling engine is used;
run it to reproduce the numbers below.

## What it demonstrates

**`pkg/pooling` is genuinely vendor-agnostic.** The proxy drives `pooling.Core`
directly with a connection type that is not a database driver at all — no rows,
no queries, just a socket and a transaction status. Everything the engine
provides for pgx (capacity, lock-striped idle buckets, the reaper, lifecycle,
statistics) applies unchanged, and the whole vendor half is `driver.go`, five
methods.

**Cross-application pooling works.** `TestProxyBoundsBackendsAcrossIndependentClients`
runs twelve separate `pgxpool` instances — twelve applications — with five
connections each, and asserts PostgreSQL never sees more than the four backends
the proxy was told to open:

```
60 client connections across 12 applications ran on at most 4 PostgreSQL backends
```

## Running it

```bash
# a credential, without ever writing the password into a file
./gpoolproxy hash > /dev/null            # prompts, prints a SCRAM verifier
printf 'app:SCRAM-SHA-256$4096:…\n' > userlist.txt && chmod 600 userlist.txt

export GPOOLPROXY_UPSTREAM='postgres://pooler:secret@db:5432/app'
./gpoolproxy -listen 0.0.0.0:6432 -userlist userlist.txt -max-conns 25
```

The upstream connection string comes from the environment rather than a flag
because a command line is readable by every process on the host. The userlist is
refused outright if it is readable beyond its owner.

Clients then connect to the proxy instead of the database:

```
postgres://app:secret@proxy:6432/postgres?default_query_exec_mode=exec
```

## Measured against PgBouncer

PgBouncer 1.25.2 and gpoolproxy, both in containers on one podman network, both
pooling **16** server connections in transaction mode against the same
PostgreSQL 17. The load generator runs in the same network, so neither proxy is
measured across a different path than the other. Host: Apple M5 Pro, 7 CPUs
visible to the VM, shared with the generator. Two runs of 60,000 queries each,
averaged.

Throughput, queries per second:

| clients | direct to PostgreSQL | PgBouncer 1.25.2 | gpoolproxy | ratio |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 11,342 | 4,798 | 4,664 | 0.97× |
| 8 | 50,058 | 27,004 | 23,830 | 0.88× |
| 32 | 120,247 | 35,390 | 43,385 | 1.23× |
| 128 | 122,591 | 32,001 | 55,055 | **1.72×** |
| 512 | *cannot connect* | 28,989 | 61,565 | **2.12×** |

Three things in that table are worth more than the headline.

**PgBouncer is faster when there is little to do.** At one and eight clients it
wins. C and a tight event loop cost less per query than Go does, and measured at
the plateau PgBouncer spends about 12 µs of CPU per query against gpoolproxy's
20 µs. Nothing here makes gpoolproxy more efficient; it is not.

**The crossover is concurrency, and the cause is structural.** Under identical
128-client load:

```
pgbouncer  Threads: 1     40% of one core     32,001 q/s
gpoolproxy Threads: 21   120% of one core     55,055 q/s
```

PgBouncer is a single thread. That is not a tuning choice — it is a ceiling of
one core, on any hardware, forever. gpoolproxy was measured *above* that ceiling,
which is the whole difference. PgBouncer's own answer is `so_reuseport`: run
several PgBouncer processes, each with its own separate pool. That works, and it
means the pool sizes no longer add up to what you configured.

**Past 128 clients PgBouncer declines and gpoolproxy still climbs** — 35,390 →
28,989 against 43,385 → 61,565. And the direct column simply stops: 512 clients
exceed `max_connections`, which is the entire reason to run a pooler at all.

Reproduce with:

```bash
DATABASE_URL=… PGBOUNCER_URL=… PROXY_URL=… \
  go test -bench=Throughput -benchtime=60000x -count=2
```

Any target whose URL is unset is skipped, so a partial comparison still runs.

## What it does and does not implement

Implemented: transaction-mode pooling, SCRAM-SHA-256 with clients, optional TLS
on the client side, query cancellation (translated from the proxy's key to the
backend's), bounded clients, and rollback of a transaction whose client
disconnected.

Not implemented, deliberately: session and statement pooling modes, an admin
console (`SHOW POOLS`), online reconfiguration, `md5` and `trust` authentication,
and multiple upstream databases. Each is real work rather than an oversight.

**Prepared statements have the same limitation PgBouncer had before 1.21.** A
client that caches statement names will find them missing when transaction-mode
pooling moves it to another backend. Connect with `default_query_exec_mode=exec`,
which still binds parameters server-side and only stops the names being cached.
Tracking them per backend, as PgBouncer 1.21+ does, is the main thing standing
between this example and something production-worthy.

**LISTEN/NOTIFY cannot work** through transaction-mode pooling, here or in
PgBouncer: a subscription needs a session, which is what transaction pooling
refuses to give. Neither can logical replication — see the CDC notes in the main
README.

## Layout

| File | Contents |
| :--- | :--- |
| `proxy.go` | listener, client bounding, cancellation registry |
| `session.go` | one client: startup, authentication, the two relay goroutines |
| `relay.go` | byte-level message forwarding, transaction-unit accounting |
| `driver.go` | `pooling.Driver` — dial, judge, clean up |
| `backend.go` | one server connection at the raw protocol level |
| `scram.go` | SCRAM-SHA-256, server half |
| `verifier.go` | PostgreSQL's stored-verifier format |
| `userlist.go` | credentials file |
