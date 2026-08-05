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
in **transaction mode**, both pooling **16** server connections, both accepting
**3,000 client connections**, against the same PostgreSQL 17. Verified from
PgBouncer's own admin console:

```
pool_mode|transaction    max_client_conn|3000    default_pool_size|16
```

The load generator runs in the same network, so neither proxy is measured across
a different path than the other. Host: Apple M5 Pro, 7 CPUs visible to the VM and
shared with the generator. Median of three runs of 40,000 queries.

The three targets are interleaved at each concurrency rather than swept one at a
time. Sweeping a target to completion before starting the next lets drift in the
machine land entirely on whichever target held the slot and be read as a
difference between them — an earlier run of this benchmark reported PgBouncer at
both 4,798 and 2,705 queries per second for the same case that way.

Throughput, queries per second:

| clients | direct to PostgreSQL | PgBouncer 1.25.2 | gpoolproxy | ratio |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 11,431 | 4,256 | 4,749 | 1.12× |
| 8 | 52,864 | 26,854 | 23,239 | 0.87× |
| 32 | 115,960 | 34,698 | 51,792 | 1.49× |
| 128 | 116,002 | 31,046 | 58,532 | 1.89× |
| 512 | *exceeds max_connections* | 31,891 | 53,368 | 1.67× |
| 1024 | — | 29,079 | 54,226 | 1.86× |
| 2048 | — | 27,517 | 52,836 | 1.92× |
| 3000 | — | 26,973 | 50,742 | 1.88× |

Cost at the top of that table, with all 3,000 clients connected and querying:

| | PgBouncer | gpoolproxy |
| :--- | ---: | ---: |
| resident memory | **18.4 MiB** | 107.2 MiB |
| per client connection | **6 KiB** | 37 KiB |
| CPU, steady state | **40% of one core** | 140% of one core |
| CPU per query | **14.8 µs** | 27.6 µs |
| threads | 1 | 21 |

Four things there are worth more than the headline.

**PgBouncer is the more efficient of the two, and it is not close.** It serves a
query for roughly half the CPU and holds a client for a sixth of the memory. C
and a tight event loop beat Go and a pair of goroutines per session. Nothing in
this example changes that, and a deployment that is memory-bound rather than
throughput-bound should prefer PgBouncer on these numbers.

**What it cannot do is use a second core.** PgBouncer is one thread — measured,
not inferred — so one core is its ceiling on any hardware, forever. gpoolproxy
was measured at 140% of a core under the same load, which is simply not a number
a single thread can produce. That, and only that, is where the throughput
difference comes from. Note that PgBouncer was *not* saturated at 40%, so its
decline is event-loop serialisation rather than raw CPU exhaustion in this
environment; the one-core ceiling is the durable limit, not this particular
number.

**The shapes differ past the peak.** PgBouncer peaks at 32 clients and falls 22%
from there to 3,000. gpoolproxy peaks at 128 and falls 13%. Both degrade under
three thousand clients; one degrades less.

**PgBouncer's own answer to the single core is `so_reuseport`** — run several
PgBouncer processes behind one port. That works, and it means each has its own
pool, so `default_pool_size` no longer describes what reaches the database.

Reproduce with:

```bash
DATABASE_URL=… PGBOUNCER_URL=… PROXY_URL=… \
  GPOOL_BENCH_CLIENTS=1,8,32,128,512,1024,2048,3000 \
  go test -bench=Throughput -benchtime=40000x -count=3
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
