# gpoolproxy

A PostgreSQL connection pooler built on gpool: an event loop over raw sockets,
with `pooling.Core` deciding who gets a server connection.

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
postgres://app:secret@proxy:6432/postgres
```

## How it is built

A client costs a descriptor and about a kilobyte. Not a goroutine, not a stack,
not a buffer.

- **Startup is the only blocking part.** TLS negotiation and SCRAM run on a
  goroutine, because they are a conversation with a deadline. Once the client is
  authenticated its socket is taken over as a raw non-blocking descriptor and the
  goroutine exits.
- **One poller, a worker per CPU.** Every client and backend socket is registered
  once, edge-triggered, with a single epoll instance. `GOMAXPROCS` workers block
  in `epoll_wait`; whichever wakes takes the session's lock and moves bytes. No
  socket is ever re-registered, which matters: moving a backend between per-worker
  pollers was measured at 10% of CPU in `epoll_ctl` before the design settled on
  one.
- **Bytes go from one socket to the other out of the worker's buffer.** Each
  worker owns one 64 KiB read buffer. What it reads from a client is written to
  the backend from that same buffer, and the reverse. A socket gets a buffer of
  its own only for what the kernel refused to take or for a message that arrived
  in pieces, and gives it back when it drains.
- **A released backend goes straight to whoever has waited longest.** A session
  that finds no idle backend (`pooling.Core.TryAcquire`) joins a FIFO queue
  instead of parking a goroutine. At a transaction boundary the releasing worker
  hands the backend to the head of that queue and serves it on the spot.
- **TLS clients are the exception.** `crypto/tls` wants a `net.Conn`, so a TLS
  client is bridged to the event loop through a socketpair by two goroutines. It
  works and is tested, and it costs what the old design cost per client.

**Platforms.** Linux (epoll) is what is tested and measured. The kqueue poller
for macOS and the BSDs compiles and passes `go vet` but has never been run. There
is no Windows poller, so the example no longer builds there.

## Measured against PgBouncer

PgBouncer 1.22.0 and gpoolproxy as processes on one host, both in **transaction
mode**, both pooling **16** server connections, both accepting 4,000 clients,
both with a 512-statement prepared cache, against the same PostgreSQL 16.15 over
loopback. Host: 4 vCPUs of a 2.1 GHz Xeon, **shared between PostgreSQL, the load
generator and the proxy under test**. Median of three passes of 40,000 queries,
the targets interleaved at each concurrency.

Interleaving matters. Sweeping one target to completion before starting the next
lets drift in the machine land on whichever target held the slot and be read as
a difference between them — an earlier run of this benchmark reported PgBouncer
at both 4,798 and 2,705 queries per second for the same case that way.

"Before" is the previous gpoolproxy (two goroutines and 24 KiB of buffers per
client), built from the commit before this design and run on the same machine in
the same session. It was run in its own three passes rather than interleaved with
the other two.

Throughput, queries per second:

| clients | direct | PgBouncer | before | gpoolproxy | vs PgBouncer | vs before |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 14,948 | 8,678 | 5,562 | 8,717 | 1.00× | 1.57× |
| 8 | 118,904 | 42,168 | 32,219 | 31,949 | 0.76× | 0.99× |
| 32 | 150,815 | 49,759 | 42,032 | 44,239 | 0.89× | 1.05× |
| 128 | 117,891 | 43,089 | 42,682 | 47,781 | 1.11× | 1.12× |
| 512 | *exceeds max_connections* | 41,452 | 46,165 | 47,843 | 1.15× | 1.04× |
| 1024 | — | 34,297 | 43,730 | 49,088 | 1.43× | 1.12× |
| 2048 | — | 29,764 | 39,941 | 45,439 | 1.53× | 1.14× |
| 3000 | — | 29,387 | 38,564 | 45,158 | 1.54× | 1.17× |

CPU the proxy process spent per query, user plus system, microseconds:

| clients | PgBouncer | before | gpoolproxy |
| ---: | ---: | ---: | ---: |
| 1 | 42.0 | 132.8 | 45.2 |
| 8 | 21.0 | 36.8 | 34.2 |
| 32 | 19.0 | 27.5 | 24.2 |
| 128 | 22.5 | 29.2 | 23.2 |
| 512 | 23.8 | 31.8 | 22.8 |
| 1024 | 26.0 | 32.0 | 24.5 |
| 2048 | 29.5 | 36.5 | 25.8 |
| 3000 | 30.0 | 36.2 | 26.5 |

Memory, resident, MiB:

| | PgBouncer | before | gpoolproxy |
| :--- | ---: | ---: | ---: |
| just started, no clients | 3.6 | 8.1 | 8.6 |
| 3,000 clients connected and idle | 9.0 | 105.7 | 15.1 |
| per idle client | 1.9 KiB | 33 KiB | 2.2 KiB |
| 3,000 clients querying, end of the sweep | 21.5 | 154.9 | 26.8 |
| threads after that sweep | 2 | *not measured* | 19 to 38 |

Storage: the gpoolproxy binary is 10.1 MB, or 6.9 MB built with
`-ldflags="-s -w" -trimpath`, and needs nothing else on disk. The PgBouncer
binary is 0.4 MB and loads about 12.8 MB of shared libraries, libc included,
which a host usually has already.

What those tables say, without rounding in anyone's favour:

**Memory under load fell 83% and is now PgBouncer's, give or take a quarter.**
155 MiB to 27 at three thousand querying clients, against PgBouncer's 21.5. An
idle client went from 33 KiB to 2.2 KiB, against 1.9 KiB.

**The floor is the Go runtime, and it is higher than PgBouncer's.** An empty
gpoolproxy is 8.6 MiB resident, of which about 7 MiB is the binary's own text
mapped from disk. PgBouncer starts at 3.6 MiB. At small client counts PgBouncer
is the lighter process and will stay so.

**CPU per query is now level with PgBouncer from 128 clients up, and a little
under it from 512.** Below that it is still higher — 34 µs against 21 at eight
clients — because four workers woken one event at a time pay for context
switches that one thread, already awake and draining several events per
`epoll_wait`, does not. Running the proxy with `GOMAXPROCS=2` on this host
brought the eight-client case to parity on both throughput and CPU and cost
throughput at 3,000, so it is a trade rather than a fix.

**Neither proxy can make the kernel cheaper.** A query through either is six
socket calls: read and write on the way in, read and write on the way back, and
two waits. Profiled, 71–81% of gpoolproxy's CPU is inside those calls. That is
the reason there is no version of this that uses a fifth of PgBouncer's CPU:
PgBouncer is already near the floor, and the floor is the same for both.

**Throughput is ahead only where PgBouncer's one thread runs out.** Level at one
client, behind at 8 and 32, ahead from 128, and 1.5× at 2,000 and beyond. The
advantage is a second core, exactly as before; what changed is that it no longer
costs seven times the memory. On a host with more cores than this one the gap
is wider — the previous version of this table, from a 7-CPU machine, had
gpoolproxy at 1.9× — but that is a statement about those cores, and these
numbers are the ones measured for this code.

**PgBouncer's own answer to the single core is `so_reuseport`** — several
PgBouncer processes behind one port. That works, and it means each has its own
pool, so `default_pool_size` no longer describes what reaches the database.

Reproduce with:

```bash
DATABASE_URL=… PGBOUNCER_URL=… PROXY_URL=… \
  PGBOUNCER_PID=$(pgrep -x pgbouncer) PROXY_PID=$(pgrep -x gpoolproxy) \
  GPOOL_BENCH_CLIENTS=1,8,32,128,512,1024,2048,3000 \
  go test -run XXX -bench=Throughput -benchtime=40000x -count=3
```

Any target whose URL is unset is skipped, so a partial comparison still runs.
With a `*_PID` set, the benchmark reads that process from `/proc` and reports its
CPU per query and resident memory alongside the throughput; Linux only.

## What it does and does not implement

Implemented: transaction-mode pooling, **named prepared statements** under a
bound, SCRAM-SHA-256 with clients, optional TLS on the client side, query
cancellation (translated from the proxy's key to the backend's), bounded clients,
and rollback of a transaction whose client disconnected.

Not implemented, deliberately: session and statement pooling modes, an admin
console (`SHOW POOLS`), online reconfiguration, `md5` and `trust` authentication,
and multiple upstream databases. Each is real work rather than an oversight.

One known defect, found by the test that measures the bound above: the server
settings replayed during a client's startup are captured from the first backend
the pool opens, so a client that connects before there is one receives none. pgx
does not mind over the extended protocol but refuses the simple protocol without
`standard_conforming_strings`.

### Prepared statements

A named prepared statement lives on the backend that parsed it, and transaction
pooling moves a client to a different backend on every transaction. The proxy
remembers each client's `Parse` messages and replays them onto whichever backend
the next transaction lands on — the same answer PgBouncer reached in 1.21. Clients
can use pgx's default execution mode; `default_query_exec_mode=exec` is no longer
needed.

Two details are what make it correct rather than merely working:

- **Two clients may use the same statement name for different SQL.** Replaying
  blindly would leave whichever got there first in place and silently run their
  query for the other — wrong results, not an error. A name already present on a
  backend is closed and re-parsed.
- **An injected `Parse` produces a `ParseComplete` the client never asked for.**
  A client counts replies to its own messages, so those completions are swallowed
  on the way back rather than forwarded.

Both are covered by tests that fail without the feature, with
`SQLSTATE 42P05: prepared statement "…" already exists`.

The cost was not measurable against this benchmark's noise: reading three of the
roughly five messages per query into a reused buffer and looking up a name, set
against a round trip of tens of microseconds.

Both sets are bounded, by `MaxPreparedStatements` (default 512), because nothing
deallocates a prepared statement when a client goes away. Without a limit a
client chooses how much memory the proxy spends — one Parse message per name it
invents — and a pooled backend, which outlives every client that touches it,
accumulates one statement per name any of them ever used. That second one is
PostgreSQL's memory, not the proxy's.

At the limit the least recently used is discarded, and on a backend it is also
closed on the server, so the proxy's idea of what exists and the server's stay in
step. Evicting from a backend costs the client that prepared it nothing: its own
set still holds the Parse, so the next backend it lands on has it replayed. A
measured run prepares 60 statements against a limit of 8 and finds 8 in
`pg_prepared_statements` afterwards.

The default is 512 rather than PgBouncer's 200 because the two limits interact.
A client caches statement names on its own side and only Binds them afterwards,
so a proxy that remembers fewer than the client does turns into "prepared
statement does not exist" the first time that client moves backends — and 512 is
pgx's own default cache. Set it below your client's cache and you are choosing
that failure; set it negative and you are choosing the unbounded growth it exists
to prevent.

**LISTEN/NOTIFY cannot work** through transaction-mode pooling, here or in
PgBouncer: a subscription needs a session, which is what transaction pooling
refuses to give. Neither can logical replication — see the CDC notes in the main
README.

## Layout

| File | Contents |
| :--- | :--- |
| `proxy.go` | listener, client bounding, cancellation registry, backend handoff |
| `handshake.go` | one client's startup: TLS, authentication, then hand over to the loop |
| `session.go` | one client after startup: forwarding, transaction-unit accounting, acquire and release |
| `worker.go` | one event-loop thread: wait, dispatch, serve what was granted |
| `sock.go` | a non-blocking descriptor and whatever it could not write yet |
| `poller_linux.go`, `poller_kqueue.go` | readiness notification |
| `waiters.go` | sessions queued for a backend, first come first served |
| `takeover.go` | turning a `net.Conn` into a raw descriptor, and the TLS bridge |
| `protocol.go` | wire-protocol constants and message classification |
| `statements.go` | prepared statement tracking and replay across backends |
| `driver.go` | `pooling.Driver` — dial, judge, clean up |
| `backend.go` | one server connection at the raw protocol level |
| `scram.go` | SCRAM-SHA-256, server half |
| `verifier.go` | PostgreSQL's stored-verifier format |
| `userlist.go` | credentials file |
