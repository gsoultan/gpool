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
holds sixty client connections open at once — twelve applications with five
connections each — and asserts PostgreSQL never sees more than the four backends
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

### The upstream connection

The proxy logs in to PostgreSQL with its own code, not a client library: nothing
in the binary comes from outside this repository and the Go standard library.
It reads a `postgres://` URL and only that.

| In the URL | Meaning |
| :--- | :--- |
| `user:password@host:port/database` | as usual; port defaults to 5432, database to the user |
| `host=/var/run/postgresql` | a Unix socket directory, for a proxy on the database's own host |
| `sslmode` | `disable`, `prefer` (the default), `require`, `verify-ca`, `verify-full`, with libpq's meanings |
| `sslrootcert`, `sslcert`, `sslkey` | PEM files |
| `connect_timeout` | seconds |
| anything else | sent to the server as a setting: `application_name`, `options`, `search_path` |

Authentication is SCRAM-SHA-256, MD5 or a cleartext password, whichever the
server asks for. Three things are stricter or narrower than libpq, on purpose:

- **Nothing is read from anywhere else.** No `~/.pgpass`, no service file, no
  `PG*` environment variables, no keyword/value strings, no list of hosts. Each
  is a place a credential can come from that the operator did not write down.
- **A cleartext password is sent only over TLS or a Unix socket.** A server, or
  something answering in its place, that asks for one on an unencrypted
  connection is refused before the password is written.
- **The password is not passed through SASLprep.** For printable ASCII that
  changes nothing. A password SASLprep would rewrite will not authenticate.

SCRAM is verified in both directions: a server that cannot prove it knew the
password's verifier is refused, even if it says the login succeeded.

**On the database's own host, use the socket or `sslmode=disable`.** `prefer`
encrypts whenever the server offers to, and an encrypted server connection
cannot be a bare descriptor on the event loop: it goes through a goroutine
bridge. Measured against a server with `ssl = on`, that took one client from
47 µs of CPU a query to 112.

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
- **One poller, one worker awake, the rest in reserve.** Every client and
  backend socket is registered once, edge-triggered, with a single epoll
  instance. One worker sleeps in `epoll_wait` and at low load is the only one
  that runs: it wakes, takes every socket that became ready while it was busy,
  and serves them in one pass. The other `GOMAXPROCS - 1` workers are parked
  and are called in one at a time when a worker takes a full batch of events,
  which is the sign that one thread is no longer keeping up. A reserve worker
  parks again as soon as a poll comes back empty. No socket is ever
  re-registered, which matters: moving a backend between per-worker pollers was
  measured at 10% of CPU in `epoll_ctl` before the design settled on one.
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
both with a 512-statement prepared cache, against the same PostgreSQL 16.15
(`wal_level=logical`) over loopback. Host: 4 vCPUs of a 2.1 GHz Xeon, **shared
between PostgreSQL, the load generator and the proxy under test**. Median of
three passes of 40,000 queries, the targets interleaved at each concurrency.

Interleaving matters. Sweeping one target to completion before starting the next
lets drift in the machine land on whichever target held the slot and be read as
a difference between them — an earlier run of this benchmark reported PgBouncer
at both 4,798 and 2,705 queries per second for the same case that way.

Throughput, queries per second:

| clients | PgBouncer | gpoolproxy | ratio |
| ---: | ---: | ---: | ---: |
| 1 | 8,791 | 7,883 | 0.90× |
| 8 | 37,790 | 35,918 | 0.95× |
| 32 | 46,047 | 44,193 | 0.96× |
| 128 | 42,188 | 44,440 | 1.05× |
| 512 | 40,392 | 46,738 | 1.16× |
| 1024 | 34,452 | 44,236 | 1.28× |
| 2048 | 30,753 | 43,483 | 1.41× |
| 3000 | 27,301 | 42,152 | 1.54× |

CPU the proxy process spent per query, user plus system, microseconds:

| clients | PgBouncer | gpoolproxy | |
| ---: | ---: | ---: | ---: |
| 1 | 40.8 | 46.0 | +13% |
| 8 | 22.5 | 21.2 | −6% |
| 32 | 19.2 | 20.0 | +4% |
| 128 | 23.0 | 20.8 | −10% |
| 512 | 23.5 | 21.2 | −10% |
| 1024 | 26.8 | 22.5 | −16% |
| 2048 | 29.2 | 23.8 | −19% |
| 3000 | 32.8 | 25.8 | −21% |

Differences under about 10% at 1, 8 and 32 clients are inside what this host
repeats: the same PgBouncer case moved between 33,000 and 44,000 queries per
second across runs an hour apart.

Memory, resident, MiB:

| | PgBouncer | gpoolproxy |
| :--- | ---: | ---: |
| just started, no clients | 3.6 | 7.4 |
| of which private (not the binary's own pages) | 1.0 | 1.5 |
| 3,000 clients connected and idle | 9.0 | 15.1 |
| per idle client | 1.9 KiB | 2.2 KiB |
| 3,000 clients querying, first sweep | 21.6 | 24.1 |
| the same after three sweeps | 21.6 | 29.4 |
| threads after that | 2 | 12 |

Against the previous gpoolproxy (two goroutines and 24 KiB of buffers per
client), built from the commit before this design and measured on the same
machine: resident memory with 3,000 querying clients was 154.9 MiB, an idle
client cost 33 KiB, CPU per query was 132.8 µs at one client and 36.2 µs at
3,000, and one client got 5,562 queries per second.

Storage: the gpoolproxy binary is 8.0 MB, or 5.5 MB built with
`-ldflags="-s -w" -trimpath`, and needs nothing else on disk. The PgBouncer
binary is 0.4 MB and loads about 12.8 MB of shared libraries, libc included,
which a host usually has already.

What those tables say, without rounding in anyone's favour:

**Memory under load fell by about 83% and is now within a third of
PgBouncer's.** 155 MiB to between 24 and 29 at three thousand querying clients,
against PgBouncer's 21.6. An idle client went from 33 KiB to 2.2 KiB, against
1.9 KiB.

**The floor is the Go runtime, and it is higher than PgBouncer's.** An empty
gpoolproxy is 7.4 MiB resident. About 6 MiB of that is the binary's own text
mapped from disk, which the kernel can drop and reread; what the process
privately owns is 1.5 MiB against PgBouncer's 1.0. At small client counts
PgBouncer is the lighter process and will stay so.

**CPU per query is level with PgBouncer from 8 clients and lower from 128.**
That is what keeping a single worker awake bought. With every worker waiting in
the kernel, eight clients cost 34 µs a query against PgBouncer's 21, because a
thread was woken for each event where one thread, already awake, would have
found several. With one worker awake it is 21.

**At one client it is still about an eighth higher, and the cause is the Go
runtime, not the proxy's own code.** The worker thread spends 42.5 µs a query,
which is PgBouncer's figure. The rest is the runtime's monitor thread, which
wakes a few thousand times a second for as long as any thread is in a system
call: 240 ms of the 1,940 ms a 40,000-query run cost. Two ways around it were
tried and measured. Parking the worker in the runtime's own poller instead of
in `epoll_wait` took CPU at one client from 44 µs to 76. Spinning briefly before
sleeping doubled it. Neither is in the code.

**Neither proxy can make the kernel cheaper.** A query through either is six
socket calls: read and write on the way in, read and write on the way back, and
two waits. Profiled, 71–81% of gpoolproxy's CPU is inside those calls. That is
the reason there is no version of this that uses a fifth of PgBouncer's CPU:
PgBouncer is already near the floor, and the floor is the same for both.

**Throughput is ahead only where PgBouncer's one thread runs out.** Within a
tenth of it up to 32 clients, ahead from 128, and 1.5× at 3,000. The advantage
is a second core; what changed is that it no longer costs seven times the
memory, and that the extra cores are not touched until they are needed. On a
host with more cores than this one the gap is wider — the previous version of
this table, from a 7-CPU machine, had gpoolproxy at 1.9× — but that is a
statement about those cores, and these numbers are the ones measured for this
code.

**The binary got a fifth smaller by dropping the client library.** With pgx
doing the upstream login it was 6.9 MB stripped, of which about 2.2 MB was
`pgconn` and what it brought (`pgtype`, `database/sql`, `encoding/json`,
`golang.org/x/text`), used once per server connection. With the proxy's own
login it is 5.5 MB, and the empty process is 1.2 MiB lighter. What is left is
the Go runtime, `crypto/tls` and `net`; a Go program that only imports those is
4.7 MB, so that is close to the floor for a Go proxy that offers TLS.

**No PostgreSQL driver is left anywhere in this module**, tests included. The
tests and the benchmark connect with `wireClient` in `client_test.go`, which is
the proxy's own login and a few hand-written messages; `go.mod` requires only
the core module, for `pkg/pooling`, and there is no `go.sum` because nothing is
fetched; `go list -m all` names the proxy and the core module and nothing
else. `psql` is run as a program by one test,
so that a client the proxy did not write still has to accept it.

The throughput and CPU tables above were measured with pgx as the load
generator, before the benchmark moved to that client. Six passes with the new
one, on the same host on a noisier day, gave the same picture — 0.88× of
PgBouncer at one client, level at 8 and 32, and 1.2× to 1.5× from 128 clients
up — so the tables stand rather than being replaced by a noisier run.

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
rollback of a transaction whose client disconnected, and **replication
passthrough** for CDC clients, off unless asked for.

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
- **The server may skip what the proxy sent.** After an error it skips every
  message up to the `Sync`, the proxy's injected `Close` and `Parse` among them,
  and the client's own. Each is recorded as it is sent, since the messages behind
  it are decided on that record, and each still unanswered when its `Sync`'s
  `ReadyForQuery` arrives is undone. Recording them as done used to leave the
  proxy believing a backend held one client's statement when it held another's
  of the same name, which then ran in its place, and to swallow a reply that was
  the client's own.

Each is covered by a test that fails without it, with
`SQLSTATE 42P05: prepared statement "…" already exists` or another client's
result.

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
refuses to give.

### CDC through the proxy

There are two ways to read changes, and they need different things from a
pooler.

**Polling with SQL works in transaction mode as it is.**
`pg_logical_slot_get_changes` is an ordinary function call that is complete
within one statement, so it goes through a pooled backend like any other query.

**A replication stream cannot be pooled, so it is passed through instead.**
`START_REPLICATION` opens a CopyBoth exchange that never reaches a transaction
boundary, and a slot admits one walsender. A client that connects with
`replication=database` (or a boolean, for physical replication) therefore gets a
server connection of its own, opened in replication mode, outside the pool and
held until either end hangs up. This is what PgBouncer does from 1.23. The event
loop moves the stream like any other bytes; the only difference is that the
backend is never handed on.

```bash
./gpoolproxy -listen 0.0.0.0:6432 -userlist userlist.txt -max-conns 25 -max-replication 2
```

`-max-replication` is the number of such clients admitted at once, and it
defaults to zero, which refuses them with SQLSTATE `0A000`. That default is
deliberate:

- Every client reaches the server as the one upstream role. Enabling
  passthrough lets anyone in the userlist read whatever WAL that role may read.
  The role needs `REPLICATION`; it does not need superuser.
- Each one holds a server connection outside `-max-conns` and one of the
  server's `max_wal_senders`. Past the limit a client is refused with `53300`.
- The proxy does not create, advance or drop slots. A slot whose consumer has
  gone still retains WAL on the primary, exactly as it would without a proxy in
  between.

The dedicated connection is not counted in the pool's statistics.

## Layout

| File | Contents |
| :--- | :--- |
| `proxy.go` | listener, client bounding, cancellation registry, backend handoff |
| `handshake.go` | one client's startup: TLS, authentication, a dedicated connection for a replication client, then hand over to the loop |
| `session.go` | one client after startup: forwarding, transaction-unit accounting, acquire and release |
| `worker.go` | one event-loop thread: wait, dispatch, serve what was granted |
| `sock.go` | a non-blocking descriptor and whatever it could not write yet |
| `poller_linux.go`, `poller_kqueue.go` | readiness notification |
| `waiters.go` | sessions queued for a backend, first come first served |
| `takeover.go` | turning a `net.Conn` into a raw descriptor, and the TLS bridge |
| `protocol.go` | wire-protocol constants and message classification |
| `statements.go` | prepared statement tracking and replay across backends |
| `driver.go` | `pooling.Driver` — dial, judge, clean up |
| `upstream.go` | the server's connection string, parsed once |
| `login.go` | logging in to PostgreSQL: TLS, startup, authentication |
| `scram_client.go` | SCRAM-SHA-256, client half |
| `wire.go` | encoders for the few messages the proxy composes itself |
| `backend.go` | one server connection at the raw protocol level |
| `scram.go` | SCRAM-SHA-256, server half |
| `verifier.go` | PostgreSQL's stored-verifier format |
| `userlist.go` | credentials file |
| `client_test.go` | the client the tests and the benchmark drive the proxy with |
