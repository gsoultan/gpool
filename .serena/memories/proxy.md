# gpoolproxy — cross-application pooling

`examples/gpoolproxy`, its own module. The one thing a library provably cannot
do: bound connections *across* applications. An in-process pool cannot see the
other thirty-nine services, so forty holding twenty-five each still open a
thousand. Closing that needs a process.

Not part of the library and never downloaded by a consumer of
`github.com/gsoultan/gpool`. That is what keeps "gpool is a library only" true
while still answering the question.

## It is the generality proof for pkg/pooling

The proxy drives `Core[*backend]` where `*backend` is a socket plus a transaction
status — not a database driver, no rows, no queries. Everything the engine
provides still applies, and the vendor half is `driver.go`, five methods. If a
change to `pkg/pooling` cannot be expressed here, the engine has grown a pgx
assumption.

## Transaction-mode multiplexing

An event loop, not goroutines. After the handshake (a goroutine, because TLS and
SCRAM are a conversation with a deadline) the client socket is taken over as a
raw non-blocking descriptor. One shared edge-triggered epoll instance holds every
client and backend socket. One worker (the watch) sleeps in `epoll_wait`; the
other `GOMAXPROCS - 1` are parked on a channel and called in one at a time when
a worker takes a full batch, then park again on an empty poll. Whichever worker
gets an event takes `session.mu` and advances that session. A client costs a
descriptor and about a kilobyte.

Do not go back to a poller per worker: a backend then has to be re-registered
every time it changes sessions, and that was measured at 10% of CPU in
`epoll_ctl`.

Handover correctness rests on three things:

- **`pending` counts only Query, Sync and FunctionCall.** Each is answered by
  exactly one ReadyForQuery, so the pairing is exact and a pipelined client is
  fully answered before its backend moves on. Flush produces output but no
  ReadyForQuery. CopyDone and CopyFail produce one only in the simple protocol,
  where it belongs to the Query that started the copy and is already counted.
- **`open` holds the backend across a split batch.** Parse/Bind/Execute in one
  write and Sync in the next leaves `pending == 0` and status `'I'` in between;
  without `open` the Sync lands on another backend. This was a bug in the
  goroutine design too.
- **`queued` is owned by the waiter lock, `waiting` is a CAS claim.** A grant in
  flight to a session that has meanwhile acquired on its own would attach two
  backends. `queue_test.go` reproduces it without a database.

Release requires `pending == 0`, not `open`, *and* `txStatus == 'I'`.

A session that misses `Core.TryAcquire` joins a FIFO queue; the releasing worker
hands the backend to the head and serves it inline. `Acquire` is called only off
the loop (`Proxy.grow`), because it may block or dial.

TLS clients are bridged through a socketpair by two goroutines, since
`crypto/tls` needs a `net.Conn`. They cost what the old design cost per client.

Linux epoll is the tested path. kqueue compiles and has never been run. No
Windows poller.

## The upstream login is the proxy's own

`upstream.go` parses a `postgres://` URL once; `login.go` dials, negotiates TLS,
sends the startup packet and authenticates (SCRAM-SHA-256, MD5, cleartext);
then the connection is taken over as a descriptor. pgx was dropped because it
was 2.2 MB of a 6.9 MB binary for one handshake per connection.

## No PostgreSQL driver anywhere in the proxy's module

Not in the binary, not in the tests, not in `go.mod`. `client_test.go` is the
client the tests and the benchmark use: `wireClient`, built on the proxy's own
login, with the extended-protocol messages written out by hand. A test that
needs a client must use it, never import a driver;
`TestModuleDependsOnNoPostgreSQLDriver` fails if one appears in `go.mod`.

The one direct dependency is the working tree (`replace ../..`). The core module
requires caduceus, so `go.sum` holds caduceus's checksums and nothing else; no
PostgreSQL client library from outside the project appears in it.

Because a client that shares code with the server could share its mistakes,
`TestProxyServesPsql` runs `psql` (libpq) as a program against the proxy. It
skips where psql is not installed; keep it.

Rules this code keeps, each with a test:

- **Parse errors never quote the URL.** `url.Parse` errors do, and the URL holds
  the password.
- **SCRAM is verified both ways.** A server that says "ok" without a valid final
  signature is refused (`login.finish`).
- **Cleartext passwords only over TLS or a Unix socket**, refused before the
  password is written.
- **Lengths and iteration counts from the peer are bounded** before they become
  allocations or PBKDF2 work.
- **Nothing may be buffered past ReadyForQuery**, or it is lost in the takeover.
- No SASLprep; ASCII passwords are unaffected.

Deliberately not read: pgpass, service files, PG* env, keyword/value strings,
multi-host. Do not add them back casually; each is a credential source the
operator did not write down.

**A TLS upstream costs double.** An encrypted server connection cannot be a bare
descriptor, so it is bridged by goroutines: 112 µs/query against 47 at one
client. `sslmode` defaults to `prefer`, which encrypts if the server offers.
On the database host use the Unix socket or `sslmode=disable`.

`ParameterStatus` values replayed to clients are captured from a real backend on
first connect. Inventing them is a lie the proxy cannot keep consistent.

## Messages are relayed, not decoded

A worker owns one 64 KiB read buffer. Bytes read from one socket are written to
the other from that buffer; only what the kernel refused, or the tail of an
incomplete message, is copied into a per-socket buffer, which is given back when
it drains. ReadyForQuery is the only backend message inspected. Only P/B/D/C from
the client are assembled whole, for statement tracking.

## Security decisions worth keeping

- **SCRAM-SHA-256, verifier only.** Stored in PostgreSQL's own
  `SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>` format, so the proxy never
  holds a password that works anywhere else, and an operator can copy one out of
  `pg_authid`. The removed CLI proxy had no authentication at all; do not regress.
- **An unknown user runs the full exchange against a decoy verifier.** Refusing
  early is faster, and that difference is a username oracle.
- **The userlist is refused if readable beyond its owner.** Failing to start beats
  running happily with world-readable credentials.
- **The upstream string comes from the environment, not a flag** — a command line
  is readable by every process on the host.
- **Cancellation keys are generated per session and unguessable.** A CancelRequest
  arrives unauthenticated and carries no other proof; the proxy translates its own
  key to the backend's, under the session lock so the backend cannot be released
  and reused mid-cancel.

## Nothing per client but the session

No per-client buffer and no goroutine. The previous design held 24 KiB of buffers
and two stacks per client: 155 MiB at 3,000 querying clients, 33 KiB per idle
one. Now 27 MiB and 2.2 KiB. A tail of 512 bytes or less is copied at its exact
size rather than into a 4 KiB spare — waiting clients each holding a spare was
12 MiB on its own.

`main` sets `MemProfileRate = 0` and GC percent 50 (an explicit `GOGC` wins):
the live heap is about a kilobyte per client and steady state allocates almost
nothing, so the headroom was pure resident memory.

## Measured against PgBouncer 1.22.0

4 vCPUs shared by PostgreSQL 16, the generator and the proxy; transaction mode,
pool 16 both sides; median of three interleaved passes. Queries per second,
PgBouncer → gpoolproxy: 8,791 → 7,883 at 1 client, 37,790 → 35,918 at 8,
46,047 → 44,193 at 32, 42,188 → 44,440 at 128, 34,452 → 44,236 at 1,024,
27,301 → 42,152 at 3,000. CPU per query: 40.8 → 46.0 µs at 1, 22.5 → 21.2 at 8,
19.2 → 20.0 at 32, 32.8 → 25.8 at 3,000. Resident at 3,000 querying: 21.6 → 24
to 29 MiB. Empty: 3.6 → 8.6 MiB, about 7 of it binary text (private 1.0 → 1.6).
Anything under 10% at low client counts is inside this host's run-to-run noise.

**An "80% faster, 80% lighter than PgBouncer" target was asked for and is not
reachable**, and the reason is durable: both proxies do the same six socket calls
per query and 71–81% of the proxy's CPU is inside them. PgBouncer is near that
floor already. The throughput lead exists only where PgBouncer's one thread
saturates, and grows with cores.

**Keep one worker awake.** With every worker blocked in `epoll_wait`, 8 clients
cost 34 µs/query against PgBouncer's 21: a thread woken per event instead of one
thread finding several. With the reserve parked it is 21.

**The one-client gap is the Go runtime's monitor thread**, about 6 µs/query
(240 ms of a 1,940 ms run); the worker thread itself matches PgBouncer. Tried
and rejected, with numbers: parking the watch in the runtime netpoller via the
epoll fd (44 → 76 µs at one client), a busy-poll before sleeping (doubled CPU),
`LockOSThread` on workers (no difference outside noise).

**Binary:** 6.9 MB stripped, of which about 2.2 MB is `pgconn` and what it pulls
in; a TLS-and-net-only Go binary is 4.7 MB. Dropping pgconn means hand-writing
the upstream handshake, not done.

**Interleave the targets at each concurrency; do not sweep one to completion.**
Sweeping lets machine drift land on whichever target held the slot and be read as
a difference between them — an early run reported PgBouncer at both 4,798 and
2,705 q/s for the same case that way. Report medians of at least three runs.
And do not repeat the mistake of comparing at unmatched pool size; see
`mem:testing`. A restarted PgBouncer that lost its descriptor limit fails the
3,000-client case rather than slowing down; check `/proc/<pid>/limits`.

## Prepared statements survive transaction pooling

A named statement lives on the backend that parsed it, and pooling moves the
client every transaction. `statements.go` remembers each client's Parse messages
and `reconcile` replays them onto whichever backend the next transaction lands
on — PgBouncer's 1.21 answer.

Two things make it correct rather than merely working, and both would be silent
if got wrong:

- **A name already on the backend may be another client's, with different SQL.**
  Not re-parsing would run their statement for this client — wrong results, not
  an error. Any collision is closed first, then re-parsed.
- **An injected Parse produces a ParseComplete the client never sent a Parse
  for.** Clients count replies to their own messages, so `s.expect` queues the
  completions to swallow and `feedBackend` drops them. Same for the injected Close
  and its CloseComplete.

Only P/B/D/C are looked at as whole messages, in place in the worker's buffer
when they arrived whole, and those are per query. Overhead was below the benchmark's noise floor —
which was ±20% with five databases on the host, so the honest claim is "not
measurable here", not "free".

Tests fail without it with `SQLSTATE 42P05`, proven by disabling `reconcile`.

## Both statement sets are bounded, and the backend's bound reaches the server

`Config.MaxPreparedStatements` (default 512) caps the session's set and each
backend's, evicting the least recently used. Unbounded, both were sized by the
client: a session holds one Parse per name its client invents, and a backend one
per name *any* client ever used on it. Nothing deallocates a prepared statement
when a client disconnects, and a backend outlives every client that touches it,
so neither shrank on its own.

**Evicting from a backend sends a real Close** (`session.reconcile`). Forgetting alone
would be a lie — the statement is in the server's memory whether the proxy
remembers it or not, and a set that drifts below what the server holds is how the
server grows with nothing accounting for it. Measured: 60 statements prepared
against a limit of 8 leave 8 in `pg_prepared_statements`.

Backend eviction is invisible to the client that prepared the statement, because
its own set still has the Parse and replays it on the next backend. Session
eviction is not: past the limit the oldest stop being replayable and the client
meets the server's own "does not exist" after it moves.

**The default is 512, not PgBouncer's 200, because the two limits interact.** A
client caches statement names and only Binds afterwards, so remembering fewer
than the client does turns into a client error the first time it moves backends.
512 is pgx's own default cache.

Eviction picks the LRU by scanning rather than keeping a list: the limit is a few
hundred, the scan happens only once a client is at it, and the cost an adversary
can impose stays a constant per message.

## Known gaps, deliberate

No session or statement pooling mode, no admin console, no online
reconfiguration, no md5/trust auth.

One real defect: `Proxy.parameters()` is captured from the first backend the pool
opens, so a client connecting before there is one gets no ParameterStatus at all.
pgx tolerates it over the extended protocol and refuses the simple protocol
without `standard_conforming_strings`. Found by the test that counts prepared
statements server-side.

LISTEN/NOTIFY cannot traverse it, for the same reason it cannot traverse
PgBouncer in transaction mode. See `mem:pool`.

## Replication is passed through, never pooled

A client whose startup packet carries `replication` gets a dedicated server
connection dialed in replication mode (`Proxy.replicate`), attached to its
session as `pinned`: `settle` never releases it and `close` closes it
(`discard`). It is outside `pooling.Core` entirely, so it is bounded separately
by `Config.MaxReplicationClients`, counted before the dial so a burst cannot
overshoot.

**Default is zero, which refuses with `0A000`.** All clients share the one
upstream role, so passthrough gives every userlist entry that role's WAL. It is
an operator's decision. Dial errors are not wrapped into the refusal: they can
quote the connection string, and the refusal goes to a client.

The dial happens only after SCRAM succeeds. An unauthenticated peer must not be
able to make the proxy open server connections.

SQL polling (`pg_logical_slot_get_changes`) needs none of this: it is one
statement and pools like any other. See `mem:cdc`.
