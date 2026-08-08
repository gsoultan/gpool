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

Two goroutines per session. A proxy is full duplex — a client may pipeline a
second batch before the first is answered — so one loop would have to guess which
side to read from.

- `pump` (client → backend) acquires a backend on demand.
- `relay` (backend → client) returns it at the transaction boundary.

Handover correctness rests on two things:

- **`pending` counts only Query, Sync and FunctionCall.** Each is answered by
  exactly one ReadyForQuery, so the pairing is exact and a pipelined client is
  fully answered before its backend moves on. Flush produces output but no
  ReadyForQuery. CopyDone and CopyFail produce one only in the simple protocol,
  where it belongs to the Query that started the copy and is already counted —
  counting them again releases the backend an answer early.
- **`s.mu` is held across the forward of a client message.** That is what stops
  the relay returning a backend to the pool while bytes are still being written
  to it. Cost is a release delayed by one slow message body.

Release requires `pending == 0` *and* `txStatus == 'I'`. An open transaction keeps
its backend at zero pending, or the next client inherits a live transaction.

## Taking a socket over from pgx

`pgconn.ConnectConfig` does the handshake (TLS, SCRAM, parameter collection),
then `Hijack()` surrenders the socket. Reimplementing that handshake would mean a
second, less tested copy of it.

Sound only if pgx's own reader has nothing buffered, which
`Frontend.ReadBufferLen()` reports — asserted rather than assumed, so a future
pgx that reads ahead fails loudly instead of dropping a message into a client's
result set. `pgproto3.Backend` offers no equivalent, which is why the *client*
startup phase is parsed by hand rather than through it.

`ParameterStatus` values replayed to clients are captured from a real backend on
first connect. Inventing them is a lie the proxy cannot keep consistent.

## Messages are relayed, not decoded

Read the 5-byte header, forward the body. ReadyForQuery is the only backend
message inspected, and its body is one byte. Bodies larger than one chunk move in
passes through the same buffer — not `io.CopyN`, which allocates a LimitedReader
per message, and per message is per query. Flush only once the source has
drained, so a pipelined batch costs one syscall rather than one per message.

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

## Buffers are per client, so they are what multiplies

`relayChunk` 4 KiB, `clientBufSize` 8 KiB, `backendBufSize` 64 KiB. A session is
two socket buffers plus two relay chunks = 24 KiB. All four were 64 KiB once,
which is 256 KiB per client and 750 MiB at three thousand — right for a handful
of connections, wrong for the number a pooler exists to make large. Bodies over
one chunk move in passes rather than being buffered whole. PgBouncer's `pkt_buf`
defaults to 4 KiB, which is the evidence 4 KiB suffices.

Measured cost at 3,000 clients: 107 MiB RSS, 37 KiB per client — the excess over
24 KiB is goroutine stacks and the session struct.

## Measured against PgBouncer 1.25.2

Transaction mode, pool size 16 both sides, `max_client_conn` 3000 both sides,
same PostgreSQL, load generator in the same container network, median of three
interleaved runs. Queries per second, PgBouncer → gpoolproxy: 26,854 → 23,239 at
8 clients, 34,698 → 51,792 at 32, 31,046 → 58,532 at 128, 26,973 → 50,742 at
3,000. PgBouncer peaks at 32 clients and falls 22% by 3,000; the proxy peaks at
128 and falls 13%.

**PgBouncer is the more efficient of the two and it is not close** — 14.8 µs of
CPU per query against 27.6 µs, 6 KiB per client against 37 KiB. The crossover is
not efficiency: PgBouncer is one thread (measured, `Threads: 1`) and therefore
capped at one core on any hardware, while the proxy was measured at 140% of a
core under the same load. PgBouncer was *not* saturated at 40% of its core, so
its decline is event-loop serialisation rather than CPU exhaustion — the one-core
ceiling is the durable claim, not that particular number. Its own answer is
`so_reuseport`: several processes with separate pools that no longer add up to
the configured size.

**Interleave the targets at each concurrency; do not sweep one to completion.**
Sweeping lets machine drift land on whichever target held the slot and be read as
a difference between them — an early run reported PgBouncer at both 4,798 and
2,705 q/s for the same case that way. Report medians of at least three runs.
And do not repeat the mistake of comparing at unmatched pool size; see
`mem:testing`.

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
  completions to swallow and `relayFrom` drops them. Same for the injected Close
  and its CloseComplete.

`readBody` reuses `relay.msg` rather than allocating: only P/B/D/C are read into
memory, and those are per query. Overhead was below the benchmark's noise floor —
which was ±20% with five databases on the host, so the honest claim is "not
measurable here", not "free".

Tests fail without it with `SQLSTATE 42P05`, proven by disabling `reconcile`.

## Known gaps, deliberate

Statements are never proactively deallocated, so a backend accumulates one entry
per distinct statement until a client closes it or a name collides — PgBouncer
bounds this with `max_prepared_statements` and this does not. No session or
statement pooling mode, no admin console, no online reconfiguration, no md5/trust
auth.

LISTEN/NOTIFY and logical replication cannot traverse it, for the same reasons
they cannot traverse PgBouncer. See `mem:pool` and `mem:cdc`.
