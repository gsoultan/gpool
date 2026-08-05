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
message inspected, and its body is one byte. Bodies over 64 KiB stream with
`io.CopyN` rather than buffering. Flush only once the source has drained, so a
pipelined batch costs one syscall rather than one per message.

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

## Measured against PgBouncer 1.25.2

Matched pool size (16), transaction mode, same PostgreSQL, load generator in the
same container network. Queries per second: 27,004 → 23,830 at 8 clients,
35,390 → 43,385 at 32, 32,001 → 55,055 at 128, 28,989 → 61,565 at 512.

**PgBouncer is the cheaper of the two per query** — about 12 µs of CPU against
20 µs. The crossover is not efficiency, it is that PgBouncer is one thread
(measured: `Threads: 1`) and therefore capped at one core on any hardware, while
the proxy was measured at 120% of a core under the same load. Its own answer is
`so_reuseport`, which is several processes with separate pools that no longer add
up to the configured size.

Do not repeat the mistake of comparing at unmatched pool size; see `mem:testing`.

## Known gaps, deliberate

Prepared statements have PgBouncer's pre-1.21 limitation — a cached statement
name is missing after transaction pooling moves the client. Clients connect with
`default_query_exec_mode=exec`. Per-backend tracking is the main thing between
this and something production-worthy. No session or statement pooling mode, no
admin console, no online reconfiguration, no md5/trust auth.

LISTEN/NOTIFY and logical replication cannot traverse it, for the same reasons
they cannot traverse PgBouncer. See `mem:pool` and `mem:cdc`.
