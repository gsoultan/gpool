# CDC internals

Two vendors now: `pkg/vendors/postgres/cdc` (pgoutput, proto_version 1) and
`vendors/mysql/cdc` (binary log). See `mem:cdc_mysql` for the second one and for
what having two proved about the shared interfaces.

## The shared surface, and what is deliberately not in it

`cdc.Subscriber` is `Stream` + `TableManager` + `Close`. **`ReplicationManager`
is optional**, reached by type assertion like `BulkCopier`/`Batcher`/`Notifier`.
Slots and publications are PostgreSQL's model; MySQL has no server-side
subscription object at all, so a mandatory interface would have forced four
methods that only return errors — a compile-time mismatch turned into a runtime
one. PostgreSQL carries `var _ cdc.ReplicationManager = (*Postgres)(nil)`
explicitly, because with it off `Subscriber` nothing else would catch dropping it.

**`Event.Position` is an opaque `cdc.Position` string, not a number.** A WAL
offset is the only change log position that fits in a uint64; MySQL's is a set of
UUID ranges or a file and offset, MongoDB's a token, SQL Server's sixteen bytes.
PostgreSQL formats its own as `0/1A2B3C4D` — the same text psql shows — and keeps
the numeric LSN internally for confirm arithmetic (`pendingEvent` carries both,
so the reader never parses back text it just formatted).

**The resume contract:** resuming starts *at or before* the change a position came
from, never after. A resumed stream may repeat, never skips. Proven both ways —
PostgreSQL replays the last event, MySQL replays the whole transaction — so a
consumer needing exactly-once needs its own idempotency.

**`Stream.SubscribeFrom(ctx, Position)`** exists because PostgreSQL remembers
where a subscriber got to and MySQL remembers nothing. Without it, MySQL CDC
could only mean "from now on".

**`ErrNoCDCSupport`** distinguishes a vendor with a pool but no CDC from one never
imported. The old message told a MySQL caller to import a package that would
never have helped.

## PostgreSQL: SubscribeFrom cannot rewind behind the slot

The server accepts a start LSN below `confirmed_flush_lsn` and silently begins at
the confirmed position instead — a stream with a hole in it, indistinguishable
from a complete one, and the WAL behind that point may already be recycled.
`checkResumable` reads `pg_replication_slots.confirmed_flush_lsn` and returns
`ErrPositionBehindSlot` rather than letting that happen. One extra round trip,
only on the SubscribeFrom path.

Found by an integration test asserting the wrong thing: resuming from event 2 of
4 delivered only `r4`, because the first stream had already confirmed past it.

## Two connection kinds, never shared

- **Control** — ordinary `pgconn.PgConn`, lazily opened, guarded by `p.mu` for the
  whole operation. Publication DDL and `pg_publication_tables` queries.
- **Replication** — `replication=database`. Short-lived for slot commands; one
  long-lived instance owned exclusively by the stream.

They cannot be shared. After `START_REPLICATION` the walsender is in CopyBoth where
an ordinary query is protocol-illegal, and `PgConn` is not concurrency-safe anyway.
Running table management against the streaming connection corrupts the protocol.
Slot names must be validated (`slotNamePattern`) because pglogrepl interpolates them
unquoted.

## Stream topology

One goroutine (`run`) owns the connection for the stream's life: reads WAL, decodes,
and sends *every* standby status update. Delivery is decoupled by a buffered channel
(`Config.Buffer`).

`emit` selects on `events <- ev`, the keepalive ticker, and ctx. That is what keeps
keepalives flowing while blocked on a slow consumer — otherwise the server drops the
connection on `wal_sender_timeout` (default 60s).

`run` defer order is load-bearing: `close(events)` → `sendFinalStatus` →
`close(done)`. `Close` cancels, waits on `done`, *then* closes the connection, so the
connection is never touched from two goroutines.

## Position tracking

Three monotonic `atomic.Uint64` advanced via `advance()` (CAS loop):

- `received` — highest LSN off the wire (XLogData and keepalive `ServerWALEnd`).
- `lastPushed` — LSN of the last event handed to the channel. Set **before** the
  channel send, so `catchUp` cannot see an empty channel while an event is in flight.
- `flushed` — what the consumer finished. Set by `All()` *after* `yield` returns.
  This is `confirmed_flush_lsn`, i.e. what releases WAL. At-least-once delivery.

`catchUp(serverEnd)` jumps `flushed` to the server's WAL end only when
`len(events)==0 && lastPushed==flushed`. Without it the position only ever moves on a
row change, so a quiet publication pins every WAL segment written since the last
event and eventually fills the primary's disk.

Start LSN `0` = resume from the slot's `confirmed_flush_lsn`. Using
`IDENTIFY_SYSTEM`'s `XLogPos` instead silently discards everything the slot retained
while disconnected — which is the entire reason the slot exists.

## Decoding

Maps allocated per tuple, sized to the column count, owned by the consumer.
Values are `string` — pgoutput text format; the stream does not carry the
destination Go type.

TupleDataColumn markers: `'n'` NULL → present key, nil value. `'t'` text → decoded.
`'u'` unchanged TOAST → **key omitted**. Conflating `'u'` with `'n'` blanks the
column in any consumer replaying the event.

## Table management

Local `config.Tables` is updated only *after* the server accepts the change, so
`IsTracking` never reports a table the publication lacks. Already-tracked adds and
untracked removes are skipped — `ALTER PUBLICATION ADD TABLE` errors on duplicates.

`quoteQualifiedName` splits on the first `.` and quotes each part. Quoting
`public.users` whole names a table literally called `public.users`.
