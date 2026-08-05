# MySQL CDC — binary log

`vendors/mysql/cdc`, a module of its own **nested inside** the pool vendor. The
binlog reader pulls the TiDB parser, zap and thirty-odd more: pool vendor 11
modules, CDC vendor 47. Someone who wants a MySQL connection pool must not
download a SQL parser.

Vendor name constants come from `vendors/mysql` rather than being repeated, so a
caller cannot end up with a pool under one name and a subscriber under another.

## It is the proof the shared interfaces are not PostgreSQL-shaped

Implements `cdc.Subscriber` and **not** `cdc.ReplicationManager` — there is no
slot and no publication. `TestMySQLCDCOffersNoReplicationManager` asserts the
type assertion *fails*; if it ever succeeds, someone has added four methods that
can only return errors. That test is the whole point of demoting the interface.

## MySQL keeps no per-consumer state, and everything follows from that

- `Subscribe` starts at the **end** of the log. Changes made while nothing was
  streaming are gone. The opposite of PostgreSQL, and the single most important
  thing for a consumer to know.
- `SubscribeFrom` with a position the consumer persisted is the only lossless
  resume. This is why `Stream.SubscribeFrom` was added to the shared interface.
- Falling behind loses data: binlogs expire on age and size
  (`binlog_expire_logs_seconds`) regardless of readers. PostgreSQL's failure mode
  is the primary's disk filling; MySQL's is silent loss.

## Positions are tagged, because the two notations are ambiguous

`gtid:<set>` or `file:<name>:<offset>`. Both contain colons —
`mysql-bin.000042:1234` versus a GTID set — so shape cannot distinguish them, and
reading one as the other resumes somewhere else entirely. GTID is preferred when
the source has it on: a file offset names a place in one server's logs and
survives no failover. Flavour matters: a MySQL GTID set must not parse as MariaDB.

`resume` advances only at **commit** (XID event, or a literal `COMMIT` query for
engines and MariaDB transactions without one). A change delivered mid-transaction
reports the position *before* it, so resuming replays that transaction rather
than skipping it. Confirmed: resuming after r2 delivered `[r2 r3 r4]`.

## The GTID set is shared with the syncer's goroutine — clone it

`BinlogSyncer.StartSyncGTID` **retains and mutates** the set it is given, from its
own goroutine, while the stream's tracker calls `Update` on it. `-race` caught a
map race inside `MysqlGTIDSet`. Both the syncer and the tracker now get
`.Clone()`. Never hand go-mysql a set anything else holds.

## Column names are not in the log by default

A binlog row is values with no names. `binlog_row_metadata=FULL` puts them in the
log — correct by construction, no round trip, and still right for rows written
before a later ALTER. The default is `MINIMAL`, which carries nothing, so names
come from `information_schema` (cached per table) and describe the table *now*.

A column-count disagreement means the table was altered after the row was
written. That is genuinely ambiguous, so it is `ErrSchemaMismatch` rather than
values handed over under names that may not be theirs.

## Table filtering is client-side

MySQL has no subscription to narrow, so the whole binlog crosses the network and
`filter` decides what is decoded. Shared between subscriber and stream so
`AddTables` reaches a *running* stream — otherwise `TableManager` is decorative.
Empty filter means every table. Names are case-folded because MySQL's own case
sensitivity depends on `lower_case_table_names` and therefore the host filesystem.

## Small things that bite

- **`ServerID` is required and must be unique.** Two consumers sharing one make
  the source disconnect the first, repeatedly, without explanation. No default,
  deliberately — a wrong value here is silent.
- **`SHOW MASTER STATUS` was removed in MySQL 8.4.** `SHOW BINARY LOG STATUS` is
  tried first, the old name kept for everything before it. Verified against 8.4.11,
  where the old form is a syntax error.
- **go-mysql logs to stderr by default.** `Logger: slog.New(slog.DiscardHandler)`.
  gpool does no logging and must not let a dependency do it either.
- Values keep the binlog parser's Go types rather than being flattened to text the
  way pgoutput forces on PostgreSQL. `[]byte` is copied to `string` — the consumer
  owns what it is given.

Integration tests need `binlog_format=ROW`; gated on `MYSQL_DSN`. Verified against
MySQL 8.4.11 with `binlog_row_metadata=MINIMAL` and `gtid_mode=ON`, which is the
hard path on both counts.
