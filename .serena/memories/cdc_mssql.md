# SQL Server CDC — change tables

`vendors/mssql/cdc`, a package **inside the pool vendor's module**, not a module
of its own. Unlike the MySQL binlog reader — which drags in the TiDB parser and
thirty-odd modules and therefore had to be isolated — this needs no dependency
the pool does not already have. One driver serves both, so `go.mod` is untouched.

## It is the vendor that is not a log tail

PostgreSQL and MySQL both follow a stream. SQL Server does not: a capture job
owned by SQL Server Agent reads the transaction log on the *server's* schedule
and writes rows into change tables, and a consumer queries those tables. Being
the third vendor and the first of a different shape is what established that
`Position` generalises — its LSN is ten opaque bytes rendered
`0x0000002B00000582001C`, which fits neither a WAL offset nor a GTID set.

Consequences that are not tuning knobs:

- **Latency is the capture job's, about five seconds.** `PollInterval` controls
  how often gpool reads; nothing here makes a slow capture deliver quickly.
- **SQL Server Agent must be running.** Without it `sp_cdc_enable_table`
  succeeds, the change tables are created, and they stay empty — indistinguishable
  from an idle database. The integration fixture skips rather than fails when the
  Agent is absent, and `testdbs.sh` sets `MSSQL_AGENT_ENABLED=true`.
- **CDC cannot be enabled on `master`**, so the tests need their own database.
  `testdbs.sh` provisions `gpoolcdc` and exports `MSSQL_CDC_DSN`.

## The bug that cost the most time

`cdc.fn_cdc_get_all_changes_<instance>` answers a NULL range argument with
`Msg 313: An insufficient number of arguments were supplied` — which names
neither the argument nor the reason, and reproduces with literal LSNs, so it is
not a driver or parameter-binding problem however much it looks like one.

The cause: **every capture instance has its own oldest LSN**, and
`sys.fn_cdc_get_min_lsn` returns NULL for one the capture job has not reached
yet — the normal state for the first seconds after enabling capture.
`readInstance` therefore clamps the window per instance: skip when min_lsn is
NULL, raise `from` to min_lsn when it is behind, skip when the window inverts.

Do not chase this into the driver. Check `sys.fn_cdc_get_min_lsn` first.

## Design notes

- **`all update old`, not `all`.** It makes an update arrive as a before image
  and an after image, which is what lets `Event.Before` be populated the way the
  other vendors populate it. `pair()` joins them; a half pair split by a window
  boundary is dropped rather than delivered as an update with no new value.
- **Metadata columns are recognised by their `__$` prefix, not by position**, so
  a future server that adds one does not shift the data columns.
- **The capture instance name is interpolated into a function name** where it can
  be neither bound nor quoted. It is read from the catalog rather than from a
  caller, and validated against `captureInstancePattern` anyway.
- **Each instance is queried separately, then `orderChanges` merges them** by
  (start_lsn, seqval). A consumer replaying downstream needs the one order the
  server committed in, and per-table queries arrive as several sorted runs.
- **`AddTables` is real DDL**, so unlike MySQL's client-side filter `VerifyTable`
  reports what the server is actually capturing. `SyncTables` disables what is no
  longer wanted, which discards that table's history.
- Implements `cdc.Subscriber` and **not** `cdc.ReplicationManager`: capture
  instances are per-table, which is what `TableManager` already describes.

## Resume semantics match the other two

`Subscribe` starts at the end — SQL Server keeps no per-consumer position, like
MySQL and unlike PostgreSQL. `SubscribeFrom` resumes, inclusively, so the change
the position came from is replayed: at-least-once, confirmed as
`resuming after "0x0000002F000008F30003" delivered [r2 r3 r4]`.

`checkRetained` refuses a position older than any instance's min_lsn with
`ErrPositionExpired`, for the same reason PostgreSQL refuses one behind the
slot: the cleanup job discards on a timer (three days by default), and starting
from whatever remains is a stream with a hole in it that looks complete.

See `mem:cdc` for the shared surface and `mem:cdc_mysql` for the binlog vendor.
