# Vendors

## Module layout

Each non-PostgreSQL vendor is its own Go module (`vendors/<name>/go.mod`), so a
PostgreSQL-only consumer never downloads another driver and govulncheck never
flags a CVE in something they do not import. Cost: a `go get` per vendor, and a
tag per module per release.

Vendor modules carry `replace github.com/gsoultan/gpool => ../..` for local
development. A replace in a *dependency* is ignored by consumers, so it is safe
to commit; it only affects building and testing that module in place.

## Two kinds of vendor

- **Native** (PostgreSQL/pgx): implements `pooling.Driver[*pgConn]` directly and
  owns its own result types.
- **database/sql** (MySQL, MariaDB, and the ones to come): shares
  `pkg/sqldriver`, which is stdlib-only and lives in the *core* module. A vendor
  module then only builds a `driver.Connector` and registers a name — about a
  hundred lines.

## pkg/sqldriver

Pools `driver.Conn`, not `*sql.DB`. Wrapping `*sql.DB` would hand pooling to
`database/sql` and none of gpool's guarantees would apply: no release gate, no
acquisition metrics, no shared engine behaviour with the native vendors.

Consequences of pooling that low:

- Outbound argument conversion is delegated — `driver.NamedValueChecker` where the
  driver has one, otherwise `driver.DefaultParameterConverter`. Not reimplemented.
- Inbound value conversion *is* written out (`value.go`). `driver.Value` is a
  closed set (nil, int64, float64, bool, []byte, string, time.Time), which makes
  it tractable; `database/sql`'s own converter is long mostly because it also
  handles the outbound direction and reflected kinds. `sql.Scanner` is honoured
  first, so user types keep working.
- Scanned `[]byte` is copied. The driver owns its buffer and reuses it.
- Optional driver interfaces are probed, with fallbacks: `ExecerContext` /
  `QueryerContext` else prepare-and-execute, `Validator` for liveness, `Pinger`
  else `SELECT 1`, `ConnPrepareContext` else `Prepare`.
- A prepared statement created for a query outlives the rows it produced; the
  rows close it.

## The transaction gate is ours, not the driver's

`conn.tx` holds the open `driver.Tx` so release can reach it after the caller has
walked away. `go-sql-driver`'s `ResetSession` does **not** roll back — verified by
removing the gate and watching an abandoned row survive into the next caller.
`ResetSession` is still called after, since a driver that implements it knows what
else has to be cleared.

## driver.Value is not a closed set

The type switches in `value.go` cover what database/sql *documents* — nil, int64,
float64, bool, []byte, string, time.Time — and that is the fast path. But
`driver.Value` is an `any`, and a driver with a richer type system returns its own
native types. ClickHouse hands back `uint8` for a boolean-ish column and `uint64`
for an unsigned integer, so even `SELECT 1` failed to scan until numeric
conversion fell back to reflection over the kind, as database/sql does.

Unsigned destinations read through `toUint64`, not via `int64`, which would
truncate anything above `math.MaxInt64` — a real range for ClickHouse `UInt64` and
MySQL `BIGINT UNSIGNED`.

## MySQL and MariaDB

One implementation, registered under both names, because MariaDB speaks the MySQL
wire protocol. DSN is parsed once in `New` rather than per connection, so a
malformed DSN is reported at construction.

Differences from PostgreSQL a caller sees:
- `Result` carries `LastInsertID() (int64, bool)` by type assertion; PostgreSQL
  has no equivalent, so it is not on `gpool.Result`.
- `Rows.FieldDescriptions` reports column names only — a database/sql driver
  exposes nothing else, and inventing type OIDs would be a lie.
- `parseTime=true` in the DSN, or DATE/DATETIME arrive as raw bytes.
- Keep `MaxConnLifetime` under the server's `wait_timeout` (MySQL default 8h).

## SQL Server

`@p1`-style ordinal placeholders, not `?` or `$1`. go-mssqldb's DSN parser is
lenient: it falls back to ADO `key=value` syntax when a string is not a URL, so
`"://nope"` and `"nonsense"` parse fine and fail later at dial time. Only
genuinely malformed URLs (bad port, bad percent-escape, bad timeout value) are
rejected up front — do not assert otherwise.

**Untestable on Apple Silicon.** SQL Server 2022 publishes no arm64 image and
segfaults under qemu emulation (exit 139, not OOM). The integration tests exist
and are correct; they need an amd64 host.

## ClickHouse

Analytical column store, so it does not behave like the transactional vendors:

- Transactions are not generally available. `Begin` fails unless the server has
  experimental support enabled. The refusal is reported, not hidden, and the
  connection must stay usable afterwards.
- Insert in batches. One row per INSERT is pathological against a column store.
- Size `MaxConns` to what the cluster can run at once, not to client concurrency:
  ClickHouse spends real memory per concurrent query.
- `Config.Options` exists for what a DSN cannot express — compression, TLS,
  multiple hosts for failover.
