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
