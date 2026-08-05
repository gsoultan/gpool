# Testing

## Layout

- `pkg/**/​*_test.go` — unit, no database. Mocks satisfy `pgx.Rows` / `pgx.Tx` /
  `gpool.Pool` / `cdc.Subscriber` in `mock_test.go`.
- `integration/` — real server, `t.Skip` unless `DATABASE_URL` is set.
- `benchmarks/` — gpool vs pgxpool vs database/sql vs PgBouncer, same workload.

Table-driven. Mock interfaces, never concrete types. Every fixed bug gets a
regression test whose comment states the old failure mode — that comment is the
test's real payload.

## Unit tests structurally cannot see driver state

Two real defects survived a green unit suite and were caught only downstream:

- `Row.Release()` without `Scan` left the pgx query open; the pooled connection came
  back `conn busy`. Caught by an integration test using `MaxConns: 1`.
- `ResetQuery: "DISCARD ALL"` invalidated pgx's statement cache → `SQLSTATE 26000`.
  Caught by a benchmark, because it repeats one statement across releases.

Exercise the real driver before calling a change done. `MaxConns: 1` is the sharpest
tool available: it turns any leaked connection into a hang instead of a silent pass.

## Benchmark hygiene

- Match capacity on both sides of a comparison, or you measure queueing and call it
  overhead. See `mem:scale`.
- Pool-mechanics benchmarks need ~200k iterations before allocs/op settles.
- `testing.AllocsPerRun` panics inside a `t.Parallel()` test.

## Multi-database tests

`integration/multidb_test.go` provisions throwaway databases from `DATABASE_URL` by
swapping the URL path, and drops them in cleanup after terminating stray backends.
Covers routing, data isolation, saturation containment (one pool exhausted must not
starve another), concurrent cross-database load, and `Engine.Close` closing every pool.
Skips when the role lacks CREATEDB or `DATABASE_URL` is not a URL.

## Running

```
go test -race ./pkg/...
DATABASE_URL=... go test -race ./integration/
DATABASE_URL=... go test -bench=. ./benchmarks/
```

Server for integration/bench (needs logical replication):

```
podman run -d --rm --name gpool-test -e POSTGRES_PASSWORD=postgres -p 55432:5432 \
  docker.io/library/postgres:17-alpine \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
```

CDC fixtures must drop their slot in cleanup. An abandoned slot retains WAL forever.
