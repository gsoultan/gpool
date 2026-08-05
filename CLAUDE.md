# gpool

@AGENTS.md

## Quick reference

gpool is a **library only**. If a change would add a binary, a config file, a logger,
a metrics server, or any process-global state, it does not belong here.

**Layout**

| Path | Contents |
| :--- | :--- |
| `pkg/gpool` | `Pool`, `Conn`, `Tx`, `Rows`, `Row`, `Result`, `Stat`, `Engine`, vendor registry |
| `pkg/gpool/cdc` | `Subscriber`, `Stream`, `TableManager`, `ReplicationManager`, `EventStream`, `Event` |
| `pkg/vendors/postgres/pool` | PostgreSQL pool (`pgx/v5`) |
| `pkg/vendors/postgres/cdc` | PostgreSQL logical replication (`pglogrepl`) |
| `integration/` | Tests against a real server, skipped unless `DATABASE_URL` is set |
| `benchmarks/` | gpool vs `pgxpool` vs `database/sql` vs PgBouncer |

**Verify a change**

```bash
gofmt -l . && go vet ./... && go test -race ./pkg/...
```

**Verify against a real server** — several classes of bug in this codebase are
invisible to unit tests, so do this before calling a change done:

```bash
podman run -d --rm --name gpool-test \
  -e POSTGRES_PASSWORD=postgres -p 55432:5432 \
  docker.io/library/postgres:17-alpine \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10

export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:55432/postgres'
go test -race ./integration/
go test -bench=Gpool ./benchmarks/

podman stop gpool-test
```
