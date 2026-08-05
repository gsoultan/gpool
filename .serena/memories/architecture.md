# Architecture

Interfaces in `pkg/gpool` + `pkg/gpool/cdc`. Implementations in
`pkg/vendors/<vendor>/`. Nothing public under `internal/` — that made the library
unimportable and is the reason the tree was restructured.

## Packages

- `pkg/gpool` — `Pool`, `Conn`, `Tx`, `Rows`, `Row`, `Result`, `Stat`, `Field`,
  `Engine`, vendor registry, sentinel errors.
  `Engine` holds named registries of both pools and subscribers. Several databases
  means one pool per database (`AddPool`/`Pool(name)`/`RemovePool`/`Pools`) sharing
  nothing — separate capacity is what stops one saturated backend starving the rest.
  `Pool()` and `Subscriber()` with no argument resolve `DefaultPool`/`DefaultSubscriber`,
  so single-database callers are unaffected.
- `pkg/gpool/cdc` — `Subscriber` composed from `Stream` + `TableManager` +
  `ReplicationManager` + `Close`. Also `EventStream`, `Event`, `Op`.
- `pkg/vendors/postgres/pool` — pgx/v5.
- `pkg/vendors/postgres/cdc` — pglogrepl.
- `integration/` — real-server tests. `benchmarks/` — comparative benchmarks.
- `examples/gpoolproxy` — own module, a PostgreSQL pooler on `pkg/pooling`. The
  one thing a library cannot do is bound connections across applications; that
  needs a process. Also the generality proof for the engine — it drives `Core`
  with a socket rather than a database driver. See `mem:proxy`.

## Conventions

- One struct or one interface per file. Filename must not restate the folder.
  Symbol must not restate the package.
- ISP: interfaces ≤7 methods, cohesive; assemble the full surface by composition.
- Compile-time proofs at the top of each impl file:
  `var _ gpool.Pool = (*Postgres)(nil)`.
- Vendors self-register from `init()`, database/sql style. Importing the vendor
  package is the *only* thing that makes `gpool.Postgres` resolvable — the factory
  error says so explicitly because a missing import is the usual cause.
- Registry is `sync.RWMutex`-guarded; `RegisterPool`/`RegisterSubscriber` return
  errors and are discarded in `init()` (unreachable there, and panicking at program
  start is worse).
- Adding a vendor must need zero edits to `pkg/gpool`.

Dependencies are deliberately minimal: pgx/v5 and pglogrepl, nothing else.
`golang.org/x/sync` was dropped when the semaphore gave way to a token channel
(`mem:scale`). Adding one back needs justification.
