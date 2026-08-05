# Gpool Core

Graph root. Library-only Go module for PostgreSQL pooling + CDC. Module path
`github.com/gsoultan/gpool`. No binary, no config file, no logger, no metrics server,
no process-global mutable state beyond the vendor registry.

Role profiles and the full working contract live in `AGENTS.md` at the repo root;
`CLAUDE.md` imports it. Read those before non-trivial changes.

## Read next

- Package layout, interface boundaries, vendor registration: `mem:architecture`
- Rules that outrank convenience and have all caused production defects here:
  `mem:invariants`
- Connection pool internals — sharding, lifecycle, reaper, ResetQuery exec-mode
  coupling: `mem:pool`
- Memory and CPU under heavy client concurrency, what the profiles actually showed,
  and benchmark hygiene: `mem:scale`
- How vendors are packaged, the shared database/sql adapter, and what differs
  between a native vendor and a database/sql one: `mem:vendors`
- Logical replication internals — connection split, LSN confirmation, WAL retention:
  `mem:cdc`
- Test layout, what unit tests structurally cannot catch, how to get a server:
  `mem:testing`
- How to add/update memories here: `mem:memory_maintenance`
