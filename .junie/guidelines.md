# Junie Guidelines

All the code generated must follow all the points of these guidelines.

## Code Guidelines
- **Plan-First Workflow**: Always create a detailed plan and perform deep analysis before execution. Propose best recommendations and handle all edge cases.
- Must Clean Code and SOLID Principle
- Must Programming by Interface and Must use design pattern 
- One interface one file, Rule of Thumb 7 methods
- Interface Segregation Principle (ISP). Clients should not be forced to depend on methods they do not use. If an interface represents different task areas (e.g., UserLoginAndAdministration), split it into smaller, specific interfaces (e.g., UserLogin, UserAdministration).
- Cohesion: All methods in an interface should be closely related and serve a single, clear purpose.
- One struct one file
- Keep the struct method small. if the logic in one method too big, then composite the logic into small function. separate all the reusable function into the internal or pkg folder.
- No nested if
- Avoid unnecessary IF-THEN statements in loops, you can use strategy pattern, pre filtering, etc.
- Must be Object Oriented
- Must use Modern Go 1.26 syntax
- **Avoid stuttering** — filenames must not have a suffix that matches their parent folder name (e.g., use `backend.go` instead of `backend_service.go` in a `service/` folder). Similarly, symbol names (structs, interfaces) must not repeat the package name (e.g., use `service.Backend` instead of `service.BackendService`).
- **Reliability** — ensure no error, no panic, and no unexpected behavior in production. Avoid panic code and ensure error handling is robust (no raw error codes).
- **Security** — ensure no exploit code, no vulnerable code, and no zero-day vulnerabilities code follow secure coding practices.

## Third party library
- No vulnerabilities 3rd party library and have high performance

## Performance & Lightweight
- **No memory leaks** — always `Close()` connections and response bodies; `Cancel()` contexts when done; avoid goroutine leaks by using bounded workers or select-with-context; prioritize **low memory code**.
- **Modern Idioms (Go 1.26)** — use `new(val)`, `for i := range n`, `strings.SplitSeq`, iterators (`maps.Keys/Values`, `slices.Collect/Sorted`), `slices` package helpers, `errors.Is/AsType/Join`, `wg.Go(fn)`, `t.Context()`, `b.Loop()`, and `omitzero` JSON tags
- **High performance** — avoid allocations in hot paths; reuse buffers; prefer `sync.Pool` for frequently allocated objects; use `strings.Builder` for string concatenation; profile before optimizing
- **Lightweight** — keep dependencies minimal; prefer stdlib; avoid heavy reflection or codegen where simple code suffices
- **PgBouncer Integration** — follow the [PgBouncer Best Practices](pgbouncer-guidelines.mdc) for all database connection pooling and configurations. Use transaction mode for high performance.

## Post-Task Cleanup & Maintenance
- **Knowledge Update**: Always update **Graphify**, **Serena**, and **Obsidian** knowledge at the end of every task.
- **README Update**: Always update `README.md` at the end of every task to reflect the latest changes.
- **Cleanup**: Always remove any temporary files, logs, or unnecessary artifacts created during the task.

## Testing
- **TDD Mandatory**: Strictly follow Test-Driven Development (TDD). Write failing tests first to define expected behavior before implementation.
- Mock interfaces, not concrete types — enables easy unit testing
- Prefer table-driven tests
