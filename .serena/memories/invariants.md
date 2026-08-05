# Invariants

Each of these was a real defect in this codebase. They outrank convenience.

1. **No panic reaches the caller.** Historical sources: nil interface method calls
   after a field was nulled, `sync.Pool` double-Put, `semaphore.Release` over-release
   (which panics the *process*, not the goroutine).

2. **Teardown is idempotent.** `Close`, `Release`, `Commit`, `Rollback`. Guard with
   `atomic.Bool.CompareAndSwap` or `sync.Once`. Users defer them *and* call them
   explicitly; `defer tx.Rollback()` + `tx.Commit()` is the canonical Go idiom and
   must work.

3. **Never recycle an object whose lifetime user code controls.** If it escapes to
   the caller it is not poolable, however hot the path. A guard flag stored *inside*
   a recycled object cannot protect it: after Put, another goroutine Gets it and
   clears the flag, so the original owner's second release corrupts the new owner's
   state. This is why `connWrapper`, `pgRows`, `pgRow`, `pgTx` are allocated per use.

4. **One goroutine owns a connection.** `pgconn.PgConn.lock()` is a debug assertion
   (`switch pgConn.status`), not a mutex — it provides no memory synchronisation.

5. **Caller-owned maps and slices are freshly allocated.** CDC event maps were pooled
   and cleared when the loop body returned, silently emptying any retained event.

6. **Confirm only after the work is done.** CDC flush position advances after the
   iterator body returns, never at parse time.

7. **Validate and default config at construction.** `MaxConns: 0` built a
   zero-capacity semaphore where `Acquire` blocked forever with no error
   (the since-removed `golang.org/x/sync/semaphore`: `n > s.size` → bare
   `<-ctx.Done()`). A zero-capacity token channel fails the same way, so the
   validation is what protects it, not the mechanism.

8. **Classify errors by SQLSTATE, never message text.** Servers localise messages;
   `strings.Contains(err, "already exists")` silently fails on non-English
   `lc_messages`.

9. **A callback must not be invoked while holding the lock it will take.**
   `Postgres.Close` drops `p.mu` before closing the stream, whose completion callback
   takes `p.mu`.

10. **Every drain is bounded.** `Close` must never hang on a leaked resource.
