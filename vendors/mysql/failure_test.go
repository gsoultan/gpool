// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package mysql_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/gsoultan/gpool/vendors/mysql"
)

// The PostgreSQL suite proves the pgx path recovers from a failover. This proves
// the other one: MySQL, SQL Server and ClickHouse all reach the pool through
// pkg/sqldriver, so the health gate that retires a dead connection is a different
// piece of code with the same job, and it was equally untested.
//
// Killing connections server-side is what a failover looks like from the pool's
// side, without needing control over the server process.

// killConnections terminates every connection to the test database except the
// one doing the killing, and reports how many it killed.
func killConnections(t *testing.T, server target) int {
	t.Helper()

	control, err := sql.Open("mysql", server.dsn)
	if err != nil {
		t.Fatalf("sql.Open() = %v", err)
	}
	defer control.Close()

	rows, err := control.QueryContext(t.Context(), `
SELECT ID FROM information_schema.PROCESSLIST
WHERE DB = DATABASE() AND ID <> CONNECTION_ID()`)
	if err != nil {
		t.Fatalf("listing connections = %v", err)
	}

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatalf("scanning connection id = %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()

	killed := 0
	for _, id := range ids {
		// A connection that closed between listing and killing is not an error;
		// it is the outcome this is trying to produce.
		if _, err := control.ExecContext(t.Context(), "KILL CONNECTION ?", id); err == nil {
			killed++
		}
	}
	return killed
}

// A pool whose every connection was killed while idle must come back on its own.
//
// As on PostgreSQL, it does not come back on the first attempt: a connection
// killed while idle is not detectably dead without touching it. The first use of
// each corpse fails, that failure retires it, and the pool refills. What must not
// happen is a pool that stays broken.
func TestPoolRecoversAfterConnectionsAreKilled(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 4, MinConns: 4, HealthCheckPeriod: -1})

		// Open them all, so there is something to kill.
		for range 8 {
			var value int
			if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
				t.Fatalf("warming the pool = %v", err)
			}
		}
		if killed := killConnections(t, server); killed == 0 {
			t.Fatal("killed no connections; the pool had none open")
		}

		var failures int
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			var value int
			if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err == nil {
				break
			}
			failures++
		}
		t.Logf("recovered after %d failed queries", failures)

		for i := range 20 {
			var value int
			if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
				t.Fatalf("query %d after recovery = %v", i, err)
			}
		}
		if stat := pool.Stat(); stat.TotalConnections() > 4 {
			t.Errorf("TotalConnections() = %d after recovery, want at most MaxConns (4)",
				stat.TotalConnections())
		}
	})
}

// With one connection the pool has nowhere to hide: it must retire the dead one
// and dial a replacement rather than handing the same corpse out forever.
func TestPoolReplacesItsOnlyConnectionWhenKilled(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 1, HealthCheckPeriod: -1})

		var before int64
		if err := pool.QueryRow(t.Context(), "SELECT CONNECTION_ID()").Scan(&before); err != nil {
			t.Fatalf("QueryRow() = %v", err)
		}
		killConnections(t, server)

		var after int64
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if err := pool.QueryRow(t.Context(), "SELECT CONNECTION_ID()").Scan(&after); err == nil {
				break
			}
		}
		if after == 0 {
			t.Fatal("the pool never produced a working connection again")
		}
		if after == before {
			t.Fatalf("still served connection %d, which was killed", before)
		}
		if stat := pool.Stat(); stat.TotalConnections() > 1 {
			t.Errorf("TotalConnections() = %d, want at most MaxConns (1)", stat.TotalConnections())
		}
	})
}

// A database that is flapping rather than one that failed once. Repeated kills
// while callers work must not leak connections, exceed the ceiling, or wedge it.
func TestPoolSurvivesRepeatedKillsUnderLoad(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		pool := newPool(t, server, mysql.Config{MaxConns: 6, HealthCheckPeriod: -1})

		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
		defer cancel()

		done := make(chan struct{})
		var succeeded, failed int64
		go func() {
			defer close(done)
			for ctx.Err() == nil {
				var value int
				if err := pool.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
					failed++
					continue
				}
				succeeded++
			}
		}()

		rounds := 0
		for ctx.Err() == nil {
			time.Sleep(600 * time.Millisecond)
			killConnections(t, server)
			rounds++
		}
		<-done

		t.Logf("%d kill rounds, %d queries succeeded, %d failed", rounds, succeeded, failed)
		if succeeded == 0 {
			t.Fatal("no query ever succeeded; the pool did not recover between failures")
		}
		if stat := pool.Stat(); stat.TotalConnections() > 6 {
			t.Errorf("TotalConnections() = %d, want at most MaxConns (6)", stat.TotalConnections())
		}

		healthy, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for healthy.Err() == nil {
			var value int
			if err := pool.QueryRow(healthy, "SELECT 1").Scan(&value); err == nil {
				return
			}
		}
		t.Fatal("the pool did not recover after the last kill")
	})
}
