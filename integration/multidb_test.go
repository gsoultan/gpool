// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package integration

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

// databaseNamed returns the configured connection string pointed at a different
// database on the same server.
func databaseNamed(t *testing.T, name string) string {
	t.Helper()

	parsed, err := url.Parse(connString(t))
	if err != nil || parsed.Scheme == "" {
		t.Skipf("DATABASE_URL is not a URL, cannot derive a sibling database: %v", err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

// provisionDatabases creates throwaway databases and returns their connection
// strings, dropping them when the test ends.
func provisionDatabases(t *testing.T, names ...string) []string {
	t.Helper()

	admin := newPool(t, postgrespool.Config{MaxConns: 2})
	ctx := t.Context()

	conns := make([]string, 0, len(names))
	for _, name := range names {
		// CREATE DATABASE cannot run inside a transaction, so it goes out on its own.
		_, _ = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %q", name))
		if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
			t.Skipf("cannot create database %q (needs CREATEDB): %v", name, err)
		}
		conns = append(conns, databaseNamed(t, name))
	}

	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, name := range names {
			// Evict stragglers so the drop is not blocked by a lingering backend.
			_, _ = admin.Exec(cleanup,
				`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, name)
			_, _ = admin.Exec(cleanup, fmt.Sprintf("DROP DATABASE IF EXISTS %q", name))
		}
	})

	return conns
}

// multiDBEngine builds an engine holding one pool per database, keyed by name.
func multiDBEngine(t *testing.T, byName map[string]string, config postgrespool.Config) *gpool.Engine {
	t.Helper()

	engine := gpool.NewEngine(nil, nil)
	for name, conn := range byName {
		config.ConnString = conn
		pool, err := gpool.NewPool(gpool.Postgres, config)
		if err != nil {
			t.Fatalf("NewPool(%s) = %v", name, err)
		}
		engine.AddPool(name, pool)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

// Each pool must reach its own database and only its own.
func TestMultiDatabaseRoutesToTheRightBackend(t *testing.T) {
	conns := provisionDatabases(t, "gpool_multidb_a", "gpool_multidb_b")

	engine := multiDBEngine(t, map[string]string{
		"alpha": conns[0],
		"beta":  conns[1],
	}, postgrespool.Config{MaxConns: 4})

	ctx := t.Context()

	for name, want := range map[string]string{"alpha": "gpool_multidb_a", "beta": "gpool_multidb_b"} {
		pool := engine.Pool(name)
		if pool == nil {
			t.Fatalf("Pool(%s) = nil", name)
		}

		var got string
		if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&got); err != nil {
			t.Fatalf("Pool(%s).QueryRow() = %v", name, err)
		}
		if got != want {
			t.Errorf("Pool(%s) reached database %q, want %q", name, got, want)
		}
	}
}

// Data written through one pool must not be visible through another. This is what
// separates "several pools" from "one pool with several connection strings".
func TestMultiDatabaseIsolatesData(t *testing.T) {
	conns := provisionDatabases(t, "gpool_multidb_iso_a", "gpool_multidb_iso_b")

	engine := multiDBEngine(t, map[string]string{
		"alpha": conns[0],
		"beta":  conns[1],
	}, postgrespool.Config{MaxConns: 4})

	ctx := t.Context()
	alpha, beta := engine.Pool("alpha"), engine.Pool("beta")

	for _, pool := range []gpool.Pool{alpha, beta} {
		if _, err := pool.Exec(ctx, "CREATE TABLE items (id int PRIMARY KEY, label text)"); err != nil {
			t.Fatalf("CREATE TABLE = %v", err)
		}
	}

	if _, err := alpha.Exec(ctx, "INSERT INTO items (id, label) VALUES (1, 'from-alpha')"); err != nil {
		t.Fatalf("INSERT = %v", err)
	}

	var count int
	if err := beta.QueryRow(ctx, "SELECT count(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("SELECT on beta = %v", err)
	}
	if count != 0 {
		t.Fatalf("beta sees %d rows written through alpha; the pools are not isolated", count)
	}

	if err := alpha.QueryRow(ctx, "SELECT count(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("SELECT on alpha = %v", err)
	}
	if count != 1 {
		t.Fatalf("alpha sees %d rows, want 1", count)
	}
}

// Saturating one database must not starve another. Each pool has its own capacity,
// so a backend that stops responding is contained rather than contagious.
func TestMultiDatabaseSaturationIsContained(t *testing.T) {
	conns := provisionDatabases(t, "gpool_multidb_sat_a", "gpool_multidb_sat_b")

	engine := multiDBEngine(t, map[string]string{
		"busy": conns[0],
		"calm": conns[1],
	}, postgrespool.Config{MaxConns: 2})

	ctx := t.Context()
	busy, calm := engine.Pool("busy"), engine.Pool("calm")

	// Hold every permit the busy pool has.
	held := make([]gpool.Conn, 0, 2)
	for range 2 {
		conn, err := busy.Acquire(ctx)
		if err != nil {
			t.Fatalf("Acquire() = %v", err)
		}
		held = append(held, conn)
	}
	defer func() {
		for _, conn := range held {
			conn.Release()
		}
	}()

	// The busy pool is now exhausted.
	saturated, cancelSaturated := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancelSaturated()
	if _, err := busy.Acquire(saturated); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("busy pool Acquire() = %v, want a deadline", err)
	}

	// The calm pool is unaffected.
	unaffected, cancelUnaffected := context.WithTimeout(ctx, 5*time.Second)
	defer cancelUnaffected()

	var value int
	if err := calm.QueryRow(unaffected, "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("a saturated pool starved an unrelated database: %v", err)
	}
	if value != 1 {
		t.Fatalf("got %d, want 1", value)
	}
}

// Concurrent traffic across databases, with every caller checking it landed on the
// right one. A shared connection or a misrouted acquire shows up here.
func TestMultiDatabaseUnderConcurrentLoad(t *testing.T) {
	conns := provisionDatabases(t, "gpool_multidb_load_a", "gpool_multidb_load_b")

	want := map[string]string{
		"alpha": "gpool_multidb_load_a",
		"beta":  "gpool_multidb_load_b",
	}
	engine := multiDBEngine(t, map[string]string{
		"alpha": conns[0],
		"beta":  conns[1],
	}, postgrespool.Config{MaxConns: 8, MinConns: 2})

	var wg sync.WaitGroup
	errs := make(chan error, 256)

	for i := range 256 {
		name := "alpha"
		if i%2 == 1 {
			name = "beta"
		}

		wg.Go(func() {
			var got string
			if err := engine.Pool(name).QueryRow(context.Background(), "SELECT current_database()").Scan(&got); err != nil {
				errs <- fmt.Errorf("pool %s: %w", name, err)
				return
			}
			if got != want[name] {
				errs <- fmt.Errorf("pool %s reached %q, want %q", name, got, want[name])
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}

	for name := range want {
		if got := engine.Pool(name).Stat().ActiveConnections(); got != 0 {
			t.Errorf("pool %s has %d active connections after every caller finished", name, got)
		}
	}
}

// Engine.Close must close every registered pool, whatever the count.
func TestMultiDatabaseEngineCloseClosesAll(t *testing.T) {
	conns := provisionDatabases(t, "gpool_multidb_close_a", "gpool_multidb_close_b")

	engine := gpool.NewEngine(nil, nil)
	for i, conn := range conns {
		pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{ConnString: conn, MaxConns: 2})
		if err != nil {
			t.Fatalf("NewPool() = %v", err)
		}
		engine.AddPool(fmt.Sprintf("db%d", i), pool)
	}

	// Establish a connection in each so there is something to close.
	for _, name := range engine.Pools() {
		var value int
		if err := engine.Pool(name).QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
			t.Fatalf("QueryRow on %s = %v", name, err)
		}
	}

	pools := make([]gpool.Pool, 0, 2)
	for _, name := range engine.Pools() {
		pools = append(pools, engine.Pool(name))
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	for i, pool := range pools {
		if _, err := pool.Acquire(context.Background()); err == nil {
			t.Errorf("pool %d still accepts acquisitions after Engine.Close", i)
		}
		if got := pool.Stat().TotalConnections(); got != 0 {
			t.Errorf("pool %d still holds %d connections after Engine.Close", i, got)
		}
	}
}
