// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package mssql

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// These tests need no server. The pooling behaviour itself lives in
// pkg/sqldriver and is covered there against a fake driver and against two real
// engines; what is specific to this package is DSN handling, config projection,
// and registration.

func TestConnectorRejectsBadDSN(t *testing.T) {
	t.Parallel()

	// Only what the driver genuinely rejects. Its parser falls back to ADO-style
	// key=value syntax when a string is not a URL, so shapes like "://nope" or
	// "nonsense" are accepted as (empty) parameter strings and fail later at dial
	// time instead. That is the driver's contract, not something to assert against.
	tests := map[string]string{
		"empty":       "",
		"bad port":    "sqlserver://host:notaport?database=app",
		"bad percent": "sqlserver://user:%zz@host?database=app",
		"bad timeout": "sqlserver://h?connection+timeout=abc",
	}

	for name, dsn := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := New(Config{DSN: dsn}); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New(%q) = %v, want ErrInvalidConfig", dsn, err)
			}
		})
	}
}

// A malformed DSN must be reported at construction, not deferred to whichever
// caller first tries to acquire a connection.
func TestNewParsesTheDSNUpFront(t *testing.T) {
	t.Parallel()

	_, err := New(Config{DSN: "sqlserver://host:notaport"})
	if err == nil {
		t.Fatal("New() accepted a malformed DSN")
	}
	if !strings.Contains(err.Error(), "parsing DSN") {
		t.Errorf("error should name the DSN as the problem, got: %v", err)
	}
}

// Both DSN dialects the driver accepts must reach a usable connector; neither
// dials, so this stays server-free.
func TestConnectorAcceptsBothDSNDialects(t *testing.T) {
	t.Parallel()

	dialects := map[string]string{
		"url": "sqlserver://user:pass@localhost:1433?database=app",
		"ado": "server=localhost;user id=user;password=pass;database=app",
	}

	for name, dsn := range dialects {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pool, err := New(Config{DSN: dsn, MaxConns: 2})
			if err != nil {
				t.Fatalf("New() = %v", err)
			}
			defer pool.Close()

			if got := pool.Stat().MaxConnections(); got != 2 {
				t.Errorf("MaxConnections() = %d, want 2", got)
			}
		})
	}
}

// The pooling knobs must reach the engine rather than being silently dropped.
func TestConfigReachesTheEngine(t *testing.T) {
	t.Parallel()

	pool, err := New(Config{
		DSN:               "sqlserver://user:pass@localhost:1433?database=app",
		MaxConns:          17,
		MinConns:          0,
		MaxConnLifetime:   3 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	defer pool.Close()

	stat := pool.Stat()
	if stat.MaxConnections() != 17 {
		t.Errorf("MaxConnections() = %d, want 17", stat.MaxConnections())
	}
	if stat.TotalConnections() != 0 {
		t.Errorf("TotalConnections() = %d; New must not dial", stat.TotalConnections())
	}
}

func TestConfigRejectsMinAboveMax(t *testing.T) {
	t.Parallel()

	_, err := New(Config{
		DSN:      "sqlserver://user:pass@localhost:1433?database=app",
		MaxConns: 2,
		MinConns: 5,
	})
	if err == nil {
		t.Fatal("New() accepted MinConns above MaxConns")
	}
}

func TestVendorIsRegistered(t *testing.T) {
	t.Parallel()

	pool, err := gpool.NewPool(SQLServer, Config{
		DSN:      "sqlserver://user:pass@localhost:1433?database=app",
		MaxConns: 2,
	})
	if err != nil {
		t.Fatalf("NewPool(%s) = %v", SQLServer, err)
	}
	defer pool.Close()
}

func TestRegistryRejectsTheWrongConfigType(t *testing.T) {
	t.Parallel()

	if _, err := gpool.NewPool(SQLServer, "not a config"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewPool() with the wrong config type = %v, want ErrInvalidConfig", err)
	}
}
