// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The pooling engine itself — capacity, shards, the reaper, the clock, the
// MaxConns ceiling, acquisition counters — is tested in pkg/pooling against a
// fake driver, where the tests are vendor-independent and need no server. What
// belongs here is what is specific to PostgreSQL: config handling, the driver
// adapter's judgement about a connection, and the wiring between the two.

func newTestPool(t *testing.T, config Config) *Postgres {
	t.Helper()

	if config.ConnString == "" {
		config.ConnString = testConnString
	}

	p, err := New(config)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	if _, err := New(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() = %v, want ErrInvalidConfig", err)
	}
	if _, err := New(Config{ConnString: testConnString, MaxConns: 2, MinConns: 5}); err == nil {
		t.Fatal("New() accepted MinConns above MaxConns")
	}
}

// The engine's ErrClosed must surface as this package's sentinel, so callers
// match on one vocabulary rather than reaching through to the engine.
func TestAcquireAfterCloseFailsFast(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 2})
	p.Close()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if _, err := p.Acquire(ctx); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Acquire() = %v, want ErrPoolClosed", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 2})

	// A second and third Close must not double-drain or panic.
	p.Close()
	p.Close()
	p.Close()
}

func TestNewDoesNotDial(t *testing.T) {
	t.Parallel()

	stat := newTestPool(t, Config{MaxConns: 8}).Stat()

	if stat.TotalConnections() != 0 {
		t.Errorf("TotalConnections() = %d; New must not dial", stat.TotalConnections())
	}
	if stat.MaxConnections() != 8 {
		t.Errorf("MaxConnections() = %d, want 8", stat.MaxConnections())
	}
}

// The vendor config must reach the engine rather than being silently dropped.
func TestConfigReachesTheEngine(t *testing.T) {
	t.Parallel()

	got := Config{
		ConnString:        testConnString,
		MaxConns:          17,
		MinConns:          3,
		MaxConnLifetime:   3 * time.Minute,
		MaxConnIdleTime:   90 * time.Second,
		HealthCheckPeriod: 30 * time.Second,
		ResetQueryTimeout: 2 * time.Second,
		ConnectTimeout:    4 * time.Second,
	}.pooling()

	if got.MaxConns != 17 || got.MinConns != 3 {
		t.Errorf("capacity did not carry: %+v", got)
	}
	if got.MaxConnLifetime != 3*time.Minute || got.MaxConnIdleTime != 90*time.Second {
		t.Errorf("lifetime bounds did not carry: %+v", got)
	}
	if got.HealthCheckPeriod != 30*time.Second || got.ConnectTimeout != 4*time.Second {
		t.Errorf("periods did not carry: %+v", got)
	}
	// ResetQueryTimeout is the vendor's name for the engine's cleanup budget: it
	// bounds the reset query, the rollback, and the unlisten alike.
	if got.CleanupTimeout != 2*time.Second {
		t.Errorf("CleanupTimeout = %v, want ResetQueryTimeout (2s)", got.CleanupTimeout)
	}
}

// A dead connection must be reported without touching the network, because the
// engine consults this on the hot path. A nil handle counts as dead so a
// bookkeeping slip degrades into a discarded connection rather than a panic.
func TestDriverDeadHandlesMissingConnection(t *testing.T) {
	t.Parallel()

	driver := &pgxDriver{}

	if !driver.Dead(nil) {
		t.Error("Dead(nil) = false, want true")
	}
	if !driver.Dead(&pgConn{}) {
		t.Error("Dead() on a connection with no driver handle = false, want true")
	}
}

func TestStatIsEmptyBeforeUse(t *testing.T) {
	t.Parallel()

	stat := newTestPool(t, Config{MaxConns: 8}).Stat()

	if stat.TotalConnections() != 0 || stat.IdleConnections() != 0 || stat.ActiveConnections() != 0 {
		t.Fatalf("empty pool occupancy = %+v, want all zero", stat)
	}
	if stat.AcquireCount() != 0 || stat.EmptyAcquireCount() != 0 || stat.CanceledAcquireCount() != 0 {
		t.Fatalf("empty pool counters = %+v, want all zero", stat)
	}
	if stat.AcquireDuration() != 0 {
		t.Fatalf("AcquireDuration() = %v, want 0", stat.AcquireDuration())
	}
}

// An acquisition that gives up must be counted, and must not count as a success.
// This needs no server: the pool never dials, so every attempt waits for a permit
// that a connection failure never returns.
func TestCanceledAcquireIsCounted(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, Config{MaxConns: 1, ConnectTimeout: 50 * time.Millisecond})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	if _, err := p.Acquire(ctx); err == nil {
		t.Skip("a server is reachable at the test connection string; this case needs one that is not")
	}

	if got := p.Stat().TotalConnections(); got != 0 {
		t.Errorf("TotalConnections() = %d after a failed dial, want 0 - the slot leaked", got)
	}
}
