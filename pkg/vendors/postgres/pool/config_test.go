// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const testConnString = "postgres://user:pass@127.0.0.1:5432/testdb"

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:   "minimal valid config",
			config: Config{ConnString: testConnString},
		},
		{
			name:   "min below max",
			config: Config{ConnString: testConnString, MaxConns: 10, MinConns: 2},
		},
		{
			name:    "missing conn string",
			config:  Config{},
			wantErr: true,
		},
		{
			name:    "negative max conns",
			config:  Config{ConnString: testConnString, MaxConns: -1},
			wantErr: true,
		},
		{
			name:    "negative min conns",
			config:  Config{ConnString: testConnString, MinConns: -1},
			wantErr: true,
		},
		{
			name:    "min above max",
			config:  Config{ConnString: testConnString, MaxConns: 2, MinConns: 5},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.validate()
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("validate() = %v, want ErrInvalidConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validate() = %v, want nil", err)
			}
		})
	}
}

// A zero MaxConns used to build a zero-capacity semaphore, where every Acquire
// blocked forever instead of failing.
func TestConfigDefaultsGiveUsableCapacity(t *testing.T) {
	t.Parallel()

	got := Config{ConnString: testConnString}.withDefaults()

	if want := int32(max(minDefaultMaxConns, runtime.GOMAXPROCS(0))); got.MaxConns != want {
		t.Fatalf("MaxConns = %d, want %d", got.MaxConns, want)
	}
	if got.MaxConnLifetime != DefaultMaxConnLifetime {
		t.Errorf("MaxConnLifetime = %v, want %v", got.MaxConnLifetime, DefaultMaxConnLifetime)
	}
	if got.MaxConnIdleTime != DefaultMaxConnIdleTime {
		t.Errorf("MaxConnIdleTime = %v, want %v", got.MaxConnIdleTime, DefaultMaxConnIdleTime)
	}
	if got.HealthCheckPeriod != DefaultHealthCheckPeriod {
		t.Errorf("HealthCheckPeriod = %v, want %v", got.HealthCheckPeriod, DefaultHealthCheckPeriod)
	}
	if got.ResetQueryTimeout != DefaultResetQueryTimeout {
		t.Errorf("ResetQueryTimeout = %v, want %v", got.ResetQueryTimeout, DefaultResetQueryTimeout)
	}
	if got.ConnectTimeout != DefaultConnectTimeout {
		t.Errorf("ConnectTimeout = %v, want %v", got.ConnectTimeout, DefaultConnectTimeout)
	}
}

func TestConfigDefaultsPreserveExplicitValues(t *testing.T) {
	t.Parallel()

	config := Config{
		ConnString:        testConnString,
		MaxConns:          7,
		MaxConnLifetime:   -1,
		MaxConnIdleTime:   -1,
		HealthCheckPeriod: -1,
		ResetQueryTimeout: 2 * time.Second,
		ConnectTimeout:    3 * time.Second,
	}

	got := config.withDefaults()

	if got.MaxConns != 7 {
		t.Errorf("MaxConns = %d, want 7", got.MaxConns)
	}
	// A negative duration is an explicit "disable", not an unset field.
	if got.MaxConnLifetime != -1 {
		t.Errorf("MaxConnLifetime = %v, want -1", got.MaxConnLifetime)
	}
	if got.HealthCheckPeriod != -1 {
		t.Errorf("HealthCheckPeriod = %v, want -1", got.HealthCheckPeriod)
	}
}

func TestConfigParseRejectsBadConnString(t *testing.T) {
	t.Parallel()

	if _, err := (Config{ConnString: "://nonsense"}).parse(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("parse() = %v, want ErrInvalidConfig", err)
	}
}

// DISCARD ALL deallocates the server's prepared statements. Leaving pgx's default
// statement-caching mode in place makes the client keep referencing names the
// server has dropped, and the connection's next use fails with SQLSTATE 26000.
func TestConfigResetQuerySelectsACompatibleExecMode(t *testing.T) {
	t.Parallel()

	withReset, err := Config{ConnString: testConnString, ResetQuery: "DISCARD ALL"}.parse()
	if err != nil {
		t.Fatalf("parse() = %v", err)
	}
	if withReset.DefaultQueryExecMode != resetSafeQueryExecMode {
		t.Errorf("DefaultQueryExecMode = %v, want %v", withReset.DefaultQueryExecMode, resetSafeQueryExecMode)
	}

	// Without a reset query, and with a statement cache to use, the driver's own
	// default is left alone.
	plain, err := Config{ConnString: testConnString}.parse()
	if err != nil {
		t.Fatalf("parse() = %v", err)
	}
	if plain.DefaultQueryExecMode != pgx.QueryExecModeCacheStatement {
		t.Errorf("DefaultQueryExecMode = %v, want the driver default", plain.DefaultQueryExecMode)
	}
}

// The per-connection statement and description caches are the largest slice of the
// pool's memory. pgx preallocates each one's map at capacity, so the default is set
// well below pgx's own 512.
func TestConfigBoundsPerConnectionCaches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		config          Config
		wantStatement   int
		wantDescription int
		wantExecMode    pgx.QueryExecMode
	}{
		{
			name:            "defaults are bounded",
			config:          Config{ConnString: testConnString},
			wantStatement:   DefaultStatementCacheCapacity,
			wantDescription: DefaultDescriptionCacheCapacity,
			wantExecMode:    pgx.QueryExecModeCacheStatement,
		},
		{
			name:            "explicit capacity is honoured",
			config:          Config{ConnString: testConnString, StatementCacheCapacity: 256, DescriptionCacheCapacity: 8},
			wantStatement:   256,
			wantDescription: 8,
			wantExecMode:    pgx.QueryExecModeCacheStatement,
		},
		{
			// With no statement cache the default exec mode has nowhere to cache
			// into, so the connection must move off named server-side statements.
			name:            "disabling the statement cache changes the exec mode",
			config:          Config{ConnString: testConnString, StatementCacheCapacity: DisableCache},
			wantStatement:   0,
			wantDescription: DefaultDescriptionCacheCapacity,
			wantExecMode:    resetSafeQueryExecMode,
		},
		{
			name:            "both caches disabled",
			config:          Config{ConnString: testConnString, StatementCacheCapacity: DisableCache, DescriptionCacheCapacity: DisableCache},
			wantStatement:   0,
			wantDescription: 0,
			wantExecMode:    resetSafeQueryExecMode,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.config.parse()
			if err != nil {
				t.Fatalf("parse() = %v", err)
			}
			if got.StatementCacheCapacity != tt.wantStatement {
				t.Errorf("StatementCacheCapacity = %d, want %d", got.StatementCacheCapacity, tt.wantStatement)
			}
			if got.DescriptionCacheCapacity != tt.wantDescription {
				t.Errorf("DescriptionCacheCapacity = %d, want %d", got.DescriptionCacheCapacity, tt.wantDescription)
			}
			if got.DefaultQueryExecMode != tt.wantExecMode {
				t.Errorf("DefaultQueryExecMode = %v, want %v", got.DefaultQueryExecMode, tt.wantExecMode)
			}
		})
	}
}

// parse must not depend on withDefaults having run first.
func TestConfigParseIsOrderIndependent(t *testing.T) {
	t.Parallel()

	raw, err := Config{ConnString: testConnString}.parse()
	if err != nil {
		t.Fatalf("parse() = %v", err)
	}
	defaulted, err := Config{ConnString: testConnString}.withDefaults().parse()
	if err != nil {
		t.Fatalf("withDefaults().parse() = %v", err)
	}

	if raw.StatementCacheCapacity != defaulted.StatementCacheCapacity {
		t.Errorf("StatementCacheCapacity: raw %d, defaulted %d", raw.StatementCacheCapacity, defaulted.StatementCacheCapacity)
	}
	if raw.DefaultQueryExecMode != defaulted.DefaultQueryExecMode {
		t.Errorf("DefaultQueryExecMode: raw %v, defaulted %v", raw.DefaultQueryExecMode, defaulted.DefaultQueryExecMode)
	}
}
