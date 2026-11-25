// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"errors"
	"runtime"
	"testing"
	"time"
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

// DISCARD ALL deallocates the server's prepared statements. A session that went
// on caching them would keep binding names the server has dropped, and the
// connection's next use would fail with SQLSTATE 26000.
func TestConfigResetQueryDisablesTheStatementCache(t *testing.T) {
	t.Parallel()

	withReset, err := Config{ConnString: testConnString, ResetQuery: "DISCARD ALL"}.parse()
	if err != nil {
		t.Fatalf("parse() = %v", err)
	}
	if withReset.statements != 0 {
		t.Errorf("statements = %d with a reset query, want 0", withReset.statements)
	}

	plain, err := Config{ConnString: testConnString}.parse()
	if err != nil {
		t.Fatalf("parse() = %v", err)
	}
	if plain.statements != DefaultStatementCacheCapacity {
		t.Errorf("statements = %d, want %d", plain.statements, DefaultStatementCacheCapacity)
	}
}

// The per-connection statement cache is multiplied by MaxConns, so its default
// is bounded and an explicit capacity, including none, is honoured.
func TestConfigBoundsPerConnectionCaches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   int
	}{
		{name: "defaults are bounded", config: Config{ConnString: testConnString}, want: DefaultStatementCacheCapacity},
		{name: "explicit capacity is honoured", config: Config{ConnString: testConnString, StatementCacheCapacity: 256}, want: 256},
		{name: "the cache can be disabled", config: Config{ConnString: testConnString, StatementCacheCapacity: DisableCache}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.config.parse()
			if err != nil {
				t.Fatalf("parse() = %v", err)
			}
			if got.statements != tt.want {
				t.Errorf("statements = %d, want %d", got.statements, tt.want)
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

	if raw.statements != defaulted.statements {
		t.Errorf("statements: raw %d, defaulted %d", raw.statements, defaulted.statements)
	}
}
