// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"fmt"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
)

// Defaults applied by Config.withDefaults for any field left at its zero value.
const (
	DefaultMaxConnLifetime   = time.Hour
	DefaultMaxConnIdleTime   = 30 * time.Minute
	DefaultHealthCheckPeriod = time.Minute
	DefaultResetQueryTimeout = 5 * time.Second
	DefaultConnectTimeout    = 10 * time.Second

	// DefaultStatementCacheCapacity and DefaultDescriptionCacheCapacity bound the
	// two per-connection caches pgx keeps.
	//
	// pgx defaults both to 512 and preallocates each one's map at that capacity, so
	// they cost roughly 74 KiB per connection before a single statement is prepared
	// — measured as 57% of the pool's entire heap footprint. That trade suits one
	// connection; multiplied across a pool it dominates. 64 covers the query variety
	// of a typical service while cutting the per-connection cost by about an order
	// of magnitude, and raising it is one field away.
	DefaultStatementCacheCapacity   = 64
	DefaultDescriptionCacheCapacity = 64
)

// DisableCache disables a per-connection cache when used as a capacity.
// Zero means "unset, use the default", so the off switch needs its own value.
const DisableCache = -1

// minDefaultMaxConns is the floor for the derived MaxConns default, so a
// single-core container still gets a usable pool.
const minDefaultMaxConns = 4

// Config holds the configuration for the PostgreSQL pool.
//
// Every duration field is optional and falls back to the corresponding Default
// constant. A negative duration disables that behaviour entirely.
type Config struct {
	// ConnString is the PostgreSQL connection string. Required.
	ConnString string

	// MaxConns is the maximum number of connections the pool will hand out at once.
	// Defaults to max(4, GOMAXPROCS). Acquire blocks once the limit is reached.
	MaxConns int32

	// MinConns is the number of idle connections the pool keeps warm in the
	// background. Defaults to 0 (purely lazy). Must not exceed MaxConns.
	MinConns int32

	// MaxConnIdleTime is how long a connection may sit idle before it is closed.
	// Defaults to DefaultMaxConnIdleTime; negative disables idle expiry.
	//
	// Both this and MaxConnLifetime are judged against a clock cached to
	// clockResolution, so a bound shorter than that is imprecise. Neither is meant
	// for sub-second use, and reading the system clock on every acquire cost more
	// than a quarter of the acquisition path.
	MaxConnIdleTime time.Duration

	// MaxConnLifetime is the total lifetime of a connection, measured from when it
	// was established. Defaults to DefaultMaxConnLifetime; negative disables it.
	// Bounding lifetime is what lets the pool recover from a failover or a DNS change.
	MaxConnLifetime time.Duration

	// HealthCheckPeriod is how often the background reaper closes expired
	// connections and tops the pool back up to MinConns.
	// Defaults to DefaultHealthCheckPeriod; negative disables the reaper.
	HealthCheckPeriod time.Duration

	// ResetQuery is executed when a connection is returned to the pool, typically
	// "DISCARD ALL" for PgBouncer-style session isolation. Empty by default.
	//
	// This costs one extra round trip on every Release, executed while the caller's
	// pool slot is still held, so it roughly halves throughput for short queries.
	// Set it only when connections carry session state that must not leak between
	// callers. A connection whose reset fails is destroyed rather than reused.
	ResetQuery string

	// ResetQueryTimeout bounds the cleanup work done when a connection is returned:
	// ResetQuery, and the rollback of a transaction the caller left open.
	// Defaults to DefaultResetQueryTimeout.
	ResetQueryTimeout time.Duration

	// ConnectTimeout bounds establishing a new connection when the caller's context
	// has no earlier deadline. Defaults to DefaultConnectTimeout.
	ConnectTimeout time.Duration

	// StatementCacheCapacity is how many prepared statements each connection caches.
	// Defaults to DefaultStatementCacheCapacity; DisableCache turns it off, which
	// also selects an execution mode that does not name statements server-side.
	//
	// This is the single largest per-connection memory cost. Raise it for a service
	// with wide query variety; lower it when connection count matters more.
	StatementCacheCapacity int

	// DescriptionCacheCapacity is how many statement descriptions each connection
	// caches. Defaults to DefaultDescriptionCacheCapacity; DisableCache turns it off.
	DescriptionCacheCapacity int

	// BeforeConnect is called with a copy of the parsed connection config before
	// each new connection is established, allowing per-connection adjustments such
	// as credential rotation. Optional.
	BeforeConnect func(*pgx.ConnConfig) error

	// AfterConnect is called on each newly established connection, for example to
	// register custom types or run SET statements. A non-nil error discards the
	// connection. Optional.
	AfterConnect func(*pgx.Conn) error
}

// withDefaults returns a copy of the config with unset fields populated.
func (c Config) withDefaults() Config {
	if c.MaxConns == 0 {
		c.MaxConns = int32(max(minDefaultMaxConns, runtime.GOMAXPROCS(0)))
	}
	if c.MaxConnIdleTime == 0 {
		c.MaxConnIdleTime = DefaultMaxConnIdleTime
	}
	if c.MaxConnLifetime == 0 {
		c.MaxConnLifetime = DefaultMaxConnLifetime
	}
	if c.HealthCheckPeriod == 0 {
		c.HealthCheckPeriod = DefaultHealthCheckPeriod
	}
	if c.ResetQueryTimeout == 0 {
		c.ResetQueryTimeout = DefaultResetQueryTimeout
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.StatementCacheCapacity == 0 {
		c.StatementCacheCapacity = DefaultStatementCacheCapacity
	}
	if c.DescriptionCacheCapacity == 0 {
		c.DescriptionCacheCapacity = DefaultDescriptionCacheCapacity
	}
	return c
}

// cacheCapacity converts a configured capacity into what pgx expects, where zero
// means disabled. It resolves the default itself rather than assuming withDefaults
// has already run, so parse gives the same answer whichever order it is called in.
func cacheCapacity(configured, fallback int) int {
	if configured == 0 {
		return fallback
	}
	return max(0, configured)
}

// validate reports why the config cannot be used, if it cannot.
func (c Config) validate() error {
	if c.ConnString == "" {
		return fmt.Errorf("%w: ConnString is required", ErrInvalidConfig)
	}
	if c.MaxConns < 0 {
		return fmt.Errorf("%w: MaxConns must not be negative, got %d", ErrInvalidConfig, c.MaxConns)
	}
	if c.MinConns < 0 {
		return fmt.Errorf("%w: MinConns must not be negative, got %d", ErrInvalidConfig, c.MinConns)
	}
	if c.MaxConns > 0 && c.MinConns > c.MaxConns {
		return fmt.Errorf("%w: MinConns (%d) must not exceed MaxConns (%d)", ErrInvalidConfig, c.MinConns, c.MaxConns)
	}
	return nil
}

// parse resolves the connection string once, up front. pgx.Connect re-parses on
// every call, which re-reads the environment, any service file, and ~/.pgpass, so
// the pool parses a single template and clones it per connection instead.
func (c Config) parse() (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(c.ConnString)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing ConnString: %w", ErrInvalidConfig, err)
	}

	connConfig.StatementCacheCapacity = cacheCapacity(c.StatementCacheCapacity, DefaultStatementCacheCapacity)
	connConfig.DescriptionCacheCapacity = cacheCapacity(c.DescriptionCacheCapacity, DefaultDescriptionCacheCapacity)

	// A reset query invalidates cached statements, and a disabled cache leaves the
	// default execution mode with nothing to cache into. Either way the connection
	// must not rely on named server-side statements.
	if c.ResetQuery != "" || connConfig.StatementCacheCapacity == 0 {
		connConfig.DefaultQueryExecMode = resetSafeQueryExecMode
	}
	return connConfig, nil
}

// resetSafeQueryExecMode is the execution mode used when a ResetQuery is configured.
//
// pgx defaults to caching named prepared statements on the server. A reset query
// such as DISCARD ALL deallocates them, but the client-side cache does not know
// that and keeps referencing names the server has dropped, so the connection's next
// use fails with SQLSTATE 26000. This mode binds parameters over the extended
// protocol using the unnamed statement, so nothing survives the reset to go stale.
// It is the same guidance pgx gives for running behind PgBouncer.
//
// Parameters are still bound by the server, unlike QueryExecModeSimpleProtocol,
// which interpolates them client-side. BeforeConnect runs after this and can
// override it if you know your reset query preserves prepared statements.
const resetSafeQueryExecMode = pgx.QueryExecModeExec
