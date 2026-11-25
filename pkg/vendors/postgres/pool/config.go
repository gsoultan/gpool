// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/gsoultan/caduceus/wire"
	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/pooling"
)

// Defaults applied by Config.withDefaults for any field left at its zero value.
const (
	DefaultMaxConnLifetime   = time.Hour
	DefaultMaxConnIdleTime   = 30 * time.Minute
	DefaultHealthCheckPeriod = time.Minute
	DefaultResetQueryTimeout = 5 * time.Second
	DefaultConnectTimeout    = 10 * time.Second

	// DefaultStatementCacheCapacity bounds the prepared statements each connection
	// keeps on the server. 64 covers the query variety of a typical service, and
	// raising it is one field away. The cache's memory grows with use rather than
	// being reserved up front, so an idle connection costs nothing for it.
	//
	// DefaultDescriptionCacheCapacity is kept for compatibility. Statement
	// descriptions are held by the statement cache, so there is no second cache
	// to bound.
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

	// MaxConnsLimit is the hard ceiling SetMaxConns may raise MaxConns to.
	//
	// Defaults to MaxConns, which makes the pool resizable downwards only: growth
	// has to be budgeted for, because a pool that can grow without bound is how a
	// database runs out of connections. Set it to the largest number this pool may
	// ever hold and MaxConns to what it should hold now.
	MaxConnsLimit int32

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

	// DescriptionCacheCapacity is accepted and validated for compatibility, and
	// otherwise unused: descriptions live with the statements they describe.
	DescriptionCacheCapacity int

	// BeforeConnect is called with a copy of the parsed connection config before
	// each new connection is established, allowing per-connection adjustments such
	// as credential rotation. Optional.
	BeforeConnect func(*wire.Config) error

	// AfterConnect is called on each newly established connection, for example to
	// run SET statements. The connection is not pooled yet, and its Release does
	// nothing. A non-nil error discards the connection. Optional.
	AfterConnect func(context.Context, gpool.Conn) error
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

// cacheCapacity resolves a configured capacity, where zero means the default
// and DisableCache means none. It resolves the default itself rather than
// assuming withDefaults has already run, so parse gives the same answer whichever
// order it is called in.
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

// pooling projects the vendor-facing config onto the engine's.
func (c Config) pooling() pooling.Config {
	return pooling.Config{
		MaxConns:          c.MaxConns,
		MinConns:          c.MinConns,
		MaxConnsLimit:     c.MaxConnsLimit,
		MaxConnIdleTime:   c.MaxConnIdleTime,
		MaxConnLifetime:   c.MaxConnLifetime,
		HealthCheckPeriod: c.HealthCheckPeriod,
		CleanupTimeout:    c.ResetQueryTimeout,
		ConnectTimeout:    c.ConnectTimeout,
	}
}

// settings is a Config resolved for dialling: the parsed connection string and
// how many statements each connection may keep prepared.
type settings struct {
	conn       *wire.Config
	statements int
}

// parse resolves the connection string once, up front, so that opening a
// connection re-reads no environment and no password file.
//
// A reset query such as DISCARD ALL deallocates prepared statements behind the
// cache's back, and a disabled cache has nowhere to keep them, so either way
// each query is parsed into the unnamed statement instead. Parameters are still
// bound by the server.
func (c Config) parse() (*settings, error) {
	connConfig, err := wire.ParseConfig(c.ConnString)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing ConnString: %w", ErrInvalidConfig, err)
	}
	statements := cacheCapacity(c.StatementCacheCapacity, DefaultStatementCacheCapacity)
	if c.ResetQuery != "" {
		statements = 0
	}
	return &settings{conn: connConfig, statements: statements}, nil
}
