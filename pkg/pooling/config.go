// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

// Defaults applied by Config.WithDefaults for any field left at its zero value.
const (
	DefaultMaxConnLifetime   = time.Hour
	DefaultMaxConnIdleTime   = 30 * time.Minute
	DefaultHealthCheckPeriod = time.Minute
	DefaultCleanupTimeout    = 5 * time.Second
	DefaultConnectTimeout    = 10 * time.Second
)

// minDefaultMaxConns is the floor for the derived MaxConns default, so a
// single-core container still gets a usable pool.
const minDefaultMaxConns = 4

// ErrInvalidConfig is returned when a configuration cannot be used.
var ErrInvalidConfig = errors.New("gpool/pooling: invalid config")

// Config is the vendor-agnostic part of a pool's configuration. A vendor embeds
// it alongside its own connection settings.
//
// Every duration is optional and falls back to the corresponding Default
// constant. A negative duration disables that behaviour entirely.
type Config struct {
	// MaxConns is the maximum number of connections handed out at once.
	// Defaults to max(4, GOMAXPROCS). Acquire blocks once the limit is reached.
	MaxConns int32

	// MaxConnsLimit is the hard ceiling SetMaxConns may raise MaxConns to.
	// Defaults to MaxConns, which means capacity is fixed unless a limit is set
	// deliberately — a pool that can silently grow is a pool that can exhaust the
	// database, and that is the operator's decision rather than a default.
	//
	// Reserving headroom is free: the permit set is a struct{} channel, whose
	// element has no backing array at any capacity.
	MaxConnsLimit int32

	// MinConns is the number of idle connections kept warm in the background.
	// Defaults to 0 (purely lazy). Must not exceed MaxConns.
	MinConns int32

	// MaxConnIdleTime is how long a connection may sit idle before it is closed.
	// Defaults to DefaultMaxConnIdleTime; negative disables idle expiry.
	//
	// Judged against a clock cached to clockResolution, so a bound shorter than
	// that is imprecise. Reading the system clock on every acquire cost more than
	// a quarter of the acquisition path, and neither bound is meant for
	// sub-second use.
	MaxConnIdleTime time.Duration

	// MaxConnLifetime is the total lifetime of a connection from when it was
	// established. Defaults to DefaultMaxConnLifetime; negative disables it.
	// Bounding lifetime is what lets a pool recover from a failover or a DNS change.
	MaxConnLifetime time.Duration

	// HealthCheckPeriod is how often the background reaper retires expired
	// connections and tops the pool back up to MinConns.
	// Defaults to DefaultHealthCheckPeriod; negative disables the reaper.
	HealthCheckPeriod time.Duration

	// CleanupTimeout bounds the work a vendor does to return a connection to a
	// clean state on release. Defaults to DefaultCleanupTimeout.
	CleanupTimeout time.Duration

	// ConnectTimeout bounds establishing a connection when the caller's context
	// has no earlier deadline. Defaults to DefaultConnectTimeout.
	ConnectTimeout time.Duration
}

// WithDefaults returns a copy with unset fields populated.
func (c Config) WithDefaults() Config {
	if c.MaxConns == 0 {
		c.MaxConns = int32(max(minDefaultMaxConns, runtime.GOMAXPROCS(0)))
	}
	if c.MaxConnsLimit == 0 || c.MaxConnsLimit < c.MaxConns {
		c.MaxConnsLimit = c.MaxConns
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
	if c.CleanupTimeout == 0 {
		c.CleanupTimeout = DefaultCleanupTimeout
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	return c
}

// Validate reports why the config cannot be used, if it cannot.
func (c Config) Validate() error {
	if c.MaxConns < 0 {
		return fmt.Errorf("%w: MaxConns must not be negative, got %d", ErrInvalidConfig, c.MaxConns)
	}
	if c.MinConns < 0 {
		return fmt.Errorf("%w: MinConns must not be negative, got %d", ErrInvalidConfig, c.MinConns)
	}
	if c.MaxConns > 0 && c.MinConns > c.MaxConns {
		return fmt.Errorf("%w: MinConns (%d) must not exceed MaxConns (%d)", ErrInvalidConfig, c.MinConns, c.MaxConns)
	}
	if c.MaxConnsLimit < 0 {
		return fmt.Errorf("%w: MaxConnsLimit must not be negative, got %d", ErrInvalidConfig, c.MaxConnsLimit)
	}
	if c.MaxConnsLimit > 0 && c.MaxConns > 0 && c.MaxConnsLimit < c.MaxConns {
		return fmt.Errorf("%w: MaxConnsLimit (%d) must not be below MaxConns (%d)", ErrInvalidConfig, c.MaxConnsLimit, c.MaxConns)
	}
	return nil
}
