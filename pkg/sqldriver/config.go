// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/gsoultan/gpool/pkg/pooling"
)

// Config holds the configuration for a database/sql-backed pool.
//
// The fields mirror the other vendors' rather than embedding pooling.Config, so a
// caller writes Config{MaxConns: 50} instead of nesting a struct literal, and each
// vendor can document its own defaults.
type Config struct {
	// Connector dials the database. A vendor module builds this from its DSN.
	Connector driver.Connector

	// MaxConns is the maximum number of connections handed out at once.
	// Defaults to max(4, GOMAXPROCS).
	MaxConns int32

	// MinConns is the number of idle connections kept warm. Defaults to 0.
	MinConns int32

	// MaxConnIdleTime is how long a connection may sit idle before it is closed.
	// Defaults to 30 minutes; negative disables idle expiry.
	MaxConnIdleTime time.Duration

	// MaxConnLifetime is the total lifetime of a connection. Defaults to an hour;
	// negative disables it. Bounding lifetime is what lets the pool recover from a
	// failover or a DNS change.
	MaxConnLifetime time.Duration

	// HealthCheckPeriod is how often the reaper retires expired connections and
	// tops the pool back up to MinConns. Defaults to a minute; negative disables it.
	HealthCheckPeriod time.Duration

	// CleanupTimeout bounds the work done to return a connection to a clean state
	// on release: unwinding an abandoned transaction, and the driver's own
	// ResetSession. Defaults to 5 seconds.
	CleanupTimeout time.Duration

	// ConnectTimeout bounds establishing a connection when the caller's context
	// has no earlier deadline. Defaults to 10 seconds.
	ConnectTimeout time.Duration
}

func (c Config) validate() error {
	if c.Connector == nil {
		return fmt.Errorf("%w: Connector is required", ErrInvalidConfig)
	}
	if err := c.pooling().Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return nil
}

// pooling projects the vendor-facing config onto the engine's.
func (c Config) pooling() pooling.Config {
	return pooling.Config{
		MaxConns:          c.MaxConns,
		MinConns:          c.MinConns,
		MaxConnIdleTime:   c.MaxConnIdleTime,
		MaxConnLifetime:   c.MaxConnLifetime,
		HealthCheckPeriod: c.HealthCheckPeriod,
		CleanupTimeout:    c.CleanupTimeout,
		ConnectTimeout:    c.ConnectTimeout,
	}
}
