// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Package mssql pools Microsoft SQL Server connections for gpool.
//
// This is a separate Go module. A consumer that does not use SQL Server never
// pulls in its driver.
package mssql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/sqldriver"
	mssqldriver "github.com/microsoft/go-mssqldb"
)

// ErrInvalidConfig is returned by New when the configuration cannot be used.
var ErrInvalidConfig = errors.New("gpool/mssql: invalid config")

// SQLServer is the vendor name this package registers.
const SQLServer = gpool.Vendor("sqlserver")

// Config holds the configuration for a SQL Server pool.
//
// The pooling fields mirror the other vendors' so that switching databases does
// not mean rewriting the shape of the configuration.
type Config struct {
	// DSN is the data source name. Required.
	//
	//	sqlserver://user:password@host:1433?database=app
	//
	// The ADO-style form is also accepted:
	//
	//	server=host;user id=user;password=pass;database=app
	DSN string

	// MaxConns is the maximum number of connections handed out at once.
	// Defaults to max(4, GOMAXPROCS).
	MaxConns int32

	// MinConns is the number of idle connections kept warm. Defaults to 0.
	MinConns int32

	// MaxConnsLimit is the hard ceiling SetMaxConns may raise MaxConns to.
	//
	// Defaults to MaxConns, which makes the pool resizable downwards only: growth
	// has to be budgeted for, because a pool that can grow without bound is how a
	// database runs out of connections. Set it to the largest number this pool may
	// ever hold and MaxConns to what it should hold now.
	MaxConnsLimit int32

	// MaxConnIdleTime is how long a connection may sit idle before it is closed.
	// Defaults to 30 minutes; negative disables idle expiry.
	MaxConnIdleTime time.Duration

	// MaxConnLifetime is the total lifetime of a connection. Defaults to an hour;
	// negative disables it. Bounding lifetime is what lets the pool recover from a
	// failover, which matters for an Availability Group where the listener moves.
	MaxConnLifetime time.Duration

	// HealthCheckPeriod is how often the reaper retires expired connections and
	// tops the pool back up to MinConns. Defaults to a minute; negative disables it.
	HealthCheckPeriod time.Duration

	// CleanupTimeout bounds the work done to return a connection to a clean state
	// on release. Defaults to 5 seconds.
	CleanupTimeout time.Duration

	// ConnectTimeout bounds establishing a connection when the caller's context
	// has no earlier deadline. Defaults to 10 seconds.
	ConnectTimeout time.Duration
}

// connector builds the driver connector this configuration describes.
func (c Config) connector() (driver.Connector, error) {
	if c.DSN == "" {
		return nil, fmt.Errorf("%w: DSN is required", ErrInvalidConfig)
	}

	// Built once here rather than per connection, so a malformed DSN is reported
	// by New rather than by the first caller to acquire.
	connector, err := mssqldriver.NewConnector(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing DSN: %w", ErrInvalidConfig, err)
	}
	return connector, nil
}

// New creates a SQL Server pool. It validates the configuration and parses the
// DSN up front, but does not dial: connections are established lazily on Acquire,
// and in the background up to MinConns.
func New(config Config) (gpool.Pool, error) {
	connector, err := config.connector()
	if err != nil {
		return nil, err
	}

	return sqldriver.New(sqldriver.Config{
		Connector:         connector,
		MaxConns:          config.MaxConns,
		MaxConnsLimit:     config.MaxConnsLimit,
		MinConns:          config.MinConns,
		MaxConnIdleTime:   config.MaxConnIdleTime,
		MaxConnLifetime:   config.MaxConnLifetime,
		HealthCheckPeriod: config.HealthCheckPeriod,
		CleanupTimeout:    config.CleanupTimeout,
		ConnectTimeout:    config.ConnectTimeout,
	})
}

// init registers the pool factory. Importing this package is what makes
// gpool.NewPool(mssql.SQLServer, ...) resolvable, in the style of a
// database/sql driver.
//
// RegisterPool only rejects an empty vendor name or a nil factory, neither of
// which is reachable from here, so the error is discarded rather than panicking
// at program start.
func init() {
	_ = gpool.RegisterPool(SQLServer, newFromConfig)
}

// newFromConfig adapts New to the registry's untyped factory signature.
func newFromConfig(config any) (gpool.Pool, error) {
	cfg, ok := config.(Config)
	if !ok {
		return nil, fmt.Errorf("%w: expected %T, got %T", ErrInvalidConfig, Config{}, config)
	}
	return New(cfg)
}
