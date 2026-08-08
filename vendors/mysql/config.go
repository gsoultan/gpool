// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Package mysql pools MySQL and MariaDB connections for gpool.
//
// MariaDB speaks the MySQL wire protocol, so one implementation serves both. It
// registers under two vendor names purely so calling code reads honestly about
// which database it is talking to.
//
// This is a separate Go module. A consumer that only uses PostgreSQL never pulls
// in the MySQL driver.
package mysql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	// ErrInvalidConfig is returned by New when the configuration cannot be used.
	ErrInvalidConfig = errors.New("gpool/mysql: invalid config")
)

// Config holds the configuration for a MySQL or MariaDB pool.
//
// The pooling fields mirror the other vendors' so that switching databases does
// not mean rewriting the shape of the configuration.
type Config struct {
	// DSN is the data source name, in go-sql-driver's format. Required.
	//
	//	user:password@tcp(host:3306)/dbname?parseTime=true
	//
	// parseTime=true is worth setting: without it the driver hands back DATE and
	// DATETIME columns as raw bytes rather than time.Time.
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
	// negative disables it.
	//
	// Worth keeping under the server's wait_timeout, which MySQL defaults to 8
	// hours: a connection the server has already dropped is only discovered when
	// a caller tries to use it.
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

	// Parsing up front means a malformed DSN is reported by New rather than by the
	// first caller to acquire a connection, and it is parsed once rather than per
	// connection.
	parsed, err := mysql.ParseDSN(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing DSN: %w", ErrInvalidConfig, err)
	}

	connector, err := mysql.NewConnector(parsed)
	if err != nil {
		return nil, fmt.Errorf("%w: building connector: %w", ErrInvalidConfig, err)
	}
	return connector, nil
}
