// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package mysql

import (
	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/sqldriver"
)

// New creates a MySQL or MariaDB pool. It validates the configuration and parses
// the DSN up front, but does not dial: connections are established lazily on
// Acquire, and in the background up to MinConns.
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
