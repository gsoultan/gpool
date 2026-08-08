// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Package cdc streams changes from SQL Server's change data capture for gpool.
//
// SQL Server's model is not the log-tailing one PostgreSQL and MySQL use, and the
// difference shows through the interface rather than being hidden by it. A capture
// job owned by SQL Server Agent reads the transaction log on the server's schedule
// and writes rows into change tables; a consumer then *queries* those tables. So:
//
//   - Changes arrive on the capture job's schedule, not as they commit. Roughly
//     five seconds by default. There is no setting on this side that makes a
//     source configured to capture slowly deliver quickly.
//   - SQL Server Agent must be running. Without it sp_cdc_enable_table succeeds,
//     the change tables are created, and they stay empty forever — a failure that
//     looks exactly like an idle database.
//   - History is bounded by the cleanup job, three days by default, not by any
//     per-consumer position. A position older than that names changes that have
//     been deleted, which is reported rather than silently skipped.
//
// It lives in the pool vendor's module rather than its own because it needs no
// dependency the pool does not already have: one driver serves both.
package cdc

import (
	"fmt"
	"time"
)

// Defaults applied for any field left at its zero value.
const (
	// DefaultPollInterval is how often the change tables are read. It is a floor
	// on delivery latency, and it sits under the capture job's own interval
	// because polling faster than the server writes only wastes round trips.
	DefaultPollInterval = 2 * time.Second

	// DefaultBuffer is how many decoded changes the stream may read ahead of the
	// consumer. One poll can return a large batch, so this is a count of changes
	// rather than of polls.
	DefaultBuffer = 256

	// DefaultQueryTimeout bounds one read of the change tables.
	DefaultQueryTimeout = 30 * time.Second
)

// Config holds the configuration for a SQL Server CDC subscriber.
type Config struct {
	// DSN is the data source name, in go-mssqldb's format. Required. It must
	// name the database the captured tables live in, because change tables are
	// per-database and the capture functions resolve within the current one.
	//
	//	sqlserver://user:pass@host:1433?database=app
	//
	// The account needs membership of the capture instance's gating role, or
	// db_owner. Enabling capture additionally needs db_owner.
	DSN string

	// Tables restricts capture to these tables, as "schema.table". An empty list
	// streams every table that already has capture enabled.
	//
	// Naming a table here does not enable capture on it — that is DDL, and it is
	// AddTables' job, so that a typo produces an error rather than silence.
	Tables []string

	// PollInterval is how often the change tables are read.
	// Defaults to DefaultPollInterval.
	PollInterval time.Duration

	// Buffer is the stream's read-ahead depth in changes. Defaults to DefaultBuffer.
	Buffer int

	// QueryTimeout bounds one read of the change tables.
	// Defaults to DefaultQueryTimeout.
	QueryTimeout time.Duration
}

// withDefaults returns a copy of the config with unset fields populated.
func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.Buffer <= 0 {
		c.Buffer = DefaultBuffer
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = DefaultQueryTimeout
	}
	return c
}

// validate reports why the config cannot be used, if it cannot.
func (c Config) validate() error {
	if c.DSN == "" {
		return fmt.Errorf("%w: DSN is required", ErrInvalidConfig)
	}
	for _, table := range c.Tables {
		if _, _, err := splitQualified(table); err != nil {
			return err
		}
	}
	return nil
}
