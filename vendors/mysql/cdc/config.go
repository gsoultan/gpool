// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Package cdc streams changes from a MySQL or MariaDB binary log for gpool.
//
// It is a module of its own, nested inside the MySQL pool vendor rather than
// part of it: reading a binlog pulls in the TiDB parser and thirty-odd other
// modules, and someone who only wants a connection pool should not download
// them.
//
// What it does not provide is as important as what it does. MySQL keeps no
// server-side object for a consumer — no slot, no publication, nothing that
// records where you got to — so this implements cdc.Subscriber and deliberately
// not cdc.ReplicationManager. Two consequences follow, and both lose data if
// they are assumed away:
//
//   - Resuming is the consumer's job. Subscribe starts from the current end of
//     the binlog; only SubscribeFrom, with a position the consumer recorded and
//     persisted, resumes without a gap.
//   - Falling behind loses changes. Binary logs expire on age and size
//     (binlog_expire_logs_seconds) no matter who is still reading. This is the
//     opposite of PostgreSQL, where an unread slot retains WAL until the
//     primary's disk fills.
package cdc

import (
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	driver "github.com/go-sql-driver/mysql"
)

// Defaults applied for any field left at its zero value.
const (
	// DefaultBuffer is how many decoded events the stream may read ahead of the
	// consumer. One binlog row event can carry many rows, so this is a count of
	// changes rather than of binlog events.
	DefaultBuffer = 256

	// DefaultHeartbeatPeriod is how often the server is asked to send a heartbeat
	// on an idle binlog. Without one, a quiet server and a consumer that is
	// merely waiting are indistinguishable from a dead connection.
	DefaultHeartbeatPeriod = 30 * time.Second

	// DefaultReadTimeout bounds a single read from the binlog connection. It must
	// exceed the heartbeat period or an idle stream times out on every cycle.
	DefaultReadTimeout = 90 * time.Second
)

// Config holds the configuration for a MySQL or MariaDB CDC subscriber.
type Config struct {
	// DSN is the data source name in go-sql-driver's format, the same as the
	// pool vendor takes. Required.
	//
	//	repl:password@tcp(host:3306)/dbname
	//
	// The account needs REPLICATION SLAVE to read the binary log, and SELECT on
	// information_schema to resolve column names. It does not need SUPER.
	DSN string

	// ServerID identifies this consumer to the source, which treats it as a
	// replica. Required, and it must be unique across every replica and every
	// other CDC consumer of the same server.
	//
	// There is no default, because a wrong value here is silent and destructive:
	// two consumers sharing an ID cause the source to disconnect one of them,
	// repeatedly and without saying why.
	ServerID uint32

	// Tables restricts capture to these tables, as "schema.table". An empty list
	// captures every table the binary log carries.
	//
	// This filter is applied by the consumer, not by the server. MySQL has no
	// per-consumer subscription to narrow, so the whole binlog crosses the
	// network either way.
	Tables []string

	// Flavor is "mysql" or "mariadb". Defaults to mysql. The two differ in how
	// GTIDs are written, so a MariaDB source read as MySQL fails to resume.
	Flavor string

	// Buffer is the stream's read-ahead depth in events. Defaults to DefaultBuffer.
	Buffer int

	// HeartbeatPeriod is how often an idle source is asked to prove it is alive.
	// Defaults to DefaultHeartbeatPeriod.
	HeartbeatPeriod time.Duration

	// ReadTimeout bounds one read from the binlog connection.
	// Defaults to DefaultReadTimeout.
	ReadTimeout time.Duration
}

// withDefaults returns a copy of the config with unset fields populated.
func (c Config) withDefaults() Config {
	if c.Flavor == "" {
		c.Flavor = mysql.MySQLFlavor
	}
	if c.Buffer <= 0 {
		c.Buffer = DefaultBuffer
	}
	if c.HeartbeatPeriod <= 0 {
		c.HeartbeatPeriod = DefaultHeartbeatPeriod
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = DefaultReadTimeout
	}
	return c
}

// validate reports why the config cannot be used, if it cannot.
func (c Config) validate() error {
	if c.DSN == "" {
		return fmt.Errorf("%w: DSN is required", ErrInvalidConfig)
	}
	if c.ServerID == 0 {
		return fmt.Errorf("%w: ServerID is required and must be unique among the source's replicas", ErrInvalidConfig)
	}
	if c.Flavor != mysql.MySQLFlavor && c.Flavor != mysql.MariaDBFlavor {
		return fmt.Errorf("%w: Flavor %q must be %q or %q", ErrInvalidConfig, c.Flavor, mysql.MySQLFlavor, mysql.MariaDBFlavor)
	}
	if _, err := c.parseDSN(); err != nil {
		return err
	}
	return nil
}

// parseDSN splits the DSN into the parts the binlog syncer needs.
//
// The syncer wants a host, a port and credentials rather than a DSN, so this is
// parsed once at construction: a malformed DSN is then reported by New rather
// than by the first Subscribe.
func (c Config) parseDSN() (*driver.Config, error) {
	parsed, err := driver.ParseDSN(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing DSN: %w", ErrInvalidConfig, err)
	}
	if parsed.Net != "tcp" {
		return nil, fmt.Errorf("%w: replication needs a tcp DSN, got %q", ErrInvalidConfig, parsed.Net)
	}
	return parsed, nil
}
