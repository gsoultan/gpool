// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Package sqldriver pools any database/sql driver.
//
// It is the shared half of every vendor that speaks database/sql — MySQL,
// MariaDB, SQL Server, ClickHouse — so those vendor modules only have to build a
// driver.Connector and register a name. Everything else, including the pooling
// engine, lives here.
//
// Connections are pooled at the driver.Conn level rather than by wrapping
// *sql.DB. Wrapping *sql.DB would mean database/sql does the pooling and none of
// gpool's guarantees would apply: no release gate, no acquisition metrics, no
// shared engine behaviour with the native vendors. Pooling the raw connection is
// what makes gpool the pooler rather than a facade over one.
//
// This package depends only on the standard library, so a vendor module adds its
// own driver and nothing else to a consumer's dependency graph.
package sqldriver

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/pooling"
)

// Pool is a connection pool over a database/sql driver, implementing gpool.Pool.
type Pool struct {
	core *pooling.Core[*conn]
}

var (
	_ gpool.Pool = (*Pool)(nil)
	// Runtime capacity control is implemented once in the engine, so every
	// database/sql vendor gets it rather than it being a PostgreSQL privilege.
	_ gpool.Resizable = (*Pool)(nil)
)

// New creates a pool from a driver.Connector.
func New(config Config) (*Pool, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	core, err := pooling.New[*conn](&adapter{connector: config.Connector}, config.pooling())
	if err != nil {
		return nil, err
	}
	return &Pool{core: core}, nil
}

// Acquire checks out a connection. The caller must Release it.
func (p *Pool) Acquire(ctx context.Context) (gpool.Conn, error) {
	wrapper, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}
	return wrapper, nil
}

// acquire is the typed form used internally, so the pool-level helpers do not
// have to type-assert their own return value.
func (p *Pool) acquire(ctx context.Context) (*connWrapper, error) {
	handle, err := p.core.Acquire(ctx)
	if err != nil {
		return nil, translate(err)
	}
	return newConnWrapper(handle), nil
}

// Close shuts the pool down. It is idempotent.
func (p *Pool) Close() {
	p.core.Close()
}

// Stat reports occupancy and cumulative acquisition counters. It is lock-free.
func (p *Pool) Stat() gpool.Stat {
	return p.core.Stat()
}

// MaxConns returns the ceiling currently in force, which may differ from the
// value the pool was constructed with.
func (p *Pool) MaxConns() int32 {
	return p.core.MaxConns()
}

// SetMaxConns changes how many connections may be handed out at once, within
// [MinConns, MaxConnsLimit]. It does not block: growing takes effect at once,
// and shrinking is applied as checked-out connections come back rather than by
// waiting for them.
func (p *Pool) SetMaxConns(n int32) error {
	return p.core.SetMaxConns(n)
}

// EvictIdle closes every idle connection and reports how many it closed.
// Connections currently checked out are judged when they are released.
func (p *Pool) EvictIdle() int {
	return p.core.EvictIdle()
}

// Exec acquires a connection, runs the statement, and releases the connection.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	conn, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return conn.Exec(ctx, sql, args...)
}

// Query acquires a connection and runs the query. The connection is held until
// the returned Rows are closed, which Rows.All does when iteration ends.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	conn, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := conn.queryOwned(ctx, conn, sql, args)
	if err != nil {
		conn.Release()
		return nil, err
	}
	return rows, nil
}

// QueryRow acquires a connection and runs the query. The connection is released
// by the first call to Scan or Release on the returned Row.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	conn, err := p.acquire(ctx)
	if err != nil {
		return errorRow{err: err}
	}

	rows, err := conn.queryOwned(ctx, conn, sql, args)
	if err != nil {
		conn.Release()
		return errorRow{err: err}
	}
	return newRow(rows.(*pgRows))
}

// translate maps the engine's sentinel errors onto this package's, so callers
// match on one vocabulary rather than reaching through to the engine.
func translate(err error) error {
	if err == pooling.ErrClosed {
		return ErrPoolClosed
	}
	return err
}

// adapter is the pooling.Driver implementation for a database/sql driver.
type adapter struct {
	connector driver.Connector
}

var _ pooling.Driver[*conn] = (*adapter)(nil)

// Connect establishes one new connection.
func (a *adapter) Connect(ctx context.Context) (*conn, error) {
	established, err := a.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("gpool/sqldriver: connect: %w", err)
	}
	return &conn{driver: established}, nil
}

// Close terminates a connection.
func (a *adapter) Close(_ context.Context, c *conn) error {
	return c.driver.Close()
}

// Dead reports whether a connection can no longer carry traffic, without I/O.
func (a *adapter) Dead(c *conn) bool {
	return c == nil || c.driver == nil || c.dead()
}

// NeedsCleanup reports whether Recyclable has work to do, without any I/O.
//
// A driver implementing SessionResetter always does, because database/sql's own
// contract is that ResetSession runs before every reuse. Without one, only an
// abandoned transaction needs unwinding.
func (a *adapter) NeedsCleanup(c *conn) bool {
	if c.tx != nil {
		return true
	}
	_, resets := c.driver.(driver.SessionResetter)
	return resets
}

// Recyclable returns the connection to a clean state for the next caller.
func (a *adapter) Recyclable(ctx context.Context, c *conn) bool {
	return c.reset(ctx)
}
