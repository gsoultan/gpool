// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/pooling"
)

// Postgres is a connection pool for PostgreSQL implementing gpool.Pool.
//
// The zero value is not usable; construct it with New. All methods are safe for
// concurrent use, and Close is idempotent.
//
// Capacity, lock-striped idle buckets, the reaper, lifecycle, and statistics come
// from pkg/pooling, which every vendor shares. What lives here is what is actually
// specific to PostgreSQL: how to dial pgx, how to tell a connection is dead, and
// how to return one to a clean state.
type Postgres struct {
	core *pooling.Core[*pgConn]
}

// Compile-time proof that Postgres satisfies the public pool contract.
var _ gpool.Pool = (*Postgres)(nil)

// New creates a PostgreSQL pool. It validates the configuration and resolves the
// connection string up front, but does not dial the database: connections are
// established lazily on Acquire, and in the background up to MinConns.
func New(config Config) (*Postgres, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	config = config.withDefaults()

	connConfig, err := config.parse()
	if err != nil {
		return nil, err
	}

	core, err := pooling.New[*pgConn](&pgxDriver{config: config, connConfig: connConfig}, config.pooling())
	if err != nil {
		return nil, err
	}
	return &Postgres{core: core}, nil
}

// Acquire checks out a connection, blocking until one is available, the context is
// cancelled, or the pool is closed. The caller must Release it.
func (p *Postgres) Acquire(ctx context.Context) (gpool.Conn, error) {
	return p.acquire(ctx)
}

// acquire is the typed form of Acquire used internally, so the pool-level helpers
// do not have to type-assert their own return value.
func (p *Postgres) acquire(ctx context.Context) (*connWrapper, error) {
	handle, err := p.core.Acquire(ctx)
	if err != nil {
		return nil, translate(err)
	}
	return newConnWrapper(handle), nil
}

// Close shuts the pool down. It is idempotent, and it never blocks indefinitely.
func (p *Postgres) Close() {
	p.core.Close()
}

// Stat reports current occupancy and cumulative acquisition counters. It is lock-free.
func (p *Postgres) Stat() gpool.Stat {
	return p.core.Stat()
}

// Exec acquires a connection, runs the statement, and releases the connection.
func (p *Postgres) Exec(ctx context.Context, sql string, args ...any) (gpool.Result, error) {
	conn, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return conn.Exec(ctx, sql, args...)
}

// Query acquires a connection and runs the query. The connection is held until the
// returned Rows are closed, which Rows.All does automatically when iteration ends.
func (p *Postgres) Query(ctx context.Context, sql string, args ...any) (gpool.Rows, error) {
	conn, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := conn.conn().conn.Query(ctx, sql, args...)
	if err != nil {
		conn.Release()
		return nil, err
	}
	return newRows(rows, conn), nil
}

// QueryRow acquires a connection and runs the query. The connection is released by
// the first call to Scan or Release on the returned Row, whichever happens first;
// calling neither leaks the connection for the lifetime of the pool.
func (p *Postgres) QueryRow(ctx context.Context, sql string, args ...any) gpool.Row {
	conn, err := p.acquire(ctx)
	if err != nil {
		return errorRow{err: err}
	}

	rows, err := conn.conn().conn.Query(ctx, sql, args...)
	if err != nil {
		closeRows(rows)
		conn.Release()
		return errorRow{err: err}
	}
	return newRow(rows, conn)
}

// translate maps the engine's sentinel errors onto this package's, so callers
// match on one vocabulary rather than reaching through to the engine.
func translate(err error) error {
	if err == pooling.ErrClosed {
		return ErrPoolClosed
	}
	return err
}
