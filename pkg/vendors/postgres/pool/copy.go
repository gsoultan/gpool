// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

// Both the pool and a checked-out connection can bulk copy.
var (
	_ gpool.BulkCopier = (*Postgres)(nil)
	_ gpool.BulkCopier = (*connWrapper)(nil)
)

// CopyFrom acquires a connection, streams the rows into the destination table
// using the COPY protocol, and releases the connection.
func (p *Postgres) CopyFrom(ctx context.Context, request gpool.CopyRequest) (int64, error) {
	conn, err := p.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	return conn.CopyFrom(ctx, request)
}

// CopyFrom streams the rows into the destination table using the COPY protocol.
func (c *connWrapper) CopyFrom(ctx context.Context, request gpool.CopyRequest) (int64, error) {
	if err := c.live(); err != nil {
		return 0, err
	}
	if err := validateCopyRequest(request); err != nil {
		return 0, err
	}

	// gpool.CopyRows and pgx.CopyFromSource have identical method sets, so the
	// source passes straight through with no adapter and no per-row copying.
	copied, err := c.conn.CopyFrom(ctx, pgx.Identifier(request.Table), request.Columns, request.Rows)
	if err != nil {
		return copied, fmt.Errorf("gpool/postgres: copy into %v: %w", request.Table, err)
	}
	return copied, nil
}

// validateCopyRequest rejects a request that cannot describe a destination,
// rather than letting the driver assemble a nonsensical COPY statement.
func validateCopyRequest(request gpool.CopyRequest) error {
	if len(request.Table) == 0 {
		return fmt.Errorf("%w: CopyRequest.Table is required", ErrInvalidConfig)
	}
	if len(request.Columns) == 0 {
		return fmt.Errorf("%w: CopyRequest.Columns is required", ErrInvalidConfig)
	}
	if request.Rows == nil {
		return fmt.Errorf("%w: CopyRequest.Rows is required", ErrInvalidConfig)
	}
	return nil
}
