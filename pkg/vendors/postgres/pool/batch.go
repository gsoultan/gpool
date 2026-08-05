// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

var _ gpool.Batcher = (*connWrapper)(nil)

// SendBatch pipelines the batch's statements in one round trip.
//
// Unlike CopyFrom there is no pool-level form: the returned reader stays valid
// only while the connection is held, so the caller must own that lifetime.
func (c *connWrapper) SendBatch(ctx context.Context, batch *gpool.Batch) gpool.BatchResults {
	if err := c.live(); err != nil {
		return failedBatchResults{err: err}
	}
	if batch == nil || batch.Len() == 0 {
		return failedBatchResults{err: ErrEmptyBatch}
	}

	pgBatch := &pgx.Batch{}
	for _, query := range batch.Queries() {
		pgBatch.Queue(query.SQL, query.Arguments...)
	}
	return &batchResults{results: c.conn.SendBatch(ctx, pgBatch)}
}
