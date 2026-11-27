// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"

	"github.com/gsoultan/nextar-engine/pkg/gpool"
)

var _ gpool.Batcher = (*connWrapper)(nil)

// SendBatch pipelines the batch's statements: one round trip when every
// statement is already prepared on this connection, two when some are not.
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
	reader, err := c.conn().session.batch(ctx, batch.Queries())
	if err != nil {
		return failedBatchResults{err: err}
	}
	return &batchResults{reader: reader}
}
