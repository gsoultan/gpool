// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"context"
)

// Batcher pipelines a batch of statements in a single round trip.
//
// It is an optional capability, kept out of Conn and Pool so neither grows past
// what every caller needs. Reach it with a type assertion:
//
//	if batcher, ok := conn.(gpool.Batcher); ok {
//	    results := batcher.SendBatch(ctx, batch)
//	    defer results.Close()
//	}
type Batcher interface {
	// SendBatch pipelines the batch and returns a reader for its replies.
	// The returned BatchResults must be closed.
	SendBatch(ctx context.Context, batch *Batch) BatchResults
}
