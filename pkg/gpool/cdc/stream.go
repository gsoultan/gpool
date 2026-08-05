// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
)

// Stream opens a change stream. It is the minimal interface a consumer needs in
// order to read changes, with no authority to alter the subscription.
type Stream interface {
	// Subscribe starts change data capture from the source's own default
	// position, which is NoPosition's meaning for that vendor. Only one stream
	// may be open per subscriber at a time.
	//
	// Read NoPosition before relying on this. Against PostgreSQL it resumes from
	// the slot and loses nothing; against a source that keeps no position of its
	// own it starts from the current end of the log, and changes written since
	// the last run are skipped rather than replayed.
	Subscribe(ctx context.Context) (EventStream, error)

	// SubscribeFrom starts change data capture immediately after a position the
	// consumer recorded earlier, and is the only way to resume without a gap
	// against a source that stores no position for its consumers.
	//
	// The position must have come from an Event of this same vendor. Passing
	// NoPosition is equivalent to Subscribe.
	SubscribeFrom(ctx context.Context, after Position) (EventStream, error)
}
