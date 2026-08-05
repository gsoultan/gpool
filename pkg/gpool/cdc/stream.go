// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
)

// Stream opens a change stream. It is the minimal interface a consumer needs in
// order to read changes, with no authority to alter the subscription.
type Stream interface {
	// Subscribe starts change data capture and returns a stream of events.
	// Only one stream may be open per subscriber at a time.
	Subscribe(ctx context.Context) (EventStream, error)
}
