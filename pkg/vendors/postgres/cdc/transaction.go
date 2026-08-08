// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"time"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// transaction is what the Begin message told us about the transaction currently
// being decoded.
//
// pgoutput sends this once, ahead of the changes, and every change in the
// transaction carries it. Keeping the two fields together is what stops one of
// them being threaded through the decoder and the other quietly forgotten.
type transaction struct {
	// position is the transaction's commit LSN, which is the same for every
	// change in it and different for every other transaction. Event.Position, by
	// contrast, is per record.
	position  cdc.Position
	committed time.Time
}
