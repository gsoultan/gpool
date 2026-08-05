// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// pendingEvent is an event in flight between the reader goroutine and the
// consumer, carrying the WAL offset alongside it.
//
// The consumer receives cdc.Event, whose Position is text. Confirming that
// position to the server is arithmetic — comparing, taking a maximum, writing it
// into a status update — so the reader keeps the number rather than parsing the
// text back out of every event it just formatted.
type pendingEvent struct {
	event cdc.Event
	lsn   uint64
}
