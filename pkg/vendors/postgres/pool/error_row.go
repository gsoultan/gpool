// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"github.com/gsoultan/gpool/pkg/gpool"
)

// errorRow defers an acquisition or validation failure to Scan, so QueryRow can
// keep its single-return signature without ever handing back a nil Row.
type errorRow struct {
	err error
}

var _ gpool.Row = errorRow{}

// Scan returns the deferred error.
func (r errorRow) Scan(...any) error {
	return r.err
}

// Release is a no-op: no connection was ever acquired.
func (r errorRow) Release() {}
