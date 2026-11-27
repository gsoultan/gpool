// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"github.com/gsoultan/caduceus"
	"github.com/gsoultan/nextar-engine/pkg/gpool"
)

// pgResult is the outcome of an Exec, implementing gpool.Result.
// It is an immutable value, so it carries no release hazard of its own.
type pgResult struct {
	tag caduceus.CommandTag
}

var _ gpool.Result = pgResult{}

// RowsAffected returns the number of rows the statement changed: the last
// number in its command tag, as in "INSERT 0 5" or "UPDATE 3".
func (r pgResult) RowsAffected() int64 {
	return r.tag.RowsAffected()
}

// Release is a no-op, kept so callers can treat every result uniformly.
func (r pgResult) Release() {}

// String returns the raw command tag, for example "UPDATE 3".
func (r pgResult) String() string {
	return r.tag.String()
}
