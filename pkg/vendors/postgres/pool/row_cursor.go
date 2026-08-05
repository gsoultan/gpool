// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/jackc/pgx/v5"
)

// rowCursor is a view over the row a result set is currently positioned on,
// yielded by pgRows.All. It is a value type holding no ownership, so copying it
// out of the loop gives a stale view rather than a dangling pooled object.
type rowCursor struct {
	rows pgx.Rows
}

var _ gpool.Row = rowCursor{}

// Scan copies the current row into dest.
func (r rowCursor) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Release is a no-op: the cursor owns nothing, and the enclosing iterator closes
// the result set when it ends.
func (r rowCursor) Release() {}
