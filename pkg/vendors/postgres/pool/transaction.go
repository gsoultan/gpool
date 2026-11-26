// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"

	"github.com/gsoultan/caduceus"
)

// transaction is what pgTx settles and runs statements through: the session
// of the connection it began on, or a scripted one in tests.
type transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, sql string, args []any) (caduceus.CommandTag, error)
	Query(ctx context.Context, sql string, args []any) (resultSet, error)
}
