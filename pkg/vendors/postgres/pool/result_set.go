// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import "github.com/gsoultan/caduceus/wire"

// resultSet is what Rows and Row are built over: a cursor on a live
// connection, or a scripted one in tests.
type resultSet interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
	Fields() []wire.Field
	RawValues() [][]byte
	Tag() string
}
