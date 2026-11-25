// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

// lease is the acquisition a transaction began on. A connection belongs to its
// caller until they release it, and to whoever acquires it next from then on,
// so a transaction asks its lease before it touches the connection.
type lease interface {
	// Released reports whether the connection has gone back to the pool.
	Released() bool
}
