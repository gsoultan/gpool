// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

// releaser is what a result set hands its connection back through when it owns
// one: the checked-out connection behind a pool-level Query or QueryRow.
type releaser interface {
	Release()
}
