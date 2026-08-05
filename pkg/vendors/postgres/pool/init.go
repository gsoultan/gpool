// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// init registers the PostgreSQL pool factory. Importing this package is what makes
// gpool.NewPool(gpool.Postgres, ...) resolvable, in the style of a database/sql driver.
//
// RegisterPool only rejects an empty vendor name or a nil factory, neither of which
// is reachable from here, so the error is discarded rather than panicking at
// program start.
func init() {
	_ = gpool.RegisterPool(gpool.Postgres, newFromConfig)
}

// newFromConfig adapts New to the registry's untyped factory signature.
func newFromConfig(config any) (gpool.Pool, error) {
	cfg, ok := config.(Config)
	if !ok {
		return nil, fmt.Errorf("%w: expected %T, got %T", ErrInvalidConfig, Config{}, config)
	}
	return New(cfg)
}
