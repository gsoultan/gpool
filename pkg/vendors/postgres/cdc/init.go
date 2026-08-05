// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// init registers the PostgreSQL CDC subscriber factory. Importing this package is
// what makes gpool.NewSubscriber(gpool.Postgres, ...) resolvable.
//
// RegisterSubscriber only rejects an empty vendor name or a nil factory, neither of
// which is reachable from here, so the error is discarded rather than panicking at
// program start.
func init() {
	_ = gpool.RegisterSubscriber(gpool.Postgres, newFromConfig)
}

// newFromConfig adapts New to the registry's untyped factory signature.
func newFromConfig(config any) (cdc.Subscriber, error) {
	cfg, ok := config.(Config)
	if !ok {
		return nil, fmt.Errorf("%w: expected %T, got %T", ErrInvalidConfig, Config{}, config)
	}
	return New(cfg)
}
