// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool"
	gpoolcdc "github.com/gsoultan/gpool/pkg/gpool/cdc"
	mssqlpool "github.com/gsoultan/gpool/vendors/mssql"
)

// init registers the subscriber factory. Importing this package is what makes
// gpool.NewSubscriber(mssql.SQLServer, ...) resolvable, in the style of a
// database/sql driver.
//
// The vendor name comes from the pool package rather than being repeated here,
// so the two cannot drift apart and leave a caller with a pool under one name
// and a subscriber under another.
func init() {
	_ = gpool.RegisterSubscriber(mssqlpool.SQLServer, newFromConfig)
}

// newFromConfig adapts New to the registry's untyped factory signature.
func newFromConfig(config any) (gpoolcdc.Subscriber, error) {
	cfg, ok := config.(Config)
	if !ok {
		return nil, fmt.Errorf("%w: expected %T, got %T", ErrInvalidConfig, Config{}, config)
	}
	return New(cfg)
}
