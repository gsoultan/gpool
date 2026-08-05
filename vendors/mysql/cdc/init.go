// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	mysqlpool "github.com/gsoultan/gpool/vendors/mysql"
)

// init registers the subscriber factory under both names the pool vendor uses.
// Importing this package is what makes gpool.NewSubscriber(mysql.MySQL, ...)
// resolvable, in the style of a database/sql driver.
//
// The vendor names come from the pool package rather than being repeated here,
// so the two cannot drift apart and leave a caller with a pool under one name
// and a subscriber under another.
//
// RegisterSubscriber only rejects an empty vendor name or a nil factory, neither
// of which is reachable from here, so the errors are discarded rather than
// panicking at program start.
func init() {
	_ = gpool.RegisterSubscriber(mysqlpool.MySQL, newFromConfig)
	_ = gpool.RegisterSubscriber(mysqlpool.MariaDB, newFromConfig)
}

// newFromConfig adapts New to the registry's untyped factory signature.
func newFromConfig(config any) (cdc.Subscriber, error) {
	cfg, ok := config.(Config)
	if !ok {
		return nil, fmt.Errorf("%w: expected %T, got %T", ErrInvalidConfig, Config{}, config)
	}
	return New(cfg)
}
