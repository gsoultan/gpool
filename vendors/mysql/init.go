// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package mysql

import (
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool"
)

// Vendor names this package registers.
//
// MariaDB is the same implementation: it speaks the MySQL wire protocol, and
// go-sql-driver serves both. The second name exists so calling code says which
// database it actually targets rather than claiming MySQL for a MariaDB cluster.
const (
	MySQL   = gpool.Vendor("mysql")
	MariaDB = gpool.Vendor("mariadb")
)

// init registers the pool factory under both names. Importing this package is
// what makes gpool.NewPool(mysql.MySQL, ...) resolvable, in the style of a
// database/sql driver.
//
// RegisterPool only rejects an empty vendor name or a nil factory, neither of
// which is reachable from here, so the errors are discarded rather than
// panicking at program start.
func init() {
	_ = gpool.RegisterPool(MySQL, newFromConfig)
	_ = gpool.RegisterPool(MariaDB, newFromConfig)
}

// newFromConfig adapts New to the registry's untyped factory signature.
func newFromConfig(config any) (gpool.Pool, error) {
	cfg, ok := config.(Config)
	if !ok {
		return nil, fmt.Errorf("%w: expected %T, got %T", ErrInvalidConfig, Config{}, config)
	}
	return New(cfg)
}
