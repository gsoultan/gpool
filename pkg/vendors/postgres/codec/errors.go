// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package codec

import (
	"errors"
	"fmt"
)

var (
	// ErrNull is returned when a NULL is scanned into a variable that cannot
	// hold one. Scan into a pointer, an any, a []byte or a sql.Null type to
	// receive NULL.
	ErrNull = errors.New("gpool/postgres/codec: cannot scan NULL into a non-nullable destination")

	// ErrOverflow is returned when a value does not fit the variable.
	ErrOverflow = errors.New("gpool/postgres/codec: value out of range for destination")

	errInfinite = errors.New("gpool/postgres/codec: infinite date or timestamp cannot be a time.Time")
	errMonths   = errors.New("gpool/postgres/codec: an interval with months cannot be a time.Duration")
)

func errFormat(oid uint32, dest string) error {
	return fmt.Errorf("gpool/postgres/codec: cannot scan type %d into %s", oid, dest)
}

func errEncode(oid uint32, value any) error {
	return fmt.Errorf("gpool/postgres/codec: cannot encode %T as type %d", value, oid)
}

var errRagged = errors.New("gpool/postgres/codec: a multi-dimensional array must not be ragged")
