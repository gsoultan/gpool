// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"database/sql/driver"
	"fmt"
	"reflect"
)

// bindable resolves every driver.Valuer among args to its value, so that the
// sql.Null* types and anything else written for database/sql keep working as
// arguments. args is returned as it is, without a copy, when it holds none,
// which is every call that passes plain values.
func bindable(args []any) ([]any, error) {
	first := -1
	for i, arg := range args {
		if _, ok := arg.(driver.Valuer); ok {
			first = i
			break
		}
	}
	if first < 0 {
		return args, nil
	}
	resolved := make([]any, len(args))
	copy(resolved, args)
	for i := first; i < len(resolved); i++ {
		valuer, ok := resolved[i].(driver.Valuer)
		if !ok {
			continue
		}
		value, err := resolveValuer(valuer)
		if err != nil {
			return nil, &ArgumentError{Index: i, Err: err}
		}
		resolved[i] = value
	}
	return resolved, nil
}

// resolveValuer calls Value, treating a nil pointer as NULL as database/sql
// does: a Value method with a value receiver would panic on it.
func resolveValuer(valuer driver.Valuer) (any, error) {
	if v := reflect.ValueOf(valuer); v.Kind() == reflect.Pointer && v.IsNil() {
		return nil, nil
	}
	value, err := valuer.Value()
	if err != nil {
		return nil, err
	}
	if _, again := value.(driver.Valuer); again {
		return nil, fmt.Errorf("%T.Value returned another Valuer", valuer)
	}
	return value, nil
}
