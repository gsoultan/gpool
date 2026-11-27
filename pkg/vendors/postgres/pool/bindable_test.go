// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

type failingValuer struct{}

func (failingValuer) Value() (driver.Value, error) { return nil, errors.New("no value") }

type valueReceiver struct{ v int64 }

func (r valueReceiver) Value() (driver.Value, error) { return r.v, nil }

// caduceus does not import database/sql, so a driver.Valuer such as
// sql.NullString reaches it only as gpool resolves it. Left unresolved it
// would be encoded by reflection as a struct and refused.
func TestBindableResolvesValuers(t *testing.T) {
	t.Parallel()

	plain := []any{int64(1), "x"}
	if got, err := bindable(plain); err != nil || &got[0] != &plain[0] {
		t.Fatal("arguments without a Valuer must be passed through without a copy")
	}

	var nilPointer *valueReceiver
	args := []any{"keep", sql.NullString{String: "a", Valid: true}, sql.NullInt64{}, nilPointer}
	got, err := bindable(args)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != "keep" || got[1] != "a" || got[2] != nil || got[3] != nil {
		t.Fatalf("bindable() = %#v", got)
	}
	if _, ok := args[1].(sql.NullString); !ok {
		t.Fatal("the caller's slice was modified")
	}

	var argErr *ArgumentError
	if _, err := bindable([]any{1, failingValuer{}}); !errors.As(err, &argErr) || argErr.Index != 1 {
		t.Fatalf("bindable(failing) = %v, want an ArgumentError for $2", err)
	}
}
