// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package integration

import (
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	postgrespool "github.com/gsoultan/nextar-engine/pkg/vendors/postgres/pool"
)

// gpool encodes parameters and decodes results itself now, choosing binary or
// text per type from what the server describes. Every case here sends a Go
// value as a parameter, has the server cast it, and scans it back, so both
// directions meet the real server's formats rather than the codec's own idea of
// them. Each runs with the statement cache on and off, because the two paths
// bind differently.
func TestTypesRoundTrip(t *testing.T) {
	when := time.Date(2026, 8, 8, 12, 34, 56, 789012000, time.UTC)
	uuid := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}

	tests := []struct {
		name string
		sql  string
		arg  any
		dest any
		want any
	}{
		{"int2", "SELECT $1::int2", int16(-32768), new(int16), int16(-32768)},
		{"int4", "SELECT $1::int4", int32(math.MaxInt32), new(int32), int32(math.MaxInt32)},
		{"int8", "SELECT $1::int8", int64(math.MinInt64), new(int64), int64(math.MinInt64)},
		{"int into int", "SELECT $1::int8", 42, new(int), 42},
		{"float4", "SELECT $1::float4", float32(1.5), new(float32), float32(1.5)},
		{"float8", "SELECT $1::float8", math.Pi, new(float64), math.Pi},
		{"numeric as string", "SELECT $1::numeric", "12345678901234567890.0123456789", new(string), "12345678901234567890.0123456789"},
		{"numeric into float", "SELECT $1::numeric", 2.5, new(float64), 2.5},
		{"bool", "SELECT $1::bool", true, new(bool), true},
		{"text", "SELECT $1::text", "héllo, wörld", new(string), "héllo, wörld"},
		{"varchar", "SELECT $1::varchar", "abc", new(string), "abc"},
		{"bytea", "SELECT $1::bytea", []byte{0, 1, 2, 0xff}, new([]byte), []byte{0, 1, 2, 0xff}},
		{"timestamptz", "SELECT $1::timestamptz", when, new(time.Time), when},
		{"timestamp", "SELECT $1::timestamp", when, new(time.Time), when},
		{"date", "SELECT $1::date", time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC), new(time.Time), time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)},
		{"interval", "SELECT $1::interval", 90*time.Minute + 1500*time.Microsecond, new(time.Duration), 90*time.Minute + 1500*time.Microsecond},
		{"uuid", "SELECT $1::uuid", uuid, new(string), "12345678-9abc-def0-1234-56789abcdef0"},
		{"uuid text in", "SELECT $1::uuid", "12345678-9abc-def0-1234-56789abcdef0", new(string), "12345678-9abc-def0-1234-56789abcdef0"},
		{"jsonb", "SELECT $1::jsonb", json.RawMessage(`{"a": [1, 2]}`), new(json.RawMessage), json.RawMessage(`{"a": [1, 2]}`)},
		{"int8 array", "SELECT $1::int8[]", []int64{1, -2, 3}, new([]int64), []int64{1, -2, 3}},
		{"text array", "SELECT $1::text[]", []string{"a", "b,c", `d"e`, ""}, new([]string), []string{"a", "b,c", `d"e`, ""}},
		{"null into pointer", "SELECT $1::int4", nil, new(*int32), (*int32)(nil)},
		{"null into NullString", "SELECT $1::text", nil, new(sql.NullString), sql.NullString{}},
		{"value into NullInt64", "SELECT $1::int8", int64(7), new(sql.NullInt64), sql.NullInt64{Int64: 7, Valid: true}},
	}

	for _, mode := range []struct {
		name   string
		config postgrespool.Config
	}{
		{"cached statements", postgrespool.Config{MaxConns: 2}},
		{"no statement cache", postgrespool.Config{MaxConns: 2, StatementCacheCapacity: postgrespool.DisableCache}},
	} {
		pool := newPool(t, mode.config)
		for _, tt := range tests {
			t.Run(mode.name+"/"+tt.name, func(t *testing.T) {
				if err := pool.QueryRow(t.Context(), tt.sql, tt.arg).Scan(tt.dest); err != nil {
					t.Fatalf("Scan() = %v", err)
				}
				got := reflect.ValueOf(tt.dest).Elem().Interface()
				if !equalValue(got, tt.want) {
					t.Errorf("got %#v, want %#v", got, tt.want)
				}
			})
		}
	}
}

func equalValue(got, want any) bool {
	if g, ok := got.(time.Time); ok {
		return g.Equal(want.(time.Time))
	}
	if g, ok := got.(json.RawMessage); ok {
		var a, b any
		return json.Unmarshal(g, &a) == nil && json.Unmarshal(want.(json.RawMessage), &b) == nil && reflect.DeepEqual(a, b)
	}
	return reflect.DeepEqual(got, want)
}

// A NULL scanned into a destination that cannot hold one must fail, not leave
// a zero that reads as a real value.
func TestNullIntoPlainValueFails(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})

	var n int
	err := pool.QueryRow(t.Context(), "SELECT NULL::int4").Scan(&n)
	if err == nil {
		t.Fatal("Scan(NULL into *int) = nil, want an error")
	}
	// The connection must come back usable after a scan error.
	if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("next query = %d, %v", n, err)
	}
}

// A server error must carry its SQLSTATE, which is how callers classify it.
func TestServerErrorCarriesSQLState(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})

	_, err := pool.Exec(t.Context(), "SELECT * FROM no_such_table_anywhere")
	var coded interface{ SQLState() string }
	if !errors.As(err, &coded) || coded.SQLState() != "42P01" {
		t.Fatalf("Exec() error = %v, want SQLSTATE 42P01", err)
	}
}
