// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package codec

import (
	"database/sql"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

// Encode and Scan are the two halves the server sits between. Each case encodes a
// value for a type, in whatever format Encode picks, and scans those bytes back
// with that format, which is what the server would return for a column of the
// type in that format. The integration suite checks the same against a real
// server; this is the half that runs without one.
func TestEncodeScanRoundTrip(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, 8, 8, 12, 34, 56, 789012000, time.UTC)
	tests := []struct {
		name string
		oid  uint32
		arg  any
		dest any
		want any
	}{
		{"int2", Int2, int16(-7), new(int16), int16(-7)},
		{"int4 from int", Int4, 123456, new(int32), int32(123456)},
		{"int8 extremes", Int8, int64(math.MinInt64), new(int64), int64(math.MinInt64)},
		{"float4", Float4, float32(0.25), new(float32), float32(0.25)},
		{"float8", Float8, math.E, new(float64), math.E},
		{"bool", Bool, true, new(bool), true},
		{"text", Text, "ünïcode", new(string), "ünïcode"},
		{"bytea", Bytea, []byte{0, 0xff, 7}, new([]byte), []byte{0, 0xff, 7}},
		{"timestamptz", Timestamptz, when, new(time.Time), when},
		{"date", Date, time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC), new(time.Time), time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"interval", Interval, 36*time.Hour + time.Microsecond, new(time.Duration), 36*time.Hour + time.Microsecond},
		{"numeric as text", Numeric, "1.50", new(string), "1.50"},
		{"nullable string", Text, "x", new(sql.NullString), sql.NullString{String: "x", Valid: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, format, null, err := Encode(nil, tt.oid, tt.arg)
			if err != nil || null {
				t.Fatalf("Encode() = %v, null %v", err, null)
			}
			// A binary parameter is only valid if the result for the type would
			// come back binary too, or the two halves describe different bytes.
			if format == binaryFormat && ResultFormat(tt.oid) != binaryFormat {
				t.Fatalf("Encode() chose binary for %d, which results return as text", tt.oid)
			}
			if err := Scan(tt.oid, format, encoded, tt.dest); err != nil {
				t.Fatalf("Scan() = %v", err)
			}
			got := reflect.ValueOf(tt.dest).Elem().Interface()
			if g, ok := got.(time.Time); ok {
				if !g.Equal(tt.want.(time.Time)) {
					t.Errorf("got %v, want %v", g, tt.want)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

const binaryFormat = int16(1)

// A value that does not fit its destination must fail rather than wrap: an int8
// of 2^40 scanned into an int32 would otherwise come back as a different number.
func TestScanRefusesOverflow(t *testing.T) {
	t.Parallel()

	encoded, format, _, err := Encode(nil, Int8, int64(1)<<40)
	if err != nil {
		t.Fatal(err)
	}
	var small int32
	if err := Scan(Int8, format, encoded, &small); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Scan(2^40 into int32) = %v, want ErrOverflow", err)
	}
}

// NULL into a destination that cannot represent it is an error, not a zero.
func TestScanNullIntoPlainValue(t *testing.T) {
	t.Parallel()

	var n int
	if err := Scan(Int4, binaryFormat, nil, &n); !errors.Is(err, ErrNull) {
		t.Fatalf("Scan(NULL into *int) = %v, want ErrNull", err)
	}
	var p *int
	if err := Scan(Int4, binaryFormat, nil, &p); err != nil || p != nil {
		t.Fatalf("Scan(NULL into **int) = %v, %v", p, err)
	}
}

// Truncated binary input comes from the network and must be an error, never a
// panic in the caller's goroutine.
func FuzzScan(f *testing.F) {
	f.Add(uint32(Int8), []byte{0, 0, 0, 0, 0, 0, 0, 1})
	f.Add(uint32(Interval), []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0})
	f.Add(uint32(Int4)+1000, []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 23})
	f.Fuzz(func(t *testing.T, oid uint32, src []byte) {
		for _, dest := range []any{new(any), new(string), new(int64), new(time.Time), new([]int64), new([]string)} {
			_ = Scan(oid, binaryFormat, src, dest)
		}
	})
}
