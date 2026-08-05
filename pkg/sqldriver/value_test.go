// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestAssignIntegers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  driver.Value
		want int64
	}{
		{name: "from int64", src: int64(42), want: 42},
		{name: "from float64", src: float64(42), want: 42},
		{name: "from bytes", src: []byte("42"), want: 42},
		{name: "from string", src: "42", want: 42},
		{name: "from bool", src: true, want: 1},
		{name: "negative", src: int64(-7), want: -7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got int64
			if err := assign(&got, tt.src); err != nil {
				t.Fatalf("assign() = %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}

// Every integer width has to land, not just int64, or a caller scanning into an
// int32 column silently gets a type error at runtime.
func TestAssignEveryIntegerWidth(t *testing.T) {
	t.Parallel()

	var (
		i   int
		i8  int8
		i16 int16
		i32 int32
		i64 int64
		u   uint
		u8  uint8
		u16 uint16
		u32 uint32
		u64 uint64
	)
	dests := []any{&i, &i8, &i16, &i32, &i64, &u, &u8, &u16, &u32, &u64}

	for _, dest := range dests {
		if err := assign(dest, int64(9)); err != nil {
			t.Errorf("assign(%T) = %v", dest, err)
		}
	}

	if i != 9 || i8 != 9 || i16 != 9 || i32 != 9 || i64 != 9 {
		t.Errorf("signed widths did not all take the value")
	}
	if u != 9 || u8 != 9 || u16 != 9 || u32 != 9 || u64 != 9 {
		t.Errorf("unsigned widths did not all take the value")
	}
}

// database/sql documents driver.Value as a narrow set, but the type is an `any`
// and a driver with a richer type system hands back its own native types.
// ClickHouse returns uint8 for a boolean-ish column and uint64 for an unsigned
// integer, so even `SELECT 1` arrives as a uint8 — which used to fail outright.
func TestAssignDriverNativeTypes(t *testing.T) {
	t.Parallel()

	natives := []driver.Value{
		uint8(7), uint16(7), uint32(7), uint64(7), uint(7),
		int8(7), int16(7), int32(7), int(7),
		float32(7),
	}

	for _, src := range natives {
		t.Run(fmt.Sprintf("%T", src), func(t *testing.T) {
			t.Parallel()

			var signed int64
			if err := assign(&signed, src); err != nil || signed != 7 {
				t.Errorf("into int64: %v, %v", signed, err)
			}

			var unsigned uint64
			if err := assign(&unsigned, src); err != nil || unsigned != 7 {
				t.Errorf("into uint64: %v, %v", unsigned, err)
			}

			var ratio float64
			if err := assign(&ratio, src); err != nil || ratio != 7 {
				t.Errorf("into float64: %v, %v", ratio, err)
			}

			var text string
			if err := assign(&text, src); err != nil {
				t.Errorf("into string: %v", err)
			}
		})
	}
}

// A boolean column arriving as a native unsigned type must still land as a bool.
func TestAssignBoolFromNativeUnsigned(t *testing.T) {
	t.Parallel()

	var flag bool
	if err := assign(&flag, uint8(1)); err != nil || !flag {
		t.Errorf("assign(uint8(1)) = %v, %v", flag, err)
	}
	if err := assign(&flag, uint8(0)); err != nil || flag {
		t.Errorf("assign(uint8(0)) = %v, %v", flag, err)
	}
}

// Routing an unsigned value through int64 would lose everything above
// math.MaxInt64, which is a real range for a UInt64 or BIGINT UNSIGNED column.
func TestAssignPreservesFullUnsignedRange(t *testing.T) {
	t.Parallel()

	const huge = uint64(math.MaxUint64) - 1

	var got uint64
	if err := assign(&got, huge); err != nil {
		t.Fatalf("assign() = %v", err)
	}
	if got != huge {
		t.Fatalf("got %d, want %d - the value was truncated through int64", got, huge)
	}
}

func TestAssignFloats(t *testing.T) {
	t.Parallel()

	var f64 float64
	if err := assign(&f64, float64(1.5)); err != nil || f64 != 1.5 {
		t.Errorf("float64 from float64 = %v, %v", f64, err)
	}
	if err := assign(&f64, int64(3)); err != nil || f64 != 3 {
		t.Errorf("float64 from int64 = %v, %v", f64, err)
	}
	if err := assign(&f64, []byte("2.25")); err != nil || f64 != 2.25 {
		t.Errorf("float64 from bytes = %v, %v", f64, err)
	}

	var f32 float32
	if err := assign(&f32, float64(1.5)); err != nil || f32 != 1.5 {
		t.Errorf("float32 = %v, %v", f32, err)
	}
}

func TestAssignStrings(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		src  driver.Value
		want string
	}{
		"from string": {src: "hello", want: "hello"},
		"from bytes":  {src: []byte("hello"), want: "hello"},
		"from int64":  {src: int64(42), want: "42"},
		"from bool":   {src: true, want: "true"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var got string
			if err := assign(&got, tt.src); err != nil {
				t.Fatalf("assign() = %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAssignBool(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		src  driver.Value
		want bool
	}{
		"true":              {src: true, want: true},
		"MySQL TINYINT one": {src: int64(1), want: true},
		"MySQL TINYINT nil": {src: int64(0), want: false},
		"text":              {src: []byte("true"), want: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var got bool
			if err := assign(&got, tt.src); err != nil {
				t.Fatalf("assign() = %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAssignTime(t *testing.T) {
	t.Parallel()

	want := time.Date(2026, 8, 5, 12, 30, 0, 0, time.UTC)

	var got time.Time
	if err := assign(&got, want); err != nil {
		t.Fatalf("assign(time.Time) = %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// A driver that hands back text must still land, which is what parseTime covers.
	for _, text := range []string{"2026-08-05 12:30:00", "2026-08-05T12:30:00Z", "2026-08-05"} {
		var parsed time.Time
		if err := assign(&parsed, text); err != nil {
			t.Errorf("assign(%q) = %v", text, err)
		}
	}

	var bad time.Time
	if err := assign(&bad, "not a timestamp"); !errors.Is(err, ErrScan) {
		t.Errorf("assign(garbage) = %v, want ErrScan", err)
	}
}

// The driver owns its buffer and may reuse it, so a scanned []byte must be a copy.
func TestAssignBytesCopies(t *testing.T) {
	t.Parallel()

	source := []byte("original")

	var got []byte
	if err := assign(&got, source); err != nil {
		t.Fatalf("assign() = %v", err)
	}

	source[0] = 'X'
	if string(got) != "original" {
		t.Fatalf("got %q; the scan aliased the driver's buffer instead of copying", got)
	}
}

func TestAssignNull(t *testing.T) {
	t.Parallel()

	// A NULL into a value type has no sensible answer, and must say so rather
	// than quietly produce a zero.
	var s string
	if err := assign(&s, nil); !errors.Is(err, ErrScan) {
		t.Errorf("assign(nil -> *string) = %v, want ErrScan", err)
	}
	var i int
	if err := assign(&i, nil); !errors.Is(err, ErrScan) {
		t.Errorf("assign(nil -> *int) = %v, want ErrScan", err)
	}

	// The nullable forms do have an answer.
	var bytes []byte
	if err := assign(&bytes, nil); err != nil || bytes != nil {
		t.Errorf("assign(nil -> *[]byte) = %v, %v", bytes, err)
	}
	var anyValue any
	if err := assign(&anyValue, nil); err != nil || anyValue != nil {
		t.Errorf("assign(nil -> *any) = %v, %v", anyValue, err)
	}
}

// A caller's own Scanner knows better than any conversion here, including for NULL.
func TestAssignPrefersScanner(t *testing.T) {
	t.Parallel()

	var nullable sql.NullString
	if err := assign(&nullable, nil); err != nil {
		t.Fatalf("assign(nil -> sql.NullString) = %v", err)
	}
	if nullable.Valid {
		t.Error("a NULL should leave sql.NullString invalid")
	}

	if err := assign(&nullable, "present"); err != nil {
		t.Fatalf("assign(value -> sql.NullString) = %v", err)
	}
	if !nullable.Valid || nullable.String != "present" {
		t.Errorf("got %+v", nullable)
	}

	var nullableInt sql.NullInt64
	if err := assign(&nullableInt, int64(5)); err != nil || nullableInt.Int64 != 5 {
		t.Errorf("sql.NullInt64 = %+v, %v", nullableInt, err)
	}
}

func TestAssignRejectsUnsupportedDestination(t *testing.T) {
	t.Parallel()

	var unsupported struct{ X int }
	if err := assign(&unsupported, int64(1)); !errors.Is(err, ErrScan) {
		t.Fatalf("assign() into an unsupported type = %v, want ErrScan", err)
	}
}

func TestScanIntoChecksArity(t *testing.T) {
	t.Parallel()

	var a, b int
	if err := scanInto([]driver.Value{int64(1)}, []any{&a, &b}); !errors.Is(err, ErrScan) {
		t.Fatalf("scanInto() with too many destinations = %v, want ErrScan", err)
	}
	if err := scanInto([]driver.Value{int64(1), int64(2)}, []any{&a}); !errors.Is(err, ErrScan) {
		t.Fatalf("scanInto() with too few destinations = %v, want ErrScan", err)
	}
}

// Outbound conversion is delegated rather than reimplemented, so this checks the
// wiring: ordinals, named arguments, and the driver's own checker.
func TestConvertArgs(t *testing.T) {
	t.Parallel()

	conn := &fakeDriverConn{owner: &fakeConnector{}}

	values, err := convertArgs(conn, []any{1, "two", true})
	if err != nil {
		t.Fatalf("convertArgs() = %v", err)
	}
	if len(values) != 3 {
		t.Fatalf("got %d values, want 3", len(values))
	}
	for i, value := range values {
		if value.Ordinal != i+1 {
			t.Errorf("value %d has ordinal %d, want %d", i, value.Ordinal, i+1)
		}
	}
	// database/sql's converter widens every integer to int64.
	if values[0].Value != int64(1) {
		t.Errorf("int argument converted to %T(%v), want int64(1)", values[0].Value, values[0].Value)
	}

	named, err := convertArgs(conn, []any{sql.Named("id", 7)})
	if err != nil {
		t.Fatalf("convertArgs(named) = %v", err)
	}
	if named[0].Name != "id" || named[0].Value != int64(7) {
		t.Errorf("named argument = %+v", named[0])
	}

	if got, err := convertArgs(conn, nil); err != nil || got != nil {
		t.Errorf("convertArgs(nil) = %v, %v", got, err)
	}
}

func TestConvertArgsRejectsUnsupported(t *testing.T) {
	t.Parallel()

	conn := &fakeDriverConn{owner: &fakeConnector{}}

	if _, err := convertArgs(conn, []any{struct{ X int }{1}}); !errors.Is(err, ErrConvertArgument) {
		t.Fatalf("convertArgs() with an unsupported argument = %v, want ErrConvertArgument", err)
	}
}
