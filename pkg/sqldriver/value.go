// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// assign copies one driver value into a caller's destination.
//
// database/sql documents driver.Value as a narrow set — nil, int64, float64,
// bool, []byte, string, time.Time — and the type switches below cover it
// directly, which is the common and fast path. But driver.Value is an `any`, and
// a driver with a richer type system than that set will hand back its own native
// types: ClickHouse returns uint8 for a boolean-ish column and uint64 for an
// unsigned integer, so even `SELECT 1` arrives as a uint8. Numeric conversion
// therefore falls back to reflection over the kind, exactly as database/sql does.
//
// Anything implementing sql.Scanner is handed the raw value, so user-defined types
// keep working. A destination this does not cover is reported rather than silently
// mangled.
func assign(dest any, src driver.Value) error {
	// A caller's own Scanner knows better than any conversion here.
	if scanner, ok := dest.(sql.Scanner); ok {
		return scanner.Scan(src)
	}

	switch d := dest.(type) {
	case *any:
		*d = src
		return nil
	case *[]byte:
		return assignBytes(d, src)
	case *string:
		return assignString(d, src)
	case *bool:
		return assignBool(d, src)
	case *time.Time:
		return assignTime(d, src)
	}

	if src == nil {
		return fmt.Errorf("%w: cannot scan NULL into %T; use a pointer type or sql.Null", ErrScan, dest)
	}
	if assignInteger(dest, src) == nil {
		return nil
	}
	return assignFloat(dest, src)
}

func assignBytes(dest *[]byte, src driver.Value) error {
	switch s := src.(type) {
	case nil:
		*dest = nil
	case []byte:
		// The driver owns its buffer and may reuse it after the next Next, so the
		// caller gets a copy rather than a window onto the read buffer.
		*dest = append([]byte(nil), s...)
	case string:
		*dest = []byte(s)
	default:
		*dest = []byte(stringify(src))
	}
	return nil
}

func assignString(dest *string, src driver.Value) error {
	if src == nil {
		return fmt.Errorf("%w: cannot scan NULL into *string; use *sql.NullString", ErrScan)
	}
	*dest = stringify(src)
	return nil
}

func assignBool(dest *bool, src driver.Value) error {
	switch s := src.(type) {
	case bool:
		*dest = s
	case int64:
		// MySQL reports BOOL as TINYINT, so a bool arrives as 0 or 1.
		*dest = s != 0
	case []byte:
		parsed, err := strconv.ParseBool(string(s))
		if err != nil {
			return fmt.Errorf("%w: %q is not a bool", ErrScan, s)
		}
		*dest = parsed
	case string:
		parsed, err := strconv.ParseBool(s)
		if err != nil {
			return fmt.Errorf("%w: %q is not a bool", ErrScan, s)
		}
		*dest = parsed
	default:
		// A driver outside the documented set: ClickHouse reports a boolean
		// column as UInt8, so read it numerically rather than refuse it.
		number, err := toInt64(src)
		if err != nil {
			return fmt.Errorf("%w: cannot scan %T into *bool", ErrScan, src)
		}
		*dest = number != 0
	}
	return nil
}

func assignTime(dest *time.Time, src driver.Value) error {
	switch s := src.(type) {
	case time.Time:
		*dest = s
		return nil
	case []byte:
		return parseTime(dest, string(s))
	case string:
		return parseTime(dest, s)
	default:
		return fmt.Errorf("%w: cannot scan %T into *time.Time", ErrScan, src)
	}
}

// timeLayouts are the shapes a driver may hand back when it does not parse
// timestamps itself, most to least specific.
var timeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999 -0700 MST",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"15:04:05",
}

func parseTime(dest *time.Time, value string) error {
	for _, layout := range timeLayouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			*dest = parsed
			return nil
		}
	}
	return fmt.Errorf("%w: %q is not a recognised timestamp", ErrScan, value)
}

// assignInteger handles every signed and unsigned integer width.
//
// Unsigned destinations are read through toUint64 rather than routed via int64,
// which would lose anything above math.MaxInt64 — a real range for a UInt64
// column in ClickHouse or a BIGINT UNSIGNED in MySQL.
func assignInteger(dest any, src driver.Value) error {
	switch d := dest.(type) {
	case *uint, *uint8, *uint16, *uint32, *uint64:
		value, err := toUint64(src)
		if err != nil {
			return err
		}
		switch u := d.(type) {
		case *uint:
			*u = uint(value)
		case *uint8:
			*u = uint8(value)
		case *uint16:
			*u = uint16(value)
		case *uint32:
			*u = uint32(value)
		case *uint64:
			*u = value
		}
		return nil
	}

	value, err := toInt64(src)
	if err != nil {
		return err
	}

	switch d := dest.(type) {
	case *int:
		*d = int(value)
	case *int8:
		*d = int8(value)
	case *int16:
		*d = int16(value)
	case *int32:
		*d = int32(value)
	case *int64:
		*d = value
	default:
		return fmt.Errorf("%w: %T is not an integer destination", ErrScan, dest)
	}
	return nil
}

func assignFloat(dest any, src driver.Value) error {
	value, err := toFloat64(src)
	if err != nil {
		return err
	}

	switch d := dest.(type) {
	case *float32:
		*d = float32(value)
	case *float64:
		*d = value
	default:
		return fmt.Errorf("%w: cannot scan %T into %T", ErrScan, src, dest)
	}
	return nil
}

func toInt64(src driver.Value) (int64, error) {
	switch s := src.(type) {
	case int64:
		return s, nil
	case float64:
		return int64(s), nil
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	case []byte:
		return strconv.ParseInt(string(s), 10, 64)
	case string:
		return strconv.ParseInt(s, 10, 64)
	}

	// A driver outside the documented set: read it by kind rather than by type.
	value := reflect.ValueOf(src)
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(value.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return int64(value.Float()), nil
	case reflect.Bool:
		if value.Bool() {
			return 1, nil
		}
		return 0, nil
	case reflect.String:
		return strconv.ParseInt(value.String(), 10, 64)
	default:
		return 0, fmt.Errorf("%w: cannot read %T as an integer", ErrScan, src)
	}
}

func toFloat64(src driver.Value) (float64, error) {
	switch s := src.(type) {
	case float64:
		return s, nil
	case int64:
		return float64(s), nil
	case []byte:
		return strconv.ParseFloat(string(s), 64)
	case string:
		return strconv.ParseFloat(s, 64)
	}

	value := reflect.ValueOf(src)
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		return value.Float(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(value.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(value.Uint()), nil
	case reflect.String:
		return strconv.ParseFloat(value.String(), 64)
	default:
		return 0, fmt.Errorf("%w: cannot read %T as a float", ErrScan, src)
	}
}

// toUint64 preserves the full unsigned range, which routing through int64 would
// lose for anything above math.MaxInt64.
func toUint64(src driver.Value) (uint64, error) {
	value := reflect.ValueOf(src)
	if value.Kind() >= reflect.Uint && value.Kind() <= reflect.Uint64 {
		return value.Uint(), nil
	}

	signed, err := toInt64(src)
	if err != nil {
		return 0, err
	}
	return uint64(signed), nil
}

// stringify renders a driver value as text.
func stringify(src driver.Value) string {
	switch s := src.(type) {
	case nil:
		return ""
	case string:
		return s
	case []byte:
		return string(s)
	case int64:
		return strconv.FormatInt(s, 10)
	case float64:
		return strconv.FormatFloat(s, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(s)
	case time.Time:
		return s.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(src)
	}
}

// convertArgs turns caller arguments into the named values a driver expects.
//
// The conversion itself is delegated to the driver where it implements
// NamedValueChecker, and otherwise to database/sql's own exported converter, so
// this does not reimplement outbound conversion at all.
func convertArgs(conn driver.Conn, args []any) ([]driver.NamedValue, error) {
	if len(args) == 0 {
		return nil, nil
	}

	checker, hasChecker := conn.(driver.NamedValueChecker)
	values := make([]driver.NamedValue, len(args))

	for i, arg := range args {
		value := driver.NamedValue{Ordinal: i + 1, Value: arg}

		if named, ok := arg.(sql.NamedArg); ok {
			value.Name = named.Name
			value.Value = named.Value
		}

		if hasChecker {
			switch err := checker.CheckNamedValue(&value); {
			case err == nil:
				values[i] = value
				continue
			case !errorsIsSkip(err):
				return nil, fmt.Errorf("%w: argument %d: %w", ErrConvertArgument, i+1, err)
			}
		}

		converted, err := driver.DefaultParameterConverter.ConvertValue(value.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: argument %d: %w", ErrConvertArgument, i+1, err)
		}
		value.Value = converted
		values[i] = value
	}
	return values, nil
}

// errorsIsSkip reports whether a NamedValueChecker declined to handle a value,
// which means fall through to the default converter rather than fail.
func errorsIsSkip(err error) bool {
	return err == driver.ErrSkip
}
