// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package codec

import (
	"database/sql"
	"encoding"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

// Scan decodes one column into dest. src is nil for NULL; format is the one the
// column arrived in.
//
// The common destinations — pointers to the integer, float, bool and string
// types, []byte, time.Time, time.Duration and any — are decoded without
// reflection and without allocating beyond the value itself. Anything else goes
// through sql.Scanner, encoding.TextUnmarshaler, or reflection: pointers to
// pointers for NULL, named basic types, slices for arrays, and any type at all
// for json and jsonb, which are unmarshalled into it.
func Scan(oid uint32, format int16, src []byte, dest any) error {
	switch d := dest.(type) {
	case *string:
		if src == nil {
			return nullInto(dest)
		}
		s, err := asString(oid, format, src)
		*d = s
		return err
	case *int64:
		n, err := asInt(oid, format, src, dest)
		*d = n
		return err
	case *int:
		n, err := asInt(oid, format, src, dest)
		*d = int(n)
		return err
	case *int32:
		n, err := asInt(oid, format, src, dest)
		if err == nil && (n < math.MinInt32 || n > math.MaxInt32) {
			return ErrOverflow
		}
		*d = int32(n)
		return err
	case *int16:
		n, err := asInt(oid, format, src, dest)
		if err == nil && (n < math.MinInt16 || n > math.MaxInt16) {
			return ErrOverflow
		}
		*d = int16(n)
		return err
	case *float64:
		f, err := asFloat(oid, format, src, dest)
		*d = f
		return err
	case *float32:
		f, err := asFloat(oid, format, src, dest)
		*d = float32(f)
		return err
	case *bool:
		b, err := asBool(oid, format, src, dest)
		*d = b
		return err
	case *[]byte:
		if src == nil {
			*d = nil
			return nil
		}
		b, err := asBytes(oid, format, src)
		*d = b
		return err
	case *time.Time:
		if src == nil {
			return nullInto(dest)
		}
		t, err := asTime(oid, format, src)
		*d = t
		return err
	case *time.Duration:
		if src == nil {
			return nullInto(dest)
		}
		duration, err := asDuration(oid, format, src)
		*d = duration
		return err
	case *any:
		value, err := Natural(oid, format, src)
		*d = value
		return err
	case *json.RawMessage:
		if src == nil {
			*d = nil
			return nil
		}
		b, err := asBytes(oid, format, src)
		*d = b
		return err
	case sql.Scanner:
		value, err := driverValue(oid, format, src)
		if err != nil {
			return err
		}
		return d.Scan(value)
	case encoding.TextUnmarshaler:
		if src == nil {
			return nullInto(dest)
		}
		text, err := asString(oid, format, src)
		if err != nil {
			return err
		}
		return d.UnmarshalText([]byte(text))
	}
	return scanReflect(oid, format, src, dest)
}

func nullInto(dest any) error {
	return fmt.Errorf("%w: %T", ErrNull, dest)
}

// asString returns a column as the text PostgreSQL would print for it, except
// bytea, which is its bytes.
func asString(oid uint32, format int16, src []byte) (string, error) {
	if format == 0 {
		if oid == Bytea {
			b, err := decodeByteaText(src)
			return string(b), err
		}
		return string(src), nil
	}
	if n, ok := binaryInt(oid, src); ok {
		return strconv.FormatInt(n, 10), nil
	}
	if f, ok := binaryFloat(oid, src); ok {
		return formatFloat(f), nil
	}
	switch oid {
	case Bool:
		if len(src) == 1 && src[0] != 0 {
			return "t", nil
		}
		return "f", nil
	case Bytea:
		return string(src), nil
	case UUID:
		if len(src) == 16 {
			return formatUUID(src), nil
		}
	case Interval:
		if i, ok := binaryInterval(src); ok {
			return formatInterval(i), nil
		}
	case JSONB:
		if len(src) > 0 && src[0] == 1 {
			return string(src[1:]), nil
		}
	case Date, Time, Timestamp, Timestamptz:
		return formatTime(oid, src)
	}
	if textual(oid) {
		return string(src), nil
	}
	if _, ok := arrayElements[oid]; ok {
		return arrayText(oid, src)
	}
	return "", errFormat(oid, "string")
}

func asInt(oid uint32, format int16, src []byte, dest any) (int64, error) {
	if src == nil {
		return 0, nullInto(dest)
	}
	if format == 0 {
		return parseInt(src)
	}
	if n, ok := binaryInt(oid, src); ok {
		return n, nil
	}
	if f, ok := binaryFloat(oid, src); ok {
		if f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
			return 0, ErrOverflow
		}
		return int64(f), nil
	}
	return 0, errFormat(oid, fmt.Sprintf("%T", dest))
}

func asFloat(oid uint32, format int16, src []byte, dest any) (float64, error) {
	if src == nil {
		return 0, nullInto(dest)
	}
	if format == 0 {
		return parseFloat(src)
	}
	if f, ok := binaryFloat(oid, src); ok {
		return f, nil
	}
	if n, ok := binaryInt(oid, src); ok {
		return float64(n), nil
	}
	return 0, errFormat(oid, fmt.Sprintf("%T", dest))
}

func asBool(oid uint32, format int16, src []byte, dest any) (bool, error) {
	if src == nil {
		return false, nullInto(dest)
	}
	if format == 0 {
		return parseBool(src)
	}
	if oid == Bool && len(src) == 1 {
		return src[0] != 0, nil
	}
	return false, errFormat(oid, "bool")
}

// asBytes returns a copy: the source aliases the connection's buffer.
func asBytes(oid uint32, format int16, src []byte) ([]byte, error) {
	switch {
	case format == 0 && oid == Bytea:
		return decodeByteaText(src)
	case format == 1 && oid == JSONB && len(src) > 0 && src[0] == 1:
		return append([]byte{}, src[1:]...), nil
	}
	return append([]byte{}, src...), nil
}

func asTime(oid uint32, format int16, src []byte) (time.Time, error) {
	if format == 0 {
		return parseTimestampText(oid, string(src))
	}
	return binaryTime(oid, src)
}

func asDuration(oid uint32, format int16, src []byte) (time.Duration, error) {
	if oid != Interval || format != 1 {
		return 0, errFormat(oid, "time.Duration")
	}
	i, ok := binaryInterval(src)
	if !ok {
		return 0, errFormat(oid, "time.Duration")
	}
	return i.duration()
}

// Natural decodes a column into the Go value that fits it best: int16, int32,
// int64, uint32 (oid), float32, float64, bool, string, []byte, time.Time,
// time.Duration, [16]byte (uuid), []any (arrays), and for json and jsonb
// whatever encoding/json makes of them. Anything else is its text. NULL is nil.
func Natural(oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	if format == 0 {
		return naturalText(oid, src)
	}
	switch oid {
	case Int2:
		n, _ := binaryInt(oid, src)
		return int16(n), nil
	case Int4:
		n, _ := binaryInt(oid, src)
		return int32(n), nil
	case OID:
		n, _ := binaryInt(oid, src)
		return uint32(n), nil
	case Int8:
		n, _ := binaryInt(oid, src)
		return n, nil
	case Float4:
		f, _ := binaryFloat(oid, src)
		return float32(f), nil
	case Float8:
		f, _ := binaryFloat(oid, src)
		return f, nil
	case Bool, Bytea:
		if oid == Bool {
			return asBool(oid, format, src, nil)
		}
		return asBytes(oid, format, src)
	case UUID:
		var u [16]byte
		copy(u[:], src)
		return u, nil
	case Date, Timestamp, Timestamptz:
		t, err := binaryTime(oid, src)
		if err == errInfinite {
			return formatTime(oid, src)
		}
		return t, err
	case Interval:
		i, _ := binaryInterval(src)
		if d, err := i.duration(); err == nil {
			return d, nil
		}
		return formatInterval(i), nil
	case JSONB:
		return unmarshalAny(src[min(1, len(src)):])
	}
	if element, ok := arrayElements[oid]; ok {
		return naturalArray(element, src)
	}
	return asString(oid, format, src)
}

func naturalText(oid uint32, src []byte) (any, error) {
	switch oid {
	case JSON, JSONB:
		return unmarshalAny(src)
	case Bytea:
		return decodeByteaText(src)
	}
	return string(src), nil
}

func unmarshalAny(src []byte) (any, error) {
	var value any
	err := json.Unmarshal(src, &value)
	return value, err
}

// driverValue converts a column to one of the types database/sql gives a
// sql.Scanner: int64, float64, bool, []byte, string, time.Time or nil.
func driverValue(oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	if format == 0 {
		if oid == Bytea {
			return decodeByteaText(src)
		}
		return string(src), nil
	}
	if n, ok := binaryInt(oid, src); ok {
		return n, nil
	}
	if f, ok := binaryFloat(oid, src); ok {
		return f, nil
	}
	switch oid {
	case Bool:
		return asBool(oid, format, src, nil)
	case Bytea:
		return asBytes(oid, format, src)
	case Date, Timestamp, Timestamptz:
		if t, err := binaryTime(oid, src); err == nil {
			return t, nil
		}
	case JSONB:
		return asBytes(oid, format, src)
	}
	return asString(oid, format, src)
}

// scanReflect handles destinations the fast path does not know by name.
func scanReflect(oid uint32, format int16, src []byte, dest any) error {
	target := reflect.ValueOf(dest)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return fmt.Errorf("gpool/postgres/codec: destination %T is not a non-nil pointer", dest)
	}
	value := target.Elem()

	if value.Kind() == reflect.Pointer {
		if src == nil {
			value.SetZero()
			return nil
		}
		fresh := reflect.New(value.Type().Elem())
		if err := Scan(oid, format, src, fresh.Interface()); err != nil {
			return err
		}
		value.Set(fresh)
		return nil
	}
	if oid == JSON || oid == JSONB {
		if src == nil {
			value.SetZero()
			return nil
		}
		raw, err := asBytes(oid, format, src)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, dest)
	}
	if src == nil {
		if value.Kind() == reflect.Slice || value.Kind() == reflect.Map || value.Kind() == reflect.Interface {
			value.SetZero()
			return nil
		}
		return nullInto(dest)
	}
	return scanKind(oid, format, src, value, dest)
}

// scanKind assigns to a named type by its underlying kind: type ID int64, type
// Status string, [16]byte for a uuid, and slices for arrays.
func scanKind(oid uint32, format int16, src []byte, value reflect.Value, dest any) error {
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := asInt(oid, format, src, dest)
		if err != nil {
			return err
		}
		if value.OverflowInt(n) {
			return ErrOverflow
		}
		value.SetInt(n)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := asInt(oid, format, src, dest)
		if err != nil {
			return err
		}
		if n < 0 || value.OverflowUint(uint64(n)) {
			return ErrOverflow
		}
		value.SetUint(uint64(n))
		return nil
	case reflect.Float32, reflect.Float64:
		f, err := asFloat(oid, format, src, dest)
		value.SetFloat(f)
		return err
	case reflect.Bool:
		b, err := asBool(oid, format, src, dest)
		value.SetBool(b)
		return err
	case reflect.String:
		s, err := asString(oid, format, src)
		value.SetString(s)
		return err
	case reflect.Array:
		return scanUUIDArray(oid, format, src, value, dest)
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			b, err := asBytes(oid, format, src)
			value.SetBytes(b)
			return err
		}
		return scanArray(oid, format, src, value)
	}
	return fmt.Errorf("gpool/postgres/codec: cannot scan type %d into %T", oid, dest)
}

func scanUUIDArray(oid uint32, format int16, src []byte, value reflect.Value, dest any) error {
	if value.Len() != 16 || value.Type().Elem().Kind() != reflect.Uint8 || oid != UUID {
		return fmt.Errorf("gpool/postgres/codec: cannot scan type %d into %T", oid, dest)
	}
	var u [16]byte
	if format == 1 && len(src) == 16 {
		copy(u[:], src)
	} else {
		parsed, err := parseUUID(string(src))
		if err != nil {
			return err
		}
		u = parsed
	}
	reflect.Copy(value, reflect.ValueOf(u))
	return nil
}
