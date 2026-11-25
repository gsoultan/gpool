// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package codec

import (
	"database/sql/driver"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

// Encode appends arg as a parameter of type oid, the type the server resolved
// for it, and reports the format it used. A NULL appends nothing and reports
// null; the caller writes the -1 length.
//
// Integers, floats, booleans, bytea, uuid and the date and time types are
// sent in binary, which the server reads without parsing. Everything else is
// sent as text, which the server parses with the type's own input function, so
// a string is accepted wherever PostgreSQL would accept it as a literal — for a
// bytea that means its hex form, \x0102, as in SQL.
//
// arg may also be a driver.Valuer, an encoding.TextMarshaler, a pointer (nil
// is NULL), a named type of a basic kind, a slice for an array type, and for
// json and jsonb any value encoding/json can marshal.
func Encode(dst []byte, oid uint32, arg any) ([]byte, int16, bool, error) {
	switch v := arg.(type) {
	case nil:
		return dst, 0, true, nil
	case string:
		return append(dst, v...), 0, false, nil
	case []byte:
		return encodeBytes(dst, oid, v)
	case int64:
		return encodeInt(dst, oid, v, arg)
	case int:
		return encodeInt(dst, oid, int64(v), arg)
	case int32:
		return encodeInt(dst, oid, int64(v), arg)
	case int16:
		return encodeInt(dst, oid, int64(v), arg)
	case int8:
		return encodeInt(dst, oid, int64(v), arg)
	case uint8:
		return encodeInt(dst, oid, int64(v), arg)
	case uint16:
		return encodeInt(dst, oid, int64(v), arg)
	case uint32:
		return encodeInt(dst, oid, int64(v), arg)
	case uint64:
		if v > math.MaxInt64 {
			return encodeText(dst, strconv.FormatUint(v, 10))
		}
		return encodeInt(dst, oid, int64(v), arg)
	case uint:
		if uint64(v) > math.MaxInt64 {
			return encodeText(dst, strconv.FormatUint(uint64(v), 10))
		}
		return encodeInt(dst, oid, int64(v), arg)
	case float64:
		return encodeFloat(dst, oid, v)
	case float32:
		return encodeFloat(dst, oid, float64(v))
	case bool:
		return encodeBool(dst, oid, v)
	case time.Time:
		return encodeTime(dst, oid, v)
	case time.Duration:
		return encodeDuration(dst, oid, v)
	case [16]byte:
		if oid == UUID {
			return append(dst, v[:]...), 1, false, nil
		}
		return encodeText(dst, formatUUID(v[:]))
	case json.RawMessage:
		if v == nil {
			return dst, 0, true, nil
		}
		return append(dst, v...), 0, false, nil
	case driver.Valuer:
		return encodeValuer(dst, oid, v)
	case encoding.TextMarshaler:
		if isNilPointer(arg) {
			return dst, 0, true, nil
		}
		text, err := v.MarshalText()
		if err != nil {
			return dst, 0, false, err
		}
		return append(dst, text...), 0, false, nil
	}
	return encodeReflect(dst, oid, arg)
}

func encodeText(dst []byte, text string) ([]byte, int16, bool, error) {
	return append(dst, text...), 0, false, nil
}

func encodeBytes(dst []byte, oid uint32, v []byte) ([]byte, int16, bool, error) {
	if v == nil {
		return dst, 0, true, nil
	}
	switch {
	case oid == Bytea:
		return append(dst, v...), 1, false, nil
	case oid == UUID && len(v) == 16:
		return append(dst, v...), 1, false, nil
	case oid == 0 || oid == Unknown:
		// No type to go by. bytea's hex form is what every bytea accepts.
		dst = append(dst, `\x`...)
		return hex.AppendEncode(dst, v), 0, false, nil
	}
	return append(dst, v...), 0, false, nil
}

func encodeInt(dst []byte, oid uint32, n int64, arg any) ([]byte, int16, bool, error) {
	switch oid {
	case Int8:
		return binary.BigEndian.AppendUint64(dst, uint64(n)), 1, false, nil
	case Int4, OID:
		if (oid == Int4 && (n < math.MinInt32 || n > math.MaxInt32)) || (oid == OID && (n < 0 || n > math.MaxUint32)) {
			return dst, 0, false, fmt.Errorf("%w: %v as type %d", ErrOverflow, arg, oid)
		}
		return binary.BigEndian.AppendUint32(dst, uint32(n)), 1, false, nil
	case Int2:
		if n < math.MinInt16 || n > math.MaxInt16 {
			return dst, 0, false, fmt.Errorf("%w: %v as type %d", ErrOverflow, arg, oid)
		}
		return binary.BigEndian.AppendUint16(dst, uint16(n)), 1, false, nil
	case Float8:
		return binary.BigEndian.AppendUint64(dst, math.Float64bits(float64(n))), 1, false, nil
	case Float4:
		return binary.BigEndian.AppendUint32(dst, math.Float32bits(float32(n))), 1, false, nil
	}
	return strconv.AppendInt(dst, n, 10), 0, false, nil
}

func encodeFloat(dst []byte, oid uint32, f float64) ([]byte, int16, bool, error) {
	switch oid {
	case Float8:
		return binary.BigEndian.AppendUint64(dst, math.Float64bits(f)), 1, false, nil
	case Float4:
		return binary.BigEndian.AppendUint32(dst, math.Float32bits(float32(f))), 1, false, nil
	case Int2, Int4, Int8:
		if f == math.Trunc(f) && f >= math.MinInt64 && f < math.MaxInt64 {
			return encodeInt(dst, oid, int64(f), f)
		}
	}
	return append(dst, formatFloat(f)...), 0, false, nil
}

func encodeBool(dst []byte, oid uint32, b bool) ([]byte, int16, bool, error) {
	if oid == Bool {
		if b {
			return append(dst, 1), 1, false, nil
		}
		return append(dst, 0), 1, false, nil
	}
	return strconv.AppendBool(dst, b), 0, false, nil
}

// encodeTime sends a timestamptz as the instant, and a timestamp or date as the
// wall clock in the time's own zone, which is what writing it as a literal
// would have meant.
func encodeTime(dst []byte, oid uint32, t time.Time) ([]byte, int16, bool, error) {
	switch oid {
	case Timestamptz:
		return binary.BigEndian.AppendUint64(dst, uint64(unixMicros(t)-postgresEpochMicros)), 1, false, nil
	case Timestamp:
		wall := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
		return binary.BigEndian.AppendUint64(dst, uint64(unixMicros(wall)-postgresEpochMicros)), 1, false, nil
	case Date:
		midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		days := (midnight.Unix() - postgresEpochMicros/1_000_000) / 86400
		return binary.BigEndian.AppendUint32(dst, uint32(int32(days))), 1, false, nil
	}
	return t.AppendFormat(dst, "2006-01-02 15:04:05.999999999Z07:00"), 0, false, nil
}

func unixMicros(t time.Time) int64 {
	return t.Unix()*1_000_000 + int64(t.Nanosecond()/1000)
}

func encodeDuration(dst []byte, oid uint32, d time.Duration) ([]byte, int16, bool, error) {
	if oid == Interval {
		dst = binary.BigEndian.AppendUint64(dst, uint64(d.Microseconds()))
		return binary.BigEndian.AppendUint64(dst, 0), 1, false, nil
	}
	if oid == Int8 || oid == Int4 || oid == Int2 {
		return encodeInt(dst, oid, int64(d), d)
	}
	dst = strconv.AppendInt(dst, d.Microseconds(), 10)
	return append(dst, " microseconds"...), 0, false, nil
}

func encodeValuer(dst []byte, oid uint32, valuer driver.Valuer) ([]byte, int16, bool, error) {
	// A nil pointer whose Value method has a value receiver would panic when
	// called, and database/sql treats it as NULL; so does this.
	if isNilPointer(valuer) {
		return dst, 0, true, nil
	}
	value, err := valuer.Value()
	if err != nil {
		return dst, 0, false, err
	}
	if _, again := value.(driver.Valuer); again {
		return dst, 0, false, fmt.Errorf("gpool/postgres/codec: %T.Value returned another Valuer", valuer)
	}
	return Encode(dst, oid, value)
}

func isNilPointer(arg any) bool {
	v := reflect.ValueOf(arg)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

// encodeReflect handles pointers, named basic types, slices for arrays, and
// json values.
func encodeReflect(dst []byte, oid uint32, arg any) ([]byte, int16, bool, error) {
	v := reflect.ValueOf(arg)
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return dst, 0, true, nil
		}
		return Encode(dst, oid, v.Elem().Interface())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return encodeInt(dst, oid, v.Int(), arg)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Encode(dst, oid, v.Uint())
	case reflect.Float32, reflect.Float64:
		return encodeFloat(dst, oid, v.Float())
	case reflect.Bool:
		return encodeBool(dst, oid, v.Bool())
	case reflect.String:
		return Encode(dst, oid, v.String())
	}
	if oid == JSON || oid == JSONB {
		encoded, err := json.Marshal(arg)
		return append(dst, encoded...), 0, false, err
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			return encodeBytes(dst, oid, v.Bytes())
		}
		if v.Kind() == reflect.Slice && v.IsNil() {
			return dst, 0, true, nil
		}
		return encodeArray(dst, oid, v)
	}
	if v.Kind() == reflect.Map || v.Kind() == reflect.Struct {
		encoded, err := json.Marshal(arg)
		return append(dst, encoded...), 0, false, err
	}
	return dst, 0, false, errEncode(oid, arg)
}
