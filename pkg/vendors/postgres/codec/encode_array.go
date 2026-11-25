// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package codec

import (
	"encoding/binary"
	"reflect"
	"strings"
)

// encodeArray sends a slice as an array: in binary when every element of the
// element type is sent in binary, and as an array literal otherwise. Nested
// slices are a multi-dimensional array.
func encodeArray(dst []byte, oid uint32, v reflect.Value) ([]byte, int16, bool, error) {
	element, ok := arrayElements[oid]
	if ok && binaryElement(element) {
		if out, ok, err := encodeBinaryArray(dst, element, v); ok || err != nil {
			return out, 1, false, err
		}
	}
	var b strings.Builder
	if err := writeArrayLiteral(&b, element, v); err != nil {
		return dst, 0, false, err
	}
	return append(dst, b.String()...), 0, false, nil
}

func binaryElement(oid uint32) bool {
	return binaryScalars[oid] && oid != Bytea
}

// encodeBinaryArray writes the binary array format, reporting false when an
// element would not be binary after all, so the caller falls back to text.
func encodeBinaryArray(dst []byte, element uint32, v reflect.Value) ([]byte, bool, error) {
	dims := arrayDims(v)
	start := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(dims)))
	flags := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, 0)
	dst = binary.BigEndian.AppendUint32(dst, element)
	for _, size := range dims {
		dst = binary.BigEndian.AppendUint32(dst, uint32(size))
		dst = binary.BigEndian.AppendUint32(dst, 1)
	}
	var hasNull bool
	var walk func(reflect.Value, int) bool
	var failure error
	walk = func(v reflect.Value, depth int) bool {
		for i := range v.Len() {
			item := v.Index(i)
			if depth+1 < len(dims) {
				if item.Kind() == reflect.Interface || item.Kind() == reflect.Pointer {
					item = item.Elem()
				}
				if item.Len() != dims[depth+1] {
					failure = errRagged
					return false
				}
				if !walk(item, depth+1) {
					return false
				}
				continue
			}
			mark := len(dst)
			dst = append(dst, 0, 0, 0, 0)
			var format int16
			var null bool
			dst, format, null, failure = Encode(dst, element, item.Interface())
			if failure != nil || (!null && format != 1) {
				return false
			}
			if null {
				hasNull = true
				binary.BigEndian.PutUint32(dst[mark:], 0xFFFFFFFF)
				continue
			}
			binary.BigEndian.PutUint32(dst[mark:], uint32(len(dst)-mark-4))
		}
		return true
	}
	if len(dims) > 0 && !walk(v, 0) {
		return dst[:start], false, failure
	}
	if hasNull {
		binary.BigEndian.PutUint32(dst[flags:], 1)
	}
	return dst, true, nil
}

// arrayDims reads an array's shape from the first element at each depth.
func arrayDims(v reflect.Value) []int {
	var dims []int
	for {
		if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
			v = v.Elem()
		}
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array || (v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 && len(dims) > 0) {
			return dims
		}
		if v.Len() == 0 {
			if len(dims) == 0 {
				return nil
			}
			return append(dims, 0)
		}
		dims = append(dims, v.Len())
		v = v.Index(0)
	}
}

func writeArrayLiteral(b *strings.Builder, element uint32, v reflect.Value) error {
	b.WriteByte('{')
	for i := range v.Len() {
		if i > 0 {
			b.WriteByte(',')
		}
		item := v.Index(i)
		for item.Kind() == reflect.Interface {
			item = item.Elem()
		}
		if item.Kind() == reflect.Slice && item.Type().Elem().Kind() != reflect.Uint8 || item.Kind() == reflect.Array && item.Type() != reflect.TypeOf([16]byte{}) {
			if err := writeArrayLiteral(b, element, item); err != nil {
				return err
			}
			continue
		}
		if !item.IsValid() || (item.Kind() == reflect.Pointer && item.IsNil()) {
			b.WriteString("NULL")
			continue
		}
		text, err := elementText(element, item.Interface())
		if err != nil {
			return err
		}
		b.WriteString(quoteArrayElement(text))
	}
	b.WriteByte('}')
	return nil
}

// elementText encodes one element as text: Encode's text form, or the text
// PostgreSQL would print for what it encodes in binary.
func elementText(element uint32, value any) (string, error) {
	encoded, format, null, err := Encode(nil, element, value)
	if err != nil {
		return "", err
	}
	if null {
		return "NULL", nil
	}
	if format == 0 {
		return string(encoded), nil
	}
	if element == Bytea {
		return `\x` + hexString(encoded), nil
	}
	return asString(element, 1, encoded)
}
