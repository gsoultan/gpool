// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

// Field describes one column of a result, as a RowDescription reports it.
type Field struct {
	Name                 string
	TableOID             uint32
	TableAttributeNumber uint16
	DataTypeOID          uint32
	DataTypeSize         int16
	TypeModifier         int32
	Format               int16
}

// ParseRowDescription reads a RowDescription body, appending to dst so a caller
// can reuse a slice across queries. Names are copied: the body aliases the
// connection's read buffer.
func ParseRowDescription(dst []Field, body []byte) ([]Field, error) {
	if len(body) < 2 {
		return dst, ErrProtocol
	}
	count := int(be16(body))
	body = body[2:]
	for range count {
		name, rest, ok := cutString(body)
		if !ok || len(rest) < 18 {
			return dst, ErrProtocol
		}
		dst = append(dst, Field{
			Name:                 name,
			TableOID:             be32(rest),
			TableAttributeNumber: be16(rest[4:]),
			DataTypeOID:          be32(rest[6:]),
			DataTypeSize:         int16(be16(rest[10:])),
			TypeModifier:         int32(be32(rest[12:])),
			Format:               int16(be16(rest[16:])),
		})
		body = rest[18:]
	}
	return dst, nil
}

// ParseParameterDescription reads a ParameterDescription body into the types
// the server resolved for each parameter.
func ParseParameterDescription(dst []uint32, body []byte) ([]uint32, error) {
	if len(body) < 2 {
		return dst, ErrProtocol
	}
	count := int(be16(body))
	body = body[2:]
	if len(body) < 4*count {
		return dst, ErrProtocol
	}
	for i := range count {
		dst = append(dst, be32(body[4*i:]))
	}
	return dst, nil
}

// SplitDataRow splits a DataRow body into its values, appending to dst. A NULL
// value is a nil slice; an empty one is non-nil and zero-length. The values
// alias body.
func SplitDataRow(dst [][]byte, body []byte) ([][]byte, error) {
	if len(body) < 2 {
		return dst, ErrProtocol
	}
	count := int(be16(body))
	body = body[2:]
	for range count {
		if len(body) < 4 {
			return dst, ErrProtocol
		}
		length := int32(be32(body))
		body = body[4:]
		if length < 0 {
			dst = append(dst, nil)
			continue
		}
		if int(length) > len(body) {
			return dst, ErrProtocol
		}
		dst = append(dst, body[:length:length])
		body = body[length:]
	}
	return dst, nil
}
