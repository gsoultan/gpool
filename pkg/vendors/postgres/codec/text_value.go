// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package codec

// EncodeText appends arg as text for a column of type oid, the form COPY's text
// format and array literals want. It reports null for NULL.
func EncodeText(dst []byte, oid uint32, arg any) ([]byte, bool, error) {
	start := len(dst)
	encoded, format, null, err := Encode(dst, oid, arg)
	if err != nil || null || format == 0 {
		return encoded, null, err
	}
	raw := append([]byte(nil), encoded[start:]...)
	dst = encoded[:start]
	if oid == Bytea {
		return append(append(dst, `\x`...), hexString(raw)...), false, nil
	}
	text, err := asString(oid, 1, raw)
	return append(dst, text...), false, err
}
