// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package codec

import (
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"time"
)

// parseInt reads a decimal integer without converting the bytes to a string.
// A numeric with a zero fraction ("12.000") counts, as pgx allowed.
func parseInt(src []byte) (int64, error) {
	if i := indexByte(src, '.'); i >= 0 {
		for _, c := range src[i+1:] {
			if c != '0' {
				return 0, ErrOverflow
			}
		}
		src = src[:i]
	}
	if len(src) == 0 {
		return 0, strconv.ErrSyntax
	}
	negative := src[0] == '-'
	digits := src
	if negative || src[0] == '+' {
		digits = src[1:]
	}
	if len(digits) == 0 {
		return 0, strconv.ErrSyntax
	}
	var n uint64
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, strconv.ErrSyntax
		}
		if n > (math.MaxUint64-9)/10 {
			return 0, ErrOverflow
		}
		n = n*10 + uint64(c-'0')
	}
	if negative {
		if n > 1<<63 {
			return 0, ErrOverflow
		}
		return -int64(n), nil
	}
	if n > math.MaxInt64 {
		return 0, ErrOverflow
	}
	return int64(n), nil
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

func parseFloat(src []byte) (float64, error) {
	switch string(src) {
	case "Infinity":
		return math.Inf(1), nil
	case "-Infinity":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(string(src), 64)
}

func parseBool(src []byte) (bool, error) {
	switch string(src) {
	case "t", "true", "TRUE", "y", "yes", "on", "1":
		return true, nil
	case "f", "false", "FALSE", "n", "no", "off", "0":
		return false, nil
	}
	return false, strconv.ErrSyntax
}

// decodeByteaText reads bytea's text output, which is hex since PostgreSQL 9.0.
func decodeByteaText(src []byte) ([]byte, error) {
	if len(src) >= 2 && src[0] == '\\' && src[1] == 'x' {
		out := make([]byte, hex.DecodedLen(len(src)-2))
		_, err := hex.Decode(out, src[2:])
		return out, err
	}
	return append([]byte{}, src...), nil
}

func formatFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case math.IsNaN(f):
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// formatTime renders a timestamp or date the way PostgreSQL does under
// DateStyle ISO. A timestamptz is rendered in UTC.
func formatTime(oid uint32, src []byte) (string, error) {
	switch {
	case oid == Date && len(src) == 4:
		days := be32(src)
		if s, ok := infinite(int64(days), infinityDays, minusInfDays); ok {
			return s, nil
		}
		return withEra(time.Date(2000, 1, 1+int(days), 0, 0, 0, 0, time.UTC), "2006-01-02"), nil
	case (oid == Timestamp || oid == Timestamptz) && len(src) == 8:
		micros := be64(src)
		if s, ok := infinite(micros, infinityMicros, minusInfMicros); ok {
			return s, nil
		}
		text := withEra(microsTime(micros, time.UTC), "2006-01-02 15:04:05.999999")
		if oid == Timestamptz {
			text = insertZone(text)
		}
		return text, nil
	case oid == Time && len(src) == 8:
		return time.Unix(0, be64(src)*1000).UTC().Format("15:04:05.999999"), nil
	}
	return "", errFormat(oid, "string")
}

func infinite(value, positive, negative int64) (string, bool) {
	switch value {
	case positive:
		return "infinity", true
	case negative:
		return "-infinity", true
	}
	return "", false
}

// withEra formats t, writing a year before 1 AD as PostgreSQL does: the year
// counted backwards and a BC suffix.
func withEra(t time.Time, layout string) string {
	if t.Year() > 0 {
		return t.Format(layout)
	}
	shifted := t.AddDate(1-2*t.Year(), 0, 0)
	return shifted.Format(layout) + " BC"
}

func insertZone(text string) string {
	if strings.HasSuffix(text, " BC") {
		return strings.TrimSuffix(text, " BC") + "+00 BC"
	}
	return text + "+00"
}

// formatInterval renders an interval in PostgreSQL's default IntervalStyle.
func formatInterval(i interval) string {
	var b strings.Builder
	part := func(n int64, unit string) {
		if n == 0 {
			return
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(strconv.FormatInt(n, 10))
		b.WriteByte(' ')
		b.WriteString(unit)
		if n != 1 && n != -1 {
			b.WriteByte('s')
		}
	}
	years, months := int64(i.months/12), int64(i.months%12)
	part(years, "year")
	if months != 0 {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(strconv.FormatInt(months, 10))
		if months == 1 || months == -1 {
			b.WriteString(" mon")
		} else {
			b.WriteString(" mons")
		}
	}
	part(int64(i.days), "day")
	if i.micros != 0 || b.Len() == 0 {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		micros := i.micros
		if micros < 0 {
			b.WriteByte('-')
			micros = -micros
		}
		hours, rest := micros/3_600_000_000, micros%3_600_000_000
		b.WriteString(pad2(hours))
		b.WriteByte(':')
		b.WriteString(pad2(rest / 60_000_000))
		b.WriteByte(':')
		b.WriteString(pad2(rest % 60_000_000 / 1_000_000))
		if fraction := rest % 1_000_000; fraction != 0 {
			b.WriteString(strings.TrimRight("."+strconv.FormatInt(1_000_000+fraction, 10)[1:], "0"))
		}
	}
	return b.String()
}

func pad2(n int64) string {
	if n < 10 {
		return "0" + strconv.FormatInt(n, 10)
	}
	return strconv.FormatInt(n, 10)
}

func formatUUID(src []byte) string {
	var out [36]byte
	hex.Encode(out[0:8], src[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], src[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], src[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], src[8:10])
	out[23] = '-'
	hex.Encode(out[24:], src[10:16])
	return string(out[:])
}

// parseUUID reads a UUID in any of the forms PostgreSQL accepts: with or
// without hyphens and braces.
func parseUUID(text string) ([16]byte, error) {
	var out [16]byte
	clean := strings.NewReplacer("-", "", "{", "", "}", "").Replace(text)
	if len(clean) != 32 {
		return out, strconv.ErrSyntax
	}
	_, err := hex.Decode(out[:], []byte(clean))
	return out, err
}

// parseTimestampText reads a timestamp in DateStyle ISO, which is what the
// server sends unless a session changed it.
func parseTimestampText(oid uint32, src string) (time.Time, error) {
	switch src {
	case "infinity", "-infinity":
		return time.Time{}, errInfinite
	}
	bc := strings.HasSuffix(src, " BC")
	src = strings.TrimSuffix(src, " BC")
	var t time.Time
	var err error
	switch oid {
	case Date:
		t, err = time.Parse("2006-01-02", src)
	case Timestamptz:
		t, err = parseWithZone(src)
		if err == nil {
			t = t.In(time.Local)
		}
	default:
		t, err = time.Parse("2006-01-02 15:04:05.999999999", src)
	}
	if err == nil && bc {
		t = t.AddDate(1-2*t.Year(), 0, 0)
	}
	return t, err
}

func parseWithZone(src string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999-07", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999-07:00:00"} {
		if t, err := time.Parse(layout, src); err == nil {
			return t, nil
		}
	}
	return time.Time{}, strconv.ErrSyntax
}

func hexString(b []byte) string { return hex.EncodeToString(b) }
