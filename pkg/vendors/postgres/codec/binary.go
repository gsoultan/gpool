// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package codec

import (
	"encoding/binary"
	"math"
	"time"
)

// postgresEpoch is 2000-01-01 00:00:00 UTC, which every binary date and time
// value counts from, in Unix microseconds.
const postgresEpochMicros = 946684800 * 1_000_000

// Infinite timestamps and dates are the extremes of their integer types.
const (
	infinityMicros = math.MaxInt64
	minusInfMicros = math.MinInt64
	infinityDays   = math.MaxInt32
	minusInfDays   = math.MinInt32
)

func be16(b []byte) int16 { return int16(binary.BigEndian.Uint16(b)) }
func be32(b []byte) int32 { return int32(binary.BigEndian.Uint32(b)) }
func be64(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

// binaryInt decodes any binary integer type by its length.
func binaryInt(oid uint32, src []byte) (int64, bool) {
	switch {
	case oid == Int2 && len(src) == 2:
		return int64(be16(src)), true
	case oid == Int4 && len(src) == 4:
		return int64(be32(src)), true
	case oid == OID && len(src) == 4:
		return int64(uint32(be32(src))), true
	case oid == Int8 && len(src) == 8:
		return be64(src), true
	}
	return 0, false
}

func binaryFloat(oid uint32, src []byte) (float64, bool) {
	switch {
	case oid == Float4 && len(src) == 4:
		return float64(math.Float32frombits(uint32(be32(src)))), true
	case oid == Float8 && len(src) == 8:
		return math.Float64frombits(uint64(be64(src))), true
	}
	return 0, false
}

// microsTime converts PostgreSQL microseconds to a time in loc.
func microsTime(micros int64, loc *time.Location) time.Time {
	unix := micros + postgresEpochMicros
	return time.Unix(unix/1_000_000, (unix%1_000_000)*1000).In(loc)
}

// binaryTime decodes a timestamp, timestamptz or date. A timestamptz comes back
// in the local zone and a timestamp or date in UTC, as pgx returned them.
func binaryTime(oid uint32, src []byte) (time.Time, error) {
	switch {
	case oid == Timestamptz && len(src) == 8, oid == Timestamp && len(src) == 8:
		micros := be64(src)
		if micros == infinityMicros || micros == minusInfMicros {
			return time.Time{}, errInfinite
		}
		loc := time.UTC
		if oid == Timestamptz {
			loc = time.Local
		}
		return microsTime(micros, loc), nil
	case oid == Date && len(src) == 4:
		days := be32(src)
		if days == infinityDays || days == minusInfDays {
			return time.Time{}, errInfinite
		}
		return time.Date(2000, 1, 1+int(days), 0, 0, 0, 0, time.UTC), nil
	}
	return time.Time{}, errFormat(oid, "time.Time")
}

// interval is a binary interval, kept in its three parts because a month has
// no fixed length.
type interval struct {
	micros int64
	days   int32
	months int32
}

func binaryInterval(src []byte) (interval, bool) {
	if len(src) != 16 {
		return interval{}, false
	}
	return interval{micros: be64(src), days: be32(src[8:]), months: be32(src[12:])}, true
}

// duration converts an interval to a time.Duration, counting a day as 24 hours.
// A month has no fixed length, so an interval with months cannot be one.
func (i interval) duration() (time.Duration, error) {
	if i.months != 0 {
		return 0, errMonths
	}
	return time.Duration(i.micros)*time.Microsecond + time.Duration(i.days)*24*time.Hour, nil
}
