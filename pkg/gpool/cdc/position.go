// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

// Position identifies where a change sat in its source's change log.
//
// The format is defined by the vendor and opaque to everyone else: a PostgreSQL
// LSN renders as "0/1A2B3C4D", a MySQL position as a GTID set or a binlog file
// and offset. Persist it verbatim and hand it back to SubscribeFrom to resume.
// Never compare positions from different vendors, and do not assume positions
// from one vendor are ordered lexically — only the vendor that produced them
// knows how to order them.
//
// It is a string rather than a number because a WAL offset is the only change
// log position that fits in one. MySQL's is a set of UUID ranges, MongoDB's is
// an opaque token, SQL Server's is sixteen bytes. Every engine has a canonical
// text form, and the only two things a consumer does with a position — record it
// and hand it back — are things a string does well.
type Position string

// NoPosition asks a stream to start wherever the source's own default is.
//
// What that means differs by vendor and is the difference that loses data if it
// is assumed rather than read: a PostgreSQL slot remembers the consumer's
// position, so this resumes exactly where the last run stopped, while MySQL
// remembers nothing and this starts from the current end of the binlog, silently
// skipping everything written since. Where the source keeps no position of its
// own, persisting Event.Position and calling SubscribeFrom is the only way to
// resume without a gap.
const NoPosition Position = ""
