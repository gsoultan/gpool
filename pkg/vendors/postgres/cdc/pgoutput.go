// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"encoding/binary"
	"errors"
	"time"
)

// Message types of the pgoutput plugin's protocol version 1, and of the
// replication stream that carries it.
const (
	msgBegin    = 'B'
	msgCommit   = 'C'
	msgRelation = 'R'
	msgInsert   = 'I'
	msgUpdate   = 'U'
	msgDelete   = 'D'

	tupleNew = 'N' // the row as written
	tupleKey = 'K' // the replica identity's columns of the old row
	tupleOld = 'O' // the whole old row, under REPLICA IDENTITY FULL

	xlogData        = 'w'
	primaryKeeplive = 'k'
	standbyStatus   = 'r'
)

// errMalformed is a replication message shorter or stranger than the protocol
// allows. It ends the stream: a decoder that guessed would emit wrong data.
var errMalformed = errors.New("gpool/postgres/cdc: malformed replication message")

// postgresEpoch is the zero of the server's timestamps.
var postgresEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// relation is what a Relation message says about a table: enough to name the
// columns of every later row change for it.
type relation struct {
	namespace string
	name      string
	columns   []string
}

// reader consumes a message body front to back. The first short read records
// errMalformed, and every read after it returns zero values, so a decoder can
// read a whole message and check once.
type reader struct {
	data []byte
	err  error
}

func (r *reader) take(n int) []byte {
	if r.err != nil || n < 0 || len(r.data) < n {
		r.err = errMalformed
		return nil
	}
	taken := r.data[:n]
	r.data = r.data[n:]
	return taken
}

func (r *reader) byte() byte {
	if b := r.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) uint16() uint16 {
	if b := r.take(2); b != nil {
		return binary.BigEndian.Uint16(b)
	}
	return 0
}

func (r *reader) uint32() uint32 {
	if b := r.take(4); b != nil {
		return binary.BigEndian.Uint32(b)
	}
	return 0
}

func (r *reader) uint64() uint64 {
	if b := r.take(8); b != nil {
		return binary.BigEndian.Uint64(b)
	}
	return 0
}

// string reads a NUL-terminated string.
func (r *reader) string() string {
	if r.err != nil {
		return ""
	}
	for i, b := range r.data {
		if b == 0 {
			s := string(r.data[:i])
			r.data = r.data[i+1:]
			return s
		}
	}
	r.err = errMalformed
	return ""
}

// timestamp reads a server timestamp: microseconds since 2000-01-01 UTC.
func (r *reader) timestamp() time.Time {
	return postgresEpoch.Add(time.Duration(int64(r.uint64())) * time.Microsecond)
}

// parseRelation reads a Relation message after its type byte, returning the
// relation's id and description.
func parseRelation(body []byte) (uint32, *relation, error) {
	r := reader{data: body}
	id := r.uint32()
	rel := &relation{namespace: r.string(), name: r.string()}
	r.byte() // replica identity setting
	count := int(r.uint16())
	if r.err == nil && count > len(r.data) {
		return 0, nil, errMalformed
	}
	rel.columns = make([]string, 0, count)
	for range count {
		r.byte() // flags: part of the key
		rel.columns = append(rel.columns, r.string())
		r.uint32() // type OID
		r.uint32() // type modifier
	}
	return id, rel, r.err
}

// parseBegin reads a Begin message after its type byte.
func parseBegin(body []byte) (transaction, error) {
	r := reader{data: body}
	final := r.uint64()
	committed := r.timestamp()
	r.uint32() // xid
	return transaction{position: position(final), committed: committed}, r.err
}
