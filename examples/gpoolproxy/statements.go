// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bytes"
	"encoding/binary"
)

// Frontend messages that name a prepared statement.
const (
	msgParse    = 'P'
	msgBind     = 'B'
	msgDescribe = 'D'
	msgClose    = 'C'

	// msgParseComplete is the backend's answer to a Parse. A Parse this proxy
	// injected produces one the client never asked for, and forwarding it would
	// put an extra message into a sequence the client is counting.
	msgParseComplete = '1'

	// describeStatement distinguishes Describe of a statement from Describe of a
	// portal, which names no statement.
	describeStatement = 'S'
)

// statements remembers the prepared statements one client has created, up to a
// limit, discarding the least recently used to stay under it.
//
// Transaction-mode pooling moves a client between backends, and a named prepared
// statement lives on the backend that parsed it. Without this, a client that
// prepares once and binds many times fails the moment it lands somewhere new —
// which is what PgBouncer answered in 1.21, and what the README previously named
// as the gap between this example and something production-worthy.
//
// What is remembered is the whole Parse message, byte for byte, so replaying it
// needs no re-encoding and cannot disagree with what the client actually sent.
//
// The limit is what keeps a set the client controls from being unbounded. Both
// users of this type are filled by client-supplied names: a session's set grows
// with what one client prepares, and a backend's with what every client that ever
// held it prepared. Neither shrinks on its own, because nothing deallocates a
// prepared statement when a client goes away.
type statements struct {
	byName map[string]*statement

	// limit is the most that may be held at once. Zero or less is unlimited.
	limit int

	// uses orders the entries by recency. A counter rather than a timestamp
	// because only the order matters, and the coarse clock this proxy runs on
	// would tie every entry within its resolution.
	uses uint64
}

type statement struct {
	message []byte
	used    uint64
}

func newStatements(limit int) *statements {
	return &statements{limit: limit}
}

// remember records a Parse so it can be replayed onto another backend, and names
// whatever had to be discarded to make room.
//
// The unnamed statement is deliberately not remembered. It lives only until the
// next Parse, every client overwrites it constantly, and replaying a stale one
// would bind arguments to the wrong SQL.
func (s *statements) remember(name string, message []byte) (evicted string, didEvict bool) {
	evicted, _, didEvict = s.displace(name, message)
	return evicted, didEvict
}

// displace is remember, and also returns the Parse of whatever was discarded,
// so that a discard the server turns out not to have made can be undone.
func (s *statements) displace(name string, message []byte) (evicted string, evictedMessage []byte, didEvict bool) {
	if name == "" {
		return "", nil, false
	}
	s.uses++

	// Re-preparing under a name already held replaces it. Counting that as a new
	// entry would spend the limit on a client that only ever holds one statement.
	if held, ok := s.byName[name]; ok {
		held.message = bytes.Clone(message)
		held.used = s.uses
		return "", nil, false
	}

	if s.limit > 0 && len(s.byName) >= s.limit {
		evicted, evictedMessage, didEvict = s.evict()
	}
	// Allocated on first use: most sessions never name a statement, and the map
	// would otherwise be the largest thing an idle client holds.
	if s.byName == nil {
		s.byName = make(map[string]*statement)
	}
	s.byName[name] = &statement{message: bytes.Clone(message), used: s.uses}
	return evicted, evictedMessage, didEvict
}

// evict discards the least recently used statement and names it.
//
// Found by a scan rather than kept in a list: the limit is a few hundred, a scan
// happens only once a client is already at it, and a few hundred comparisons are
// nothing beside the round trip the Parse that triggered it is about to make. The
// cost an adversarial client can impose is therefore a constant per message, not
// a growing one.
func (s *statements) evict() (string, []byte, bool) {
	var oldest string
	var oldestUse uint64
	first := true

	for name, held := range s.byName {
		if first || held.used < oldestUse {
			oldest, oldestUse, first = name, held.used, false
		}
	}
	if first {
		return "", nil, false
	}
	message := s.byName[oldest].message
	delete(s.byName, oldest)
	return oldest, message, true
}

func (s *statements) forget(name string) {
	delete(s.byName, name)
}

// peek returns a remembered Parse without counting it as a use.
func (s *statements) peek(name string) ([]byte, bool) {
	held, ok := s.byName[name]
	if !ok {
		return nil, false
	}
	return held.message, true
}

// restore puts back a Parse that a change the server skipped had taken out or
// replaced. It takes the message as it is, already a copy of its own, and it
// may leave the set one over its limit until the next statement is remembered.
func (s *statements) restore(name string, message []byte) {
	if s.byName == nil {
		s.byName = make(map[string]*statement)
	}
	s.uses++
	s.byName[name] = &statement{message: message, used: s.uses}
}

// lookup returns a remembered Parse and marks it as the most recently used, which
// is what keeps a client's working set from being evicted out from under it.
func (s *statements) lookup(name string) ([]byte, bool) {
	held, ok := s.byName[name]
	if !ok {
		return nil, false
	}
	s.uses++
	held.used = s.uses
	return held.message, true
}

// find is lookup for a name still sitting in a message buffer. Converting it to
// a string to ask would allocate, and this is asked once for every Bind — which
// is to say once for every query a client with a statement cache runs.
func (s *statements) find(name []byte) ([]byte, bool) {
	held, ok := s.byName[string(name)]
	if !ok {
		return nil, false
	}
	s.uses++
	held.used = s.uses
	return held.message, true
}

func (s *statements) len() int {
	return len(s.byName)
}

// parseName reads the statement name a Parse message declares, which is the
// first null-terminated string in its body.
func parseName(body []byte) (string, bool) {
	return cString(body)
}

// cString reads a null-terminated string from the front of a buffer.
func cString(body []byte) (string, bool) {
	value, _, ok := cutBytes(body)
	return value, ok
}

// cutBytes splits a null-terminated string from the front of a buffer.
func cutBytes(body []byte) (value string, rest []byte, ok bool) {
	end := bytes.IndexByte(body, 0)
	if end < 0 {
		return "", body, false
	}
	return string(body[:end]), body[end+1:], true
}

// namesStatement reports whether a message refers to a prepared statement by
// name, and is therefore one this proxy has to look at rather than relay blind.
func namesStatement(kind byte) bool {
	switch kind {
	case msgParse, msgBind, msgDescribe, msgClose:
		return true
	}
	return false
}

// msgCloseComplete is the backend's answer to a Close.
const msgCloseComplete = '3'

// closeStatement builds a Close for a named prepared statement.
//
// Encoded by hand rather than through a protocol library: it is eleven bytes of
// a fixed shape, and the proxy otherwise has no reason to depend on an encoder.
func closeStatement(name string) []byte {
	body := make([]byte, 0, len(name)+2)
	body = append(body, describeStatement)
	body = append(body, name...)
	body = append(body, 0)

	message := make([]byte, headerSize+len(body))
	message[0] = msgClose
	binary.BigEndian.PutUint32(message[1:], uint32(len(body)+4))
	copy(message[headerSize:], body)
	return message
}

// statementNameOf reads the prepared statement a message refers to, for the
// message types that refer to one by name. The name is a slice of the message,
// not a copy of it.
func statementNameOf(kind byte, body []byte) ([]byte, bool) {
	switch kind {
	case msgBind:
		// Portal name, then statement name.
		portal := bytes.IndexByte(body, 0)
		if portal < 0 {
			return nil, false
		}
		return cBytes(body[portal+1:])
	case msgDescribe, msgClose:
		// Describe and Close name a portal as readily as a statement.
		if len(body) == 0 || body[0] != describeStatement {
			return nil, false
		}
		return cBytes(body[1:])
	default:
		return nil, false
	}
}

// cBytes reads a null-terminated string from the front of a buffer without
// copying it.
func cBytes(body []byte) ([]byte, bool) {
	end := bytes.IndexByte(body, 0)
	if end < 0 {
		return nil, false
	}
	return body[:end], true
}
