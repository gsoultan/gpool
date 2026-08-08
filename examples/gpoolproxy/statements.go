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

// statements remembers the prepared statements one client has created.
//
// Transaction-mode pooling moves a client between backends, and a named prepared
// statement lives on the backend that parsed it. Without this, a client that
// prepares once and binds many times fails the moment it lands somewhere new —
// which is what PgBouncer answered in 1.21, and what the README previously named
// as the gap between this example and something production-worthy.
//
// What is remembered is the whole Parse message, byte for byte, so replaying it
// needs no re-encoding and cannot disagree with what the client actually sent.
type statements struct {
	byName map[string][]byte
}

func newStatements() *statements {
	return &statements{byName: make(map[string][]byte)}
}

// remember records a Parse so it can be replayed onto another backend.
//
// The unnamed statement is deliberately not remembered. It lives only until the
// next Parse, every client overwrites it constantly, and replaying a stale one
// would bind arguments to the wrong SQL.
func (s *statements) remember(name string, message []byte) {
	if name == "" {
		return
	}
	s.byName[name] = bytes.Clone(message)
}

func (s *statements) forget(name string) {
	delete(s.byName, name)
}

func (s *statements) lookup(name string) ([]byte, bool) {
	message, ok := s.byName[name]
	return message, ok
}

// parseName reads the statement name a Parse message declares, which is the
// first null-terminated string in its body.
func parseName(body []byte) (string, bool) {
	return cString(body)
}

// bindName reads the statement a Bind refers to. Its body is the portal name
// then the statement name.
func bindName(body []byte) (string, bool) {
	_, rest, ok := cutBytes(body)
	if !ok {
		return "", false
	}
	return cString(rest)
}

// targetName reads the statement a Describe or Close refers to, and reports
// whether it refers to a statement at all rather than to a portal.
func targetName(body []byte) (string, bool) {
	if len(body) == 0 || body[0] != describeStatement {
		return "", false
	}
	return cString(body[1:])
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
// message types that refer to one by name.
func statementNameOf(kind byte, body []byte) (string, bool) {
	switch kind {
	case msgBind:
		return bindName(body)
	case msgDescribe, msgClose:
		return targetName(body)
	default:
		return "", false
	}
}
