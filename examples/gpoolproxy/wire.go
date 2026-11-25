// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import "encoding/binary"

// Authentication request codes, the first four bytes of an 'R' message.
const (
	authOK           uint32 = 0
	authCleartext    uint32 = 3
	authMD5          uint32 = 5
	authSASL         uint32 = 10
	authSASLContinue uint32 = 11
	authSASLFinal    uint32 = 12
)

// Message types of the startup phase, in both directions.
const (
	msgAuthentication  = 'R'
	msgParameterStatus = 'S'
	msgBackendKeyData  = 'K'
	msgErrorResponse   = 'E'
	msgPassword        = 'p'
)

const scramMechanism = "SCRAM-SHA-256"

// appendMessage appends one typed, length-prefixed message. The length counts
// itself and the body but not the type byte, which is the protocol's rule and
// the thing most often got wrong by hand.
func appendMessage(dst []byte, kind byte, body ...[]byte) []byte {
	length := 4
	for _, part := range body {
		length += len(part)
	}

	dst = append(dst, kind)
	dst = binary.BigEndian.AppendUint32(dst, uint32(length))
	for _, part := range body {
		dst = append(dst, part...)
	}
	return dst
}

// appendAuthentication appends an authentication request with its payload.
func appendAuthentication(dst []byte, code uint32, data []byte) []byte {
	return appendMessage(dst, msgAuthentication, binary.BigEndian.AppendUint32(nil, code), data)
}

// appendParameterStatus appends one server setting as a client expects to be
// told it during startup.
func appendParameterStatus(dst []byte, name, value string) []byte {
	return appendMessage(dst, msgParameterStatus, terminated(name), terminated(value))
}

// appendBackendKeyData appends the key a client must quote to cancel a query.
func appendBackendKeyData(dst []byte, pid uint32, secret []byte) []byte {
	return appendMessage(dst, msgBackendKeyData, binary.BigEndian.AppendUint32(nil, pid), secret)
}

// appendReadyForQuery appends the message that ends startup and every
// transaction unit after it.
func appendReadyForQuery(dst []byte, status byte) []byte {
	return appendMessage(dst, msgReadyForQuery, []byte{status})
}

// terminated is a string with the terminator the protocol puts after every one.
func terminated(text string) []byte {
	out := make([]byte, 0, len(text)+1)
	out = append(out, text...)
	return append(out, 0)
}
