// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// PostgreSQL frames every post-startup message as a one-byte type followed by a
// four-byte length that counts itself.
const (
	headerSize = 5

	// relayBufSize is how large a message body may be and still pass through the
	// reusable buffer rather than being streamed. Anything larger is copied
	// straight from one socket to the other, so a big result set costs no
	// per-message allocation either way.
	relayBufSize = 64 << 10

	// maxMessageLen rejects a length field that would have us allocate or wait on
	// an absurd amount of data. An adversarial peer can otherwise announce 4 GiB
	// and hold the connection open.
	maxMessageLen = 256 << 20
)

// Frontend message types this proxy has to recognise. Everything else is
// forwarded without being understood, which is the point: a proxy that decodes
// every DataRow pays for parsing it will never use.
const (
	msgQuery        = 'Q'
	msgSync         = 'S'
	msgFunctionCall = 'F'
	msgTerminate    = 'X'

	// msgReadyForQuery is the only backend message the proxy inspects. Its
	// one-byte body carries the transaction status, which is what marks a
	// transaction boundary and therefore when a backend may be handed on.
	msgReadyForQuery = 'Z'
)

// Transaction status values carried by ReadyForQuery.
const (
	txIdle    = 'I'
	txInBlock = 'T'
	txFailed  = 'E'
)

// endsTransactionUnit reports whether a frontend message will eventually be
// answered with exactly one ReadyForQuery.
//
// The pairing has to be exact, because it is what lets the proxy know a pipelined
// client is fully answered before its backend is handed to someone else. Query,
// Sync and FunctionCall each produce one. Flush produces output but no
// ReadyForQuery. CopyDone and CopyFail produce one only in the simple protocol,
// and there it belongs to the Query that started the copy, which is already
// counted — counting them again would release the backend an answer early.
func endsTransactionUnit(kind byte) bool {
	return kind == msgQuery || kind == msgSync || kind == msgFunctionCall
}

// relay forwards protocol messages between two sockets without decoding them.
//
// One relay belongs to one direction of one session, so its buffer needs no
// synchronisation.
type relay struct {
	header [headerSize]byte
	buf    []byte
}

func newRelay() *relay {
	return &relay{buf: make([]byte, relayBufSize)}
}

// readHeader reads a message header and reports its type and body length. The
// header is held in the relay until forwardBody writes it, which is what lets a
// caller decide where a message is going after seeing what it is.
func (r *relay) readHeader(src *bufio.Reader) (kind byte, bodyLen int, err error) {
	if _, err := io.ReadFull(src, r.header[:]); err != nil {
		return 0, 0, err
	}

	length := binary.BigEndian.Uint32(r.header[1:])
	if length < 4 || length > maxMessageLen {
		return 0, 0, fmt.Errorf("gpoolproxy: message length %d out of range", length)
	}
	return r.header[0], int(length) - 4, nil
}

// forwardBody writes the header read by readHeader followed by the body.
//
// The returned slice is the body only when it fit in the reusable buffer, and it
// stays valid only until the next call. ReadyForQuery is one byte, so the one
// message whose contents matter always comes back.
func (r *relay) forwardBody(dst *bufio.Writer, src *bufio.Reader, bodyLen int) ([]byte, error) {
	if _, err := dst.Write(r.header[:]); err != nil {
		return nil, err
	}
	if bodyLen == 0 {
		return nil, nil
	}

	if bodyLen <= len(r.buf) {
		body := r.buf[:bodyLen]
		if _, err := io.ReadFull(src, body); err != nil {
			return nil, err
		}
		if _, err := dst.Write(body); err != nil {
			return nil, err
		}
		return body, nil
	}

	if _, err := io.CopyN(dst, src, int64(bodyLen)); err != nil {
		return nil, err
	}
	return nil, nil
}

// forward moves one whole message, for a direction that does not need to look at
// the header before choosing a destination.
func (r *relay) forward(dst *bufio.Writer, src *bufio.Reader) (kind byte, body []byte, err error) {
	kind, bodyLen, err := r.readHeader(src)
	if err != nil {
		return 0, nil, err
	}
	body, err = r.forwardBody(dst, src, bodyLen)
	return kind, body, err
}

// flushIfDrained writes buffered output only once the source has nothing more
// waiting. A pipelining client sends several messages in one segment, so
// flushing per message would multiply syscalls by the batch size; waiting for the
// source to drain coalesces them and still flushes the instant the peer pauses.
func flushIfDrained(dst *bufio.Writer, src *bufio.Reader) error {
	if src.Buffered() > 0 {
		return nil
	}
	return dst.Flush()
}
