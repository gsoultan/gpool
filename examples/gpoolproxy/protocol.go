// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

// PostgreSQL frames every post-startup message as a one-byte type followed by a
// four-byte length that counts itself.
const (
	headerSize = 5

	// readBufSize is the one buffer a whole event loop reads through. It belongs
	// to the loop rather than to a connection, so it is multiplied by the number
	// of cores and not by the number of clients — which is the number a pooler
	// exists to make large.
	readBufSize = 64 << 10

	// spareBufSize is the unit a connection borrows when it has to hold bytes
	// across events: the tail of a message that arrived split, or output the
	// peer's socket would not take. A connection with neither holds no buffer at
	// all, and that is the state every idle client is in.
	spareBufSize = 4 << 10

	// smallTail is the size up to which held input is copied into an allocation
	// of exactly its own length instead of a standard buffer.
	smallTail = 512

	// spareBufKeep bounds how many returned buffers a loop keeps for reuse. Past
	// it they go back to the collector, so a burst does not become permanent
	// resident memory.
	spareBufKeep = 256

	// wholeMessageLen is the size up to which a client message is assembled
	// complete before a backend is acquired for it. Without this a client could
	// hold one of a handful of server connections by sending five bytes of
	// header and nothing else.
	wholeMessageLen = spareBufSize

	// handshakeBufSize is the read buffer for a client's startup exchange. It
	// lives only until the client is authenticated; startup packets are a few
	// hundred bytes.
	handshakeBufSize = 1 << 10

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
	msgExecute      = 'E'

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

// opensTransactionUnit reports whether a frontend message leaves the backend
// owing work that only a later Sync will close.
//
// Everything in the extended protocol short of Sync does: a Parse, Bind,
// Describe, Execute or Close is answered, but the backend is not back at a
// boundary until the Sync that follows. A backend handed on between an Execute
// and its Sync would receive the Sync from a different client's point of view —
// which is exactly what happens to a pipelining client whose batch arrives in
// two segments, if only ReadyForQuery is counted.
func opensTransactionUnit(kind byte) bool {
	switch kind {
	case msgParse, msgBind, msgDescribe, msgExecute, msgClose:
		return true
	}
	return false
}
