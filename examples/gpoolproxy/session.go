// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/gsoultan/gpool/pkg/pooling"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Startup-phase packets carry a version code instead of a message type.
const (
	protocolVersion30 = 196608
	sslRequestCode    = 80877103
	cancelRequestCode = 80877102
	gssEncRequestCode = 80877104

	// maxStartupLen bounds the startup packet, which is read before any
	// authentication has happened and is therefore the one message an
	// unauthenticated peer can make the proxy allocate for.
	maxStartupLen = 10000
)

// errHandled ends a connection that was served completely during startup, such
// as a cancellation request, without it being a failure.
var errHandled = errors.New("gpoolproxy: connection handled during startup")

// session is one client connection.
//
// Two goroutines run it: one pumps client messages towards whichever backend is
// attached, the other relays that backend's replies back. A proxy is full duplex
// — a client may pipeline a second batch before the first is answered — so a
// single loop would have to guess which side to read from next.
type session struct {
	proxy  *Proxy
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	key cancelKey

	// mu guards the attachment between this session and a pooled backend.
	//
	// It is held across the forward of a client message, which is what makes the
	// handover safe: the relay goroutine cannot return a backend to the pool
	// while bytes are still being written to it. The cost is that a release can
	// be delayed by one slow message body; the alternative is a backend serving
	// two clients at once.
	mu       sync.Mutex
	held     pooling.Handle[*backend]
	attached bool

	// statements is every prepared statement this client has created, kept so it
	// can be replayed onto whichever backend the next transaction lands on.
	statements *statements

	// expect is the completion messages the backend owes for work this proxy
	// injected rather than the client sending it. They are swallowed on the way
	// back, because a client counting replies to its own messages must not
	// receive one for a message it never sent.
	expect []byte

	// pending counts client messages that will each be answered with exactly one
	// ReadyForQuery. A backend goes back to the pool only when this reaches zero,
	// so a pipelined client is fully answered before its backend is handed on.
	pending int

	attach chan *backend

	ctx  context.Context
	stop context.CancelFunc
}

func newSession(proxy *Proxy, conn net.Conn, key cancelKey) *session {
	ctx, stop := context.WithCancel(proxy.ctx)

	return &session{
		proxy:      proxy,
		conn:       conn,
		reader:     bufio.NewReaderSize(conn, clientBufSize),
		writer:     bufio.NewWriterSize(conn, clientBufSize),
		key:        key,
		statements: newStatements(proxy.config.MaxPreparedStatements),
		attach:     make(chan *backend, 1),
		ctx:        ctx,
		stop:       stop,
	}
}

// serve runs the session to completion.
func (s *session) serve() {
	defer s.stop()
	defer s.conn.Close()

	if err := s.startup(); err != nil {
		if !errors.Is(err, errHandled) {
			s.reject(err)
		}
		return
	}

	s.proxy.register(s)
	defer s.proxy.unregister(s)

	var wg sync.WaitGroup
	wg.Go(func() { s.relay() })

	s.pump()

	s.detach()
	wg.Wait()
	s.releaseBackend()
}

// startup performs the client handshake up to ReadyForQuery.
//
// The startup phase is parsed here rather than through pgproto3.Backend so that
// the proxy owns its socket buffer from the first byte. Handing the reader to a
// decoder and taking it back would mean trusting that the decoder had not read
// ahead, and unlike the frontend it offers no way to check.
func (s *session) startup() error {
	for {
		code, body, err := s.readStartupPacket()
		if err != nil {
			return err
		}

		switch code {
		case sslRequestCode, gssEncRequestCode:
			if err := s.negotiateEncryption(code); err != nil {
				return err
			}
		case cancelRequestCode:
			if len(body) < 8 {
				return errors.New("gpoolproxy: truncated cancellation request")
			}
			s.proxy.cancel(binary.BigEndian.Uint32(body), string(body[4:]))
			return errHandled
		case protocolVersion30:
			if err := s.authenticate(parameterOf(body, "user")); err != nil {
				return err
			}
			return s.ready()
		default:
			return fmt.Errorf("gpoolproxy: unsupported protocol version %d", code)
		}
	}
}

// readStartupPacket reads one length-prefixed, untyped startup packet.
func (s *session) readStartupPacket() (code uint32, body []byte, err error) {
	var header [8]byte
	if _, err := io.ReadFull(s.reader, header[:]); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(header[:4])
	if length < 8 || length > maxStartupLen {
		return 0, nil, fmt.Errorf("gpoolproxy: startup packet length %d out of range", length)
	}

	body = make([]byte, length-8)
	if _, err := io.ReadFull(s.reader, body); err != nil {
		return 0, nil, err
	}
	return binary.BigEndian.Uint32(header[4:]), body, nil
}

// negotiateEncryption answers an SSLRequest or GSSENCRequest. Either is a single
// byte: 'S' to proceed, 'N' to continue in the clear.
func (s *session) negotiateEncryption(code uint32) error {
	if code != sslRequestCode || s.proxy.tls == nil {
		return s.writeByte('N')
	}
	if err := s.writeByte('S'); err != nil {
		return err
	}

	encrypted := s.proxy.upgrade(s.conn)
	if err := encrypted.HandshakeContext(s.ctx); err != nil {
		return err
	}

	s.conn = encrypted
	s.reader.Reset(encrypted)
	s.writer.Reset(encrypted)
	return nil
}

func (s *session) writeByte(value byte) error {
	if err := s.writer.WriteByte(value); err != nil {
		return err
	}
	return s.writer.Flush()
}

// authenticate runs SCRAM-SHA-256 against the client.
//
// An unknown user is taken through the whole exchange against a throwaway
// verifier rather than refused up front. Refusing early is measurably faster,
// and that difference is exactly what turns the proxy into an oracle for which
// usernames exist.
func (s *session) authenticate(user string) error {
	credential, known := s.proxy.users.lookup(user)
	if !known {
		credential = s.proxy.decoy
	}
	scram := &scramServer{verifier: credential}

	if err := s.send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}}); err != nil {
		return err
	}

	initial, err := s.readSASLInitialResponse()
	if err != nil {
		return err
	}
	challenge, err := scram.challenge(initial)
	if err != nil {
		return err
	}
	if err := s.send(&pgproto3.AuthenticationSASLContinue{Data: challenge}); err != nil {
		return err
	}

	final, err := s.readSASLResponse()
	if err != nil {
		return err
	}
	signature, err := scram.verify(final)
	if err != nil {
		return err
	}
	if !known {
		return ErrAuthFailed
	}

	return s.send(
		&pgproto3.AuthenticationSASLFinal{Data: signature},
		&pgproto3.AuthenticationOk{},
	)
}

// readSASLInitialResponse reads the client's first SASL message and checks the
// mechanism it chose.
func (s *session) readSASLInitialResponse() ([]byte, error) {
	body, err := s.readPasswordMessage()
	if err != nil {
		return nil, err
	}

	mechanism, rest, found := cutCString(body)
	if !found {
		return nil, fmt.Errorf("%w: SASL response names no mechanism", ErrAuthFailed)
	}
	if mechanism != "SCRAM-SHA-256" {
		return nil, fmt.Errorf("%w: unsupported SASL mechanism %q", ErrAuthFailed, mechanism)
	}
	if len(rest) < 4 {
		return nil, fmt.Errorf("%w: SASL response is truncated", ErrAuthFailed)
	}

	size := int32(binary.BigEndian.Uint32(rest))
	rest = rest[4:]
	if size < 0 || int(size) > len(rest) {
		return nil, fmt.Errorf("%w: SASL response length %d does not match its body", ErrAuthFailed, size)
	}
	return rest[:size], nil
}

func (s *session) readSASLResponse() ([]byte, error) {
	return s.readPasswordMessage()
}

// readPasswordMessage reads one 'p' message, which is what every authentication
// reply from the client is typed as.
func (s *session) readPasswordMessage() ([]byte, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(s.reader, header[:]); err != nil {
		return nil, err
	}
	if header[0] != 'p' {
		return nil, fmt.Errorf("%w: expected an authentication message, got %q", ErrAuthFailed, header[0])
	}

	length := binary.BigEndian.Uint32(header[1:])
	if length < 4 || length > maxStartupLen {
		return nil, fmt.Errorf("%w: authentication message length %d out of range", ErrAuthFailed, length)
	}

	body := make([]byte, length-4)
	_, err := io.ReadFull(s.reader, body)
	return body, err
}

// ready completes startup, replaying the server's own parameters so the client
// sees the settings it will actually be running under.
func (s *session) ready() error {
	messages := []pgproto3.BackendMessage{}
	for name, value := range s.proxy.parameters() {
		messages = append(messages, &pgproto3.ParameterStatus{Name: name, Value: value})
	}
	messages = append(messages,
		&pgproto3.BackendKeyData{ProcessID: s.key.pid, SecretKey: []byte(s.key.secret)},
		&pgproto3.ReadyForQuery{TxStatus: txIdle},
	)
	return s.send(messages...)
}

// send encodes and flushes backend messages.
func (s *session) send(messages ...pgproto3.BackendMessage) error {
	var buf []byte
	var err error

	for _, message := range messages {
		if buf, err = message.Encode(buf); err != nil {
			return err
		}
	}
	if _, err := s.writer.Write(buf); err != nil {
		return err
	}
	return s.writer.Flush()
}

// reject reports a startup failure to the client in the protocol's own terms.
//
// The message is deliberately generic. An authentication failure that explained
// itself would tell an unauthenticated peer whether a username exists.
func (s *session) reject(cause error) {
	message := &pgproto3.ErrorResponse{Severity: "FATAL", Code: "08P01", Message: "protocol error"}
	if errors.Is(cause, ErrAuthFailed) {
		message = &pgproto3.ErrorResponse{Severity: "FATAL", Code: "28P01", Message: "authentication failed"}
	}
	_ = s.send(message)
}

// pump forwards client messages to the attached backend, acquiring one on demand.
func (s *session) pump() {
	forwarder := newRelay()

	for {
		kind, bodyLen, err := forwarder.readHeader(s.reader)
		if err != nil {
			return
		}
		if kind == msgTerminate {
			return
		}
		if err := s.forward(forwarder, kind, bodyLen); err != nil {
			return
		}
	}
}

// forward sends one client message onward under the attachment lock.
func (s *session) forward(forwarder *relay, kind byte, bodyLen int) error {
	// Messages that name a prepared statement have to be understood before they
	// can be sent on, so they are read into memory first. Everything else — every
	// Execute, every CopyData, the bulk of the traffic — is still relayed without
	// being decoded.
	var message []byte
	if namesStatement(kind) {
		var err error
		if message, err = forwarder.readBody(s.reader, bodyLen); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	target, err := s.ensureBackend()
	if err != nil {
		return err
	}

	if message != nil {
		if err := s.reconcile(target, kind, message); err != nil {
			return err
		}
		if _, err := target.writer.Write(message); err != nil {
			target.fail()
			return err
		}
	} else if _, err := forwarder.forwardBody(target.writer, s.reader, bodyLen); err != nil {
		target.fail()
		return err
	}

	if endsTransactionUnit(kind) {
		s.pending++
	}
	if err := flushIfDrained(target.writer, s.reader); err != nil {
		target.fail()
		return err
	}
	return nil
}

// reconcile makes the backend hold the prepared statement the message refers to.
//
// This is what makes named prepared statements survive transaction pooling. A
// statement lives on the backend that parsed it, and the next transaction may
// land somewhere else; replaying the remembered Parse first is the whole trick,
// and it is what PgBouncer added in 1.21.
//
// The caller holds s.mu.
func (s *session) reconcile(target *backend, kind byte, message []byte) error {
	body := message[headerSize:]

	switch kind {
	case msgParse:
		name, ok := parseName(body)
		if !ok || name == "" {
			// The unnamed statement is replaced by every Parse and never replayed.
			return nil
		}
		// A name already on this backend belongs to whoever put it there, which
		// may be a different client with different SQL. Re-parsing over it fails
		// with 42P05, and *not* re-parsing would silently run their statement
		// instead of this one — so it is closed first, either way.
		if _, held := target.prepared.lookup(name); held {
			if err := s.inject(target, closeStatement(name), msgCloseComplete); err != nil {
				return err
			}
		}
		// Whatever this displaces from the client's own set simply stops being
		// replayable. It is still on the backend that has it, and that backend's
		// limit is what eventually takes it off the server.
		s.statements.remember(name, message)
		remembered, _ := s.statements.lookup(name)
		if err := s.hold(target, name, remembered); err != nil {
			return err
		}

	case msgBind, msgDescribe, msgClose:
		name, ok := statementNameOf(kind, body)
		if !ok || name == "" {
			return nil
		}
		if kind == msgClose {
			// The client is deallocating it, so neither side should remember it.
			target.prepared.forget(name)
			s.statements.forget(name)
			return nil
		}
		if err := s.ensurePrepared(target, name); err != nil {
			return err
		}
	}
	return nil
}

// ensurePrepared replays a remembered Parse onto a backend that lacks it.
func (s *session) ensurePrepared(target *backend, name string) error {
	remembered, known := s.statements.lookup(name)
	if !known {
		// The client is binding something it never parsed on this session. The
		// server will say so; forwarding is more honest than inventing an error.
		return nil
	}
	if held, ok := target.prepared.lookup(name); ok {
		if bytes.Equal(held, remembered) {
			return nil
		}
		if err := s.inject(target, closeStatement(name), msgCloseComplete); err != nil {
			return err
		}
	}

	if err := s.inject(target, remembered, msgParseComplete); err != nil {
		return err
	}
	return s.hold(target, name, remembered)
}

// hold records a statement as present on a backend, closing whatever had to be
// discarded to make room for it.
//
// Forgetting alone would be a lie: the statement is on the server whether the
// proxy remembers it or not, and a set that silently drifts below what the server
// holds is how the server's memory grows without anything accounting for it. The
// client that prepared the evicted statement is unharmed — its own set still has
// the Parse, so the next backend it lands on gets it replayed.
//
// The caller holds s.mu.
func (s *session) hold(target *backend, name string, message []byte) error {
	evicted, didEvict := target.prepared.remember(name, message)
	if !didEvict {
		return nil
	}
	return s.inject(target, closeStatement(evicted), msgCloseComplete)
}

// inject writes a message the client did not send, and records the completion
// the backend will answer with so the relay can swallow it.
func (s *session) inject(target *backend, message []byte, completion byte) error {
	if _, err := target.writer.Write(message); err != nil {
		target.fail()
		return err
	}
	s.expect = append(s.expect, completion)
	return nil
}

// swallows reports whether the next backend message answers something this proxy
// injected, and consumes the expectation if so.
func (s *session) swallows(kind byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.expect) == 0 || s.expect[0] != kind {
		return false
	}
	s.expect = s.expect[1:]
	return true
}

// ensureBackend attaches a pooled backend if the session has none. The caller
// holds s.mu.
func (s *session) ensureBackend() (*backend, error) {
	if s.attached {
		return s.held.Conn(), nil
	}

	handle, err := s.proxy.core.Acquire(s.ctx)
	if err != nil {
		return nil, err
	}
	s.held = handle
	s.attached = true

	// Buffered by one, and only ever one attachment is outstanding, so this
	// cannot block while the lock is held.
	s.attach <- handle.Conn()
	return handle.Conn(), nil
}

// relay carries backend replies to the client and returns each backend to the
// pool at its transaction boundary.
func (s *session) relay() {
	forwarder := newRelay()

	for source := range s.attach {
		if !s.relayFrom(forwarder, source) {
			return
		}
	}
}

// relayFrom relays one backend's replies until it is detached or fails. It
// reports whether the session may continue.
func (s *session) relayFrom(forwarder *relay, source *backend) bool {
	for {
		kind, bodyLen, err := forwarder.readHeader(source.reader)
		if err != nil {
			source.fail()
			s.stop()
			return false
		}

		// A completion for a Parse or Close this proxy injected is not the
		// client's to see: it is counting replies to its own messages.
		if s.swallows(kind) {
			if err := forwarder.discardBody(source.reader, bodyLen); err != nil {
				source.fail()
				s.stop()
				return false
			}
			continue
		}

		body, err := forwarder.forwardBody(s.writer, source.reader, bodyLen)
		if err != nil {
			source.fail()
			s.stop()
			return false
		}

		if kind == msgReadyForQuery && len(body) == 1 {
			if s.completeUnit(source, body[0]) {
				return s.writer.Flush() == nil
			}
		}
		if err := flushIfDrained(s.writer, source.reader); err != nil {
			s.stop()
			return false
		}
	}
}

// completeUnit records a ReadyForQuery and reports whether the backend was
// returned to the pool.
func (s *session) completeUnit(source *backend, status byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	source.txStatus = status
	if s.pending > 0 {
		s.pending--
	}

	// An open transaction keeps its backend even at zero pending: releasing it
	// would hand another client a live transaction. Transaction-mode pooling
	// means one backend per transaction, not one per statement.
	if s.pending > 0 || status != txIdle {
		return false
	}

	s.held.Release()
	s.attached = false
	return true
}

// detach unblocks the relay goroutine once the client has gone.
func (s *session) detach() {
	s.mu.Lock()
	if s.attached {
		// The relay is parked reading a backend that will not answer, because
		// whatever it was waiting for died with the client. Interrupting the
		// read is what lets the session end without waiting on the server, and
		// the connection is discarded afterwards because a read abandoned
		// mid-message leaves the stream unparseable.
		s.held.Conn().interrupt()
		s.held.Conn().fail()
	}
	s.mu.Unlock()
	close(s.attach)
}

// releaseBackend returns a still-attached backend after both goroutines are done.
func (s *session) releaseBackend() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.attached {
		s.held.Release()
		s.attached = false
	}
}

// currentBackend reports the backend this session is using, for cancellation.
func (s *session) currentBackend() (*backend, func()) {
	s.mu.Lock()
	if !s.attached {
		s.mu.Unlock()
		return nil, func() {}
	}
	// The lock is handed to the caller so the backend cannot be released, and
	// reused by another client, between being read here and being cancelled.
	return s.held.Conn(), s.mu.Unlock
}

// parameterOf extracts a startup parameter from its null-terminated key/value list.
func parameterOf(body []byte, want string) string {
	for {
		key, rest, ok := cutCString(body)
		if !ok || key == "" {
			return ""
		}
		value, rest, ok := cutCString(rest)
		if !ok {
			return ""
		}
		if key == want {
			return value
		}
		body = rest
	}
}

// cutCString splits a null-terminated string from the front of a buffer.
func cutCString(body []byte) (value string, rest []byte, ok bool) {
	end := -1
	for i, b := range body {
		if b == 0 {
			end = i
			break
		}
	}
	if end < 0 {
		return "", body, false
	}
	return string(body[:end]), body[end+1:], true
}
