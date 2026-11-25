// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

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

// handshake is one client connection from accept until it is authenticated.
//
// It runs on a goroutine of its own, with ordinary blocking reads, because that
// is the part of a connection's life where blocking is the simple and correct
// thing: a TLS negotiation and a SCRAM exchange are a strict back-and-forth, they
// happen once, and they are over in milliseconds. What it holds — a goroutine, a
// read buffer — is released when it finishes, and the connection is handed to an
// event loop as a bare file descriptor. Nothing here is still allocated for a
// client that is merely connected.
type handshake struct {
	proxy  *Proxy
	conn   net.Conn
	reader *bufio.Reader

	key cancelKey

	// pinned is the dedicated server connection opened for a client that asked
	// for replication, and nil for everyone else.
	pinned *backend

	ctx  context.Context
	stop context.CancelFunc
}

func newHandshake(proxy *Proxy, conn net.Conn, key cancelKey) *handshake {
	ctx, stop := context.WithCancel(proxy.ctx)

	return &handshake{
		proxy:  proxy,
		conn:   conn,
		reader: bufio.NewReaderSize(conn, handshakeBufSize),
		key:    key,
		ctx:    ctx,
		stop:   stop,
	}
}

// serve authenticates the client and hands it to an event loop. It reports
// whether the client was adopted; if not, the connection is closed and the
// caller still owns the client slot.
func (s *handshake) serve() bool {
	defer s.stop()

	if err := s.startup(); err != nil {
		if !errors.Is(err, errHandled) {
			s.reject(err)
		}
		s.abandon()
		return false
	}

	// A client is entitled to send its first query without waiting to be asked,
	// so whatever is already in the read buffer travels with the connection.
	early, _ := s.reader.Peek(s.reader.Buffered())
	if err := s.proxy.adopt(s.conn, s.key, early, s.pinned); err != nil {
		s.abandon()
		return false
	}
	return true
}

// abandon closes a client that will not be adopted, along with the dedicated
// connection opened for it if there was one.
func (s *handshake) abandon() {
	if s.pinned != nil {
		s.proxy.endReplication(s.pinned)
		s.pinned = nil
	}
	_ = s.conn.Close()
}

// replicationMode reports whether a startup packet asks for a replication
// connection, and with what value.
//
// PostgreSQL takes "database" for logical replication and any boolean spelling
// for physical. Only the spellings of false mean an ordinary connection, and a
// client that sends one of those gets one.
func replicationMode(value string) (string, bool) {
	switch strings.ToLower(value) {
	case "", "false", "off", "no", "0", "f", "n":
		return "", false
	}
	return value, true
}

// startup performs the client handshake up to ReadyForQuery.
//
// The startup phase is parsed here rather than through pgproto3.Backend so that
// the proxy owns its socket buffer from the first byte. Handing the reader to a
// decoder and taking it back would mean trusting that the decoder had not read
// ahead, and unlike the frontend it offers no way to check.
func (s *handshake) startup() error {
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
			// Only after authentication: an unauthenticated peer must not be
			// able to make the proxy dial the server.
			if mode, wanted := replicationMode(parameterOf(body, "replication")); wanted {
				if s.pinned, err = s.proxy.replicate(s.ctx, mode); err != nil {
					return err
				}
			}
			return s.ready()
		default:
			return fmt.Errorf("gpoolproxy: unsupported protocol version %d", code)
		}
	}
}

// readStartupPacket reads one length-prefixed, untyped startup packet.
func (s *handshake) readStartupPacket() (code uint32, body []byte, err error) {
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
func (s *handshake) negotiateEncryption(code uint32) error {
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
	return nil
}

func (s *handshake) writeByte(value byte) error {
	_, err := s.conn.Write([]byte{value})
	return err
}

// authenticate runs SCRAM-SHA-256 against the client.
//
// An unknown user is taken through the whole exchange against a throwaway
// verifier rather than refused up front. Refusing early is measurably faster,
// and that difference is exactly what turns the proxy into an oracle for which
// usernames exist.
func (s *handshake) authenticate(user string) error {
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
func (s *handshake) readSASLInitialResponse() ([]byte, error) {
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

func (s *handshake) readSASLResponse() ([]byte, error) {
	return s.readPasswordMessage()
}

// readPasswordMessage reads one 'p' message, which is what every authentication
// reply from the client is typed as.
func (s *handshake) readPasswordMessage() ([]byte, error) {
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
func (s *handshake) ready() error {
	// Serve warms these before accepting anyone, so this only does anything if
	// the server was unreachable then. Paying one connection here is better than
	// telling a client nothing about the server it is about to use.
	s.proxy.warm(s.ctx)

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

// send encodes backend messages and writes them as one segment.
func (s *handshake) send(messages ...pgproto3.BackendMessage) error {
	var buf []byte
	var err error

	for _, message := range messages {
		if buf, err = message.Encode(buf); err != nil {
			return err
		}
	}
	_, err = s.conn.Write(buf)
	return err
}

// reject reports a startup failure to the client in the protocol's own terms.
//
// The message is deliberately generic. An authentication failure that explained
// itself would tell an unauthenticated peer whether a username exists.
func (s *handshake) reject(cause error) {
	message := &pgproto3.ErrorResponse{Severity: "FATAL", Code: "08P01", Message: "protocol error"}
	switch {
	case errors.Is(cause, ErrAuthFailed):
		message = &pgproto3.ErrorResponse{Severity: "FATAL", Code: "28P01", Message: "authentication failed"}
	case errors.Is(cause, errReplicationDisabled):
		message = &pgproto3.ErrorResponse{Severity: "FATAL", Code: "0A000", Message: "replication connections are not enabled on this proxy"}
	case errors.Is(cause, errReplicationFull):
		message = &pgproto3.ErrorResponse{Severity: "FATAL", Code: "53300", Message: "too many replication connections"}
	case errors.Is(cause, errReplicationUnavailable):
		message = &pgproto3.ErrorResponse{Severity: "FATAL", Code: "08006", Message: "the server did not accept a replication connection"}
	}
	_ = s.send(message)
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
