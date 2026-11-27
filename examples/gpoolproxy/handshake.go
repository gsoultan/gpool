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
)

// Startup-phase packets carry a version code instead of a message type.
const (
	protocolVersion30 = 196608
	// protocolVersion32 is PostgreSQL 18's, which differs from 3.0 only in a
	// longer cancel key. The proxy offers 3.0 in its place.
	protocolVersion32 = protocolVersion30 | 2
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
// The startup phase is parsed here, by hand, so that the proxy owns its socket
// buffer from the first byte and knows exactly what has been read from it.
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
		case protocolVersion30, protocolVersion32:
			// A client asking for a newer minor version, as one does for
			// PostgreSQL 18's 3.2, is told the proxy speaks 3.0, which is what
			// a server older than 18 answers. Refusing the packet instead, as
			// PgBouncer before 1.24 does, costs such a client a second dial.
			if code != protocolVersion30 {
				if err := s.send(negotiateProtocolVersion(body)); err != nil {
					return err
				}
			}
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

	if err := s.send(appendAuthentication(nil, authSASL, append(terminated(scramMechanism), 0))); err != nil {
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
	if err := s.send(appendAuthentication(nil, authSASLContinue, challenge)); err != nil {
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

	return s.send(appendAuthentication(appendAuthentication(nil, authSASLFinal, signature), authOK, nil))
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

	var buf []byte
	for name, value := range s.proxy.parameters() {
		buf = appendParameterStatus(buf, name, value)
	}
	buf = appendBackendKeyData(buf, s.key.pid, []byte(s.key.secret))
	return s.send(appendReadyForQuery(buf, txIdle))
}

// send writes encoded messages as one segment.
func (s *handshake) send(encoded []byte) error {
	_, err := s.conn.Write(encoded)
	return err
}

// reject reports a startup failure to the client in the protocol's own terms.
//
// The message is deliberately generic. An authentication failure that explained
// itself would tell an unauthenticated peer whether a username exists.
func (s *handshake) reject(cause error) {
	code, text := "08P01", "protocol error"
	switch {
	case errors.Is(cause, ErrAuthFailed):
		code, text = "28P01", "authentication failed"
	case errors.Is(cause, errReplicationDisabled):
		code, text = "0A000", "replication connections are not enabled on this proxy"
	case errors.Is(cause, errReplicationFull):
		code, text = "53300", "too many replication connections"
	case errors.Is(cause, errReplicationUnavailable):
		code, text = "08006", "the server did not accept a replication connection"
	}
	_ = s.send(errorResponse("FATAL", code, text))
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

// negotiateProtocolVersion is the NegotiateProtocolVersion that offers 3.0,
// naming the protocol options in a startup packet's body (those whose names
// begin "_pq_."), none of which the proxy recognises.
func negotiateProtocolVersion(body []byte) []byte {
	var options []string
	for {
		key, rest, ok := cutCString(body)
		if !ok || key == "" {
			break
		}
		if strings.HasPrefix(key, "_pq_.") {
			options = append(options, key)
		}
		if _, body, ok = cutCString(rest); !ok {
			break
		}
	}
	message := []byte{msgNegotiateProtocolVersion, 0, 0, 0, 0}
	message = binary.BigEndian.AppendUint32(message, 0)
	message = binary.BigEndian.AppendUint32(message, uint32(len(options)))
	for _, option := range options {
		message = append(append(message, option...), 0)
	}
	binary.BigEndian.PutUint32(message[1:], uint32(len(message)-1))
	return message
}
