// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"time"
)

const (
	// maxLoginMessage bounds any one message of the login exchange. The largest
	// honest one is an ErrorResponse with a long detail; nothing in startup is
	// megabytes, and the length comes from a peer that has proved nothing yet.
	maxLoginMessage = 64 << 10

	// maxLoginMessages bounds the exchange as a whole, so a peer cannot keep a
	// dial alive by trickling notices.
	maxLoginMessages = 256
)

// Refusals to log in that the server did not cause.
var (
	errTLSRefused    = errors.New("gpoolproxy: server does not offer TLS and the connection string requires it")
	errCleartext     = errors.New("gpoolproxy: server asked for the password in cleartext over an unencrypted connection")
	errAuthMethod    = errors.New("gpoolproxy: server asked for an authentication method this proxy does not implement")
	errLoginProtocol = errors.New("gpoolproxy: server broke the startup protocol")
)

// login is one connection to PostgreSQL, taken from a dial to ReadyForQuery.
//
// This is the proxy's own driver, and it is all the driver a proxy needs: once
// the server says it is ready, the connection is a descriptor that an event
// loop moves bytes through, and nothing here is used again. It decodes no rows
// and knows no types.
type login struct {
	target *upstream
	conn   net.Conn
	reader *bufio.Reader

	encrypted bool
	scram     *scramClient
	verified  bool

	pid      uint32
	secret   []byte
	statuses map[string]string

	// version is the protocol version the startup packet asks for, and
	// offered the NegotiateProtocolVersion the server answered with, if any.
	version uint32
	offered []byte
}

// connect dials the server and logs in. extra is settings for this connection
// alone, on top of the ones in the connection string.
func (u *upstream) connect(ctx context.Context, extra ...[2]string) (*login, error) {
	return u.connectAsking(ctx, protocolVersion30, extra...)
}

// connectAsking is connect with the protocol version to ask for.
func (u *upstream) connectAsking(ctx context.Context, version uint32, extra ...[2]string) (*login, error) {
	if u.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, u.timeout)
		defer cancel()
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, u.network, u.address)
	if err != nil {
		return nil, err
	}

	// A context cancelled mid-exchange has to interrupt a read that is already
	// blocked, and a deadline in the past is how a connection is told to.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	l := &login{target: u, conn: conn, statuses: map[string]string{}, version: version}
	if err := l.run(ctx, extra); err != nil {
		_ = l.conn.Close()
		return nil, err
	}
	if !stop() {
		_ = l.conn.Close()
		return nil, context.Cause(ctx)
	}
	return l, nil
}

func (l *login) run(ctx context.Context, extra [][2]string) error {
	if err := l.negotiateTLS(ctx); err != nil {
		return err
	}
	l.reader = bufio.NewReaderSize(l.conn, handshakeBufSize)

	if _, err := l.conn.Write(l.startupMessage(extra)); err != nil {
		return err
	}
	for range maxLoginMessages {
		kind, body, err := l.receive()
		if err != nil {
			return err
		}
		done, err := l.handle(kind, body)
		if done || err != nil {
			return err
		}
	}
	return errLoginProtocol
}

// negotiateTLS asks the server for encryption before anything else is sent, so
// that no credential and no setting crosses in the clear when it is available.
func (l *login) negotiateTLS(ctx context.Context) error {
	config := l.target.tls
	if config == nil {
		return nil
	}

	request := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 8), sslRequestCode)
	if _, err := l.conn.Write(request); err != nil {
		return err
	}
	// Exactly one byte, read from the connection itself. A buffered reader here
	// could take bytes that belong to the TLS handshake.
	var answer [1]byte
	if _, err := io.ReadFull(l.conn, answer[:]); err != nil {
		return err
	}

	switch answer[0] {
	case 'S':
		secured := tls.Client(l.conn, config)
		if err := secured.HandshakeContext(ctx); err != nil {
			return err
		}
		l.conn, l.encrypted = secured, true
		return nil
	case 'N':
		if l.target.required {
			return errTLSRefused
		}
		return nil
	}
	return errLoginProtocol
}

func (l *login) startupMessage(extra [][2]string) []byte {
	body := binary.BigEndian.AppendUint32(nil, l.version)
	for _, pair := range [][2]string{{"user", l.target.user}, {"database", l.target.database}} {
		body = append(append(body, terminated(pair[0])...), terminated(pair[1])...)
	}
	for _, settings := range [][][2]string{l.target.settings, extra} {
		for _, pair := range settings {
			body = append(append(body, terminated(pair[0])...), terminated(pair[1])...)
		}
	}
	body = append(body, 0)

	return append(binary.BigEndian.AppendUint32(nil, uint32(len(body)+4)), body...)
}

// receive reads one typed message.
func (l *login) receive() (byte, []byte, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(l.reader, header[:]); err != nil {
		return 0, nil, err
	}
	length := int(binary.BigEndian.Uint32(header[1:])) - 4
	if length < 0 || length > maxLoginMessage {
		return 0, nil, errLoginProtocol
	}

	body := make([]byte, length)
	_, err := io.ReadFull(l.reader, body)
	return header[0], body, err
}

// handle acts on one message and reports whether the server is ready.
func (l *login) handle(kind byte, body []byte) (bool, error) {
	switch kind {
	case msgAuthentication:
		return false, l.authenticate(body)
	case msgParameterStatus:
		name, rest, ok := cutCString(body)
		value, _, valueOK := cutCString(rest)
		if !ok || !valueOK {
			return false, errLoginProtocol
		}
		l.statuses[name] = value
	case msgBackendKeyData:
		if len(body) < 4 {
			return false, errLoginProtocol
		}
		l.pid, l.secret = binary.BigEndian.Uint32(body), body[4:]
	case msgErrorResponse:
		return false, parseServerError(body)
	case msgReadyForQuery:
		return true, l.finish()
	case msgNegotiateProtocolVersion:
		l.offered = body
	}
	// Notices and protocol negotiation carry nothing a proxy acts on.
	return false, nil
}

// finish checks the state the connection is handed over in.
func (l *login) finish() error {
	// The connection becomes a bare descriptor next, and bytes left in this
	// reader would be lost on the way. After ReadyForQuery the server is silent,
	// so there should be none; a server that spoke anyway is not one to pool.
	if l.reader.Buffered() != 0 {
		return errLoginProtocol
	}
	// A server that began SCRAM and then declared success without proving it
	// knew the verifier has not authenticated itself.
	if l.scram != nil && !l.verified {
		return errSCRAM
	}
	return nil
}

func (l *login) authenticate(body []byte) error {
	if len(body) < 4 {
		return errLoginProtocol
	}
	code, data := binary.BigEndian.Uint32(body), body[4:]

	switch code {
	case authOK:
		return nil
	case authCleartext:
		if !l.encrypted && !l.target.local() {
			return errCleartext
		}
		return l.sendPassword(terminated(l.target.password))
	case authMD5:
		if len(data) != 4 {
			return errLoginProtocol
		}
		return l.sendPassword(terminated(md5Response(l.target.user, l.target.password, data)))
	case authSASL:
		return l.beginSCRAM(data)
	case authSASLContinue:
		return l.continueSCRAM(data)
	case authSASLFinal:
		return l.finishSCRAM(data)
	}
	return errAuthMethod
}

func (l *login) sendPassword(body ...[]byte) error {
	_, err := l.conn.Write(appendMessage(nil, msgPassword, body...))
	return err
}

func (l *login) beginSCRAM(mechanisms []byte) error {
	if !offersMechanism(mechanisms, scramMechanism) {
		return errAuthMethod
	}

	l.scram = &scramClient{password: l.target.password}
	first, err := l.scram.first()
	if err != nil {
		return err
	}
	return l.sendPassword(terminated(scramMechanism), binary.BigEndian.AppendUint32(nil, uint32(len(first))), first)
}

func (l *login) continueSCRAM(challenge []byte) error {
	if l.scram == nil {
		return errLoginProtocol
	}
	final, err := l.scram.final(challenge)
	if err != nil {
		return err
	}
	return l.sendPassword(final)
}

func (l *login) finishSCRAM(signature []byte) error {
	if l.scram == nil {
		return errLoginProtocol
	}
	if err := l.scram.verify(signature); err != nil {
		return err
	}
	l.verified = true
	return nil
}

// offersMechanism reports whether a null-separated list names a mechanism.
func offersMechanism(list []byte, want string) bool {
	for len(list) > 0 {
		name, rest, ok := cutCString(list)
		if !ok || name == "" {
			return false
		}
		if name == want {
			return true
		}
		list = rest
	}
	return false
}

// md5Response is PostgreSQL's MD5 password exchange: the stored hash, hashed
// again with a salt the server chose for this connection.
func md5Response(user, password string, salt []byte) string {
	inner := md5.Sum([]byte(password + user))
	outer := md5.Sum(append([]byte(hex.EncodeToString(inner[:])), salt...))
	return "md5" + hex.EncodeToString(outer[:])
}

// cancel asks the server to cancel whatever the given backend is running. The
// protocol wants it on a connection of its own, which is closed unanswered.
func (u *upstream) cancel(ctx context.Context, pid uint32, secret []byte) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, u.network, u.address)
	if err != nil {
		return err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	request := binary.BigEndian.AppendUint32(nil, uint32(12+len(secret)))
	request = binary.BigEndian.AppendUint32(request, cancelRequestCode)
	request = binary.BigEndian.AppendUint32(request, pid)
	_, err = conn.Write(append(request, secret...))
	return err
}
