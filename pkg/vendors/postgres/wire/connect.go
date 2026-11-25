// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sort"
	"time"
)

// Authentication request codes, the first four bytes of an 'R' message.
const (
	authOK           = 0
	authCleartext    = 3
	authMD5          = 5
	authSASL         = 10
	authSASLContinue = 11
	authSASLFinal    = 12
)

const scramMechanism = "SCRAM-SHA-256"

// maxLoginMessages bounds the exchange before ReadyForQuery, so a server that
// never finishes logging in cannot keep a dial going for ever on a context
// with no deadline.
const maxLoginMessages = 1024

// Connect dials the server described by config and logs in.
//
// With sslmode=prefer, a server that declines TLS or fails the handshake is
// dialled again in plaintext, as libpq does.
func Connect(ctx context.Context, config *Config) (*Conn, error) {
	if config.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.ConnectTimeout)
		defer cancel()
	}

	conn, err := connect(ctx, config, config.TLSConfig)
	if err == nil || config.TLSConfig == nil || !config.TLSFallback || !fallbackWorthy(err) {
		return conn, err
	}
	return connect(ctx, config, nil)
}

// fallbackWorthy reports whether a failure was TLS's rather than the server's
// verdict on the login, which is the only kind a plaintext retry could cure.
func fallbackWorthy(err error) bool {
	var serverErr *Error
	return !errors.As(err, &serverErr) && ctxErr(err) == nil
}

func ctxErr(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func connect(ctx context.Context, config *Config, tlsConfig *tls.Config) (*Conn, error) {
	network, address := config.target()
	netConn, err := (&net.Dialer{KeepAlive: 5 * time.Minute}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	// A context ending mid-login has to interrupt a read already blocked, and a
	// deadline in the past is how a connection is told to.
	stop := context.AfterFunc(ctx, func() { _ = netConn.SetDeadline(time.Unix(1, 0)) })
	if deadline, ok := ctx.Deadline(); ok {
		_ = netConn.SetDeadline(deadline)
	}

	c := &Conn{
		netConn: netConn,
		config:  config,
		rbuf:    make([]byte, initialReadBuffer),
		params:  make(map[string]string, 16),
	}
	err = c.login(ctx, tlsConfig)
	interrupted := !stop()
	if err == nil && interrupted {
		// Logged in, but the deadline was already moved into the past.
		err = context.Cause(ctx)
	}
	if err != nil {
		_ = c.netConn.Close()
		if isTimeout(err) {
			awaitExpiry(ctx)
		}
		if ctx.Err() != nil && ctxErr(err) == nil {
			err = errors.Join(context.Cause(ctx), err)
		}
		return nil, err
	}
	_ = c.netConn.SetDeadline(time.Time{})
	return c, nil
}

func (c *Conn) login(ctx context.Context, tlsConfig *tls.Config) error {
	if tlsConfig != nil {
		if err := c.negotiateTLS(ctx, tlsConfig); err != nil {
			return err
		}
	}
	if _, err := c.netConn.Write(c.startupMessage()); err != nil {
		return err
	}

	auth := authenticator{conn: c}
	for range maxLoginMessages {
		kind, body, err := c.Receive()
		if err != nil {
			return err
		}
		switch kind {
		case MsgAuthentication:
			if err := auth.handle(body); err != nil {
				return err
			}
		case MsgBackendKeyData:
			if len(body) < 8 {
				return ErrProtocol
			}
			c.pid, c.secret = be32(body), append([]byte(nil), body[4:]...)
		case MsgErrorResponse:
			return parseError(body)
		case MsgReadyForQuery:
			return auth.finish()
		case MsgNegotiateProtocol:
			// The server speaks an older minor version than asked for; 3.0 is
			// all this asks for, so there is nothing to negotiate down from.
		default:
			return ErrProtocol
		}
	}
	return ErrProtocol
}

// negotiateTLS asks for encryption before anything else is sent, so that no
// credential and no setting crosses in the clear when encryption is available.
func (c *Conn) negotiateTLS(ctx context.Context, config *tls.Config) error {
	request := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 8), sslRequestCode)
	if _, err := c.netConn.Write(request); err != nil {
		return err
	}
	// Exactly one byte, from the connection itself: anything buffered here could
	// belong to the TLS handshake.
	var answer [1]byte
	if _, err := io.ReadFull(c.netConn, answer[:]); err != nil {
		return err
	}
	switch answer[0] {
	case 'S':
		secured := tls.Client(c.netConn, config)
		if err := secured.HandshakeContext(ctx); err != nil {
			return err
		}
		c.netConn = secured
		return nil
	case 'N':
		return ErrTLSRefused
	}
	return ErrProtocol
}

func (c *Conn) startupMessage() []byte {
	body := binary.BigEndian.AppendUint32(make([]byte, 4, 256), protocolVersion30)
	body = AppendString(AppendString(body, "user"), c.config.User)
	body = AppendString(AppendString(body, "database"), c.config.Database)

	// In a fixed order, so two connections never differ by map iteration.
	names := make([]string, 0, len(c.config.RuntimeParams))
	for name := range c.config.RuntimeParams {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		body = AppendString(AppendString(body, name), c.config.RuntimeParams[name])
	}
	body = append(body, 0)
	binary.BigEndian.PutUint32(body, uint32(len(body)))
	return body
}

// authenticator answers the server's authentication requests.
type authenticator struct {
	conn     *Conn
	scram    *scramClient
	verified bool
}

func (a *authenticator) handle(body []byte) error {
	if len(body) < 4 {
		return ErrProtocol
	}
	code, data := be32(body), body[4:]
	switch code {
	case authOK:
		return nil
	case authCleartext:
		return a.send(AppendString(nil, a.conn.config.Password))
	case authMD5:
		if len(data) != 4 {
			return ErrProtocol
		}
		return a.send(AppendString(nil, md5Response(a.conn.config.User, a.conn.config.Password, data)))
	case authSASL:
		return a.beginSCRAM(data)
	case authSASLContinue:
		return a.continueSCRAM(data)
	case authSASLFinal:
		return a.finishSCRAM(data)
	}
	return ErrAuthMethod
}

// finish checks the login ended in a state the connection can be used from.
// A server that began SCRAM and then declared success without proving it knew
// the verifier has not authenticated itself.
func (a *authenticator) finish() error {
	if a.scram != nil && !a.verified {
		return ErrSCRAM
	}
	return nil
}

func (a *authenticator) send(body []byte) error {
	message, mark := StartMessage(nil, 'p')
	_, err := a.conn.netConn.Write(FinishMessage(append(message, body...), mark))
	return err
}

func (a *authenticator) beginSCRAM(mechanisms []byte) error {
	if !offersMechanism(mechanisms, scramMechanism) {
		return ErrAuthMethod
	}
	a.scram = &scramClient{password: a.conn.config.Password}
	first, err := a.scram.first()
	if err != nil {
		return err
	}
	body := AppendString(nil, scramMechanism)
	body = AppendInt32(body, int32(len(first)))
	return a.send(append(body, first...))
}

func (a *authenticator) continueSCRAM(challenge []byte) error {
	if a.scram == nil {
		return ErrProtocol
	}
	final, err := a.scram.final(challenge)
	if err != nil {
		return err
	}
	return a.send(final)
}

func (a *authenticator) finishSCRAM(signature []byte) error {
	if a.scram == nil {
		return ErrProtocol
	}
	if err := a.scram.verify(signature); err != nil {
		return err
	}
	a.verified = true
	return nil
}

// offersMechanism reports whether a NUL-separated list names a mechanism.
func offersMechanism(list []byte, want string) bool {
	for len(list) > 0 {
		name, rest, ok := cutString(list)
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
