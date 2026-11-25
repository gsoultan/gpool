// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"context"
	"net"
	"sync/atomic"
	"time"
)

// initialReadBuffer is what a connection reads through until a message needs
// more. Everything per connection is multiplied by the pool size, so it starts
// small and grows only for the connection that receives a large row.
const initialReadBuffer = 8 << 10

// shrinkAbove is the size past which a buffer grown for one large message is
// let go once that message has been consumed, so a single wide row does not pin
// its size for the connection's lifetime.
const shrinkAbove = 256 << 10

// Conn is one connection to PostgreSQL after login.
//
// It is owned by one goroutine at a time; only IsClosed and CancelRequest may be
// called from another.
type Conn struct {
	netConn net.Conn
	config  *Config

	// rbuf[rpos:rend] is received but not yet consumed. A message is only ever
	// consumed whole, so a read interrupted by a deadline leaves the stream
	// exactly where it was and the connection usable.
	rbuf       []byte
	rpos, rend int
	// wbuf is lent to the caller by Buffer and returned by Send, so encoding a
	// query allocates nothing once the connection has warmed up.
	wbuf []byte

	pid      uint32
	secret   []byte
	params   map[string]string
	txStatus byte

	notifications []Notification

	closed atomic.Bool
}

// PID returns the server process serving this connection.
func (c *Conn) PID() uint32 { return c.pid }

// TxStatus returns the transaction status from the last ReadyForQuery: TxIdle,
// TxActive or TxFailed.
func (c *Conn) TxStatus() byte { return c.txStatus }

// ParameterStatus returns a setting the server reported, such as server_version
// or standard_conforming_strings, or the empty string.
func (c *Conn) ParameterStatus(name string) string { return c.params[name] }

// IsClosed reports whether the connection can no longer carry traffic. It does
// no I/O. Safe from any goroutine.
func (c *Conn) IsClosed() bool { return c.closed.Load() }

// Config returns the configuration the connection was opened with. It must not
// be modified.
func (c *Conn) Config() *Config { return c.config }

// NetConn returns the underlying network connection.
func (c *Conn) NetConn() net.Conn { return c.netConn }

// Buffer returns an empty slice to encode frontend messages into, reusing the
// connection's write buffer. Hand the result to Send.
func (c *Conn) Buffer() []byte { return c.wbuf[:0] }

// Send writes encoded messages. The slice becomes the connection's write buffer
// again, so it must not be used after Send returns.
func (c *Conn) Send(messages []byte) error {
	if c.closed.Load() {
		return ErrClosed
	}
	_, err := c.netConn.Write(messages)
	if cap(messages) <= shrinkAbove {
		c.wbuf = messages[:0]
	} else {
		c.wbuf = nil
	}
	if err != nil {
		return c.fail(err)
	}
	return nil
}

// Notifications removes and returns the notifications received so far.
func (c *Conn) Notifications() []Notification {
	pending := c.notifications
	c.notifications = nil
	return pending
}

// Close sends Terminate and closes the connection. It is idempotent, and it is
// bounded by ctx and by a short grace period whatever ctx says, so a dead server
// cannot hold up shutdown.
func (c *Conn) Close(ctx context.Context) error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	deadline := time.Now().Add(closeGrace)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.netConn.SetWriteDeadline(deadline)
	_, _ = c.netConn.Write(AppendTerminate(nil))
	return c.netConn.Close()
}

// closeGrace bounds the Terminate sent on Close.
const closeGrace = time.Second

// fail closes the connection after an error that leaves it unusable, and
// returns the error.
func (c *Conn) fail(err error) error {
	if c.closed.CompareAndSwap(false, true) {
		_ = c.netConn.Close()
	}
	return err
}
