// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"time"
)

// backend is one connection to the real PostgreSQL server, held at the raw
// protocol level so the proxy can move bytes between sockets without decoding
// them.
//
// It is a bare descriptor rather than a net.Conn. While a session holds it, the
// session's event loop reads and writes it without blocking; between sessions
// nothing reads it at all.
type backend struct {
	sock sock

	// pid and secret are the server's own cancellation key for this connection.
	// A client's CancelRequest arrives on a separate connection carrying the
	// proxy's key, and has to be translated into these before the server will
	// act on it.
	pid    uint32
	secret []byte

	// txStatus is the transaction status last reported by ReadyForQuery. It is
	// what decides both when a backend may be handed to another client and, on
	// release, whether anything needs rolling back.
	txStatus byte

	// prepared is the Parse message behind each named prepared statement that
	// currently exists on this connection.
	//
	// It outlives the session that created it, because the statements really do:
	// nothing deallocates them when a client goes away. Keeping the message
	// rather than just the name is what lets a later client's identically named
	// but different statement be detected instead of silently executed.
	//
	// That nothing deallocates them is also why this is bounded. A connection
	// serves client after client for the life of the pool, accumulating a
	// statement per distinct name every one of them used; the ceiling here is on
	// the server's memory as much as on the proxy's, which is why evicting from
	// it sends a real Close rather than only forgetting.
	prepared *statements

	// listed records that the descriptor is registered with the poller. Like
	// txStatus it belongs to whoever has the connection checked out.
	listed bool

	broken atomic.Bool
}

// fail marks the connection unusable. Anything that interrupts the protocol
// mid-message leaves the stream unparseable, and no amount of cleanup recovers
// it — the pool has to throw the connection away.
func (b *backend) fail() {
	b.broken.Store(true)
}

// dirty reports whether returning this connection to the pool will cost I/O: a
// rollback if a transaction was left open, a close if it is broken.
func (b *backend) dirty() bool {
	return b.broken.Load() || b.txStatus != txIdle
}

// enlist registers the connection with the poller, if it is not already.
//
// The registration is left in place when the connection goes back to the pool,
// so in the steady state this does nothing: a backend is registered once in its
// life and then passed between clients without a system call.
func (b *backend) enlist(p *poller) error {
	if b.listed {
		return nil
	}
	if err := p.add(b.sock.fd); err != nil {
		return err
	}
	b.listed = true
	return nil
}

// delist withdraws the connection from the poller, for the one operation that
// talks to the server with blocking I/O from another goroutine. A reply arriving
// while it was listed would look like an idle connection speaking out of turn.
func (b *backend) delist(p *poller) {
	if b.listed {
		p.remove(b.sock.fd)
		b.listed = false
	}
}

// rollback ends whatever transaction the previous client left open.
//
// It is the one place a pooled connection is used with blocking I/O, because it
// runs on the pool's release path, off any event loop, and a deadline on a
// blocking socket is the simplest bounded round trip there is.
func (b *backend) rollback(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultCleanupTimeout)
	}

	if err := b.setBlocking(time.Until(deadline)); err != nil {
		return err
	}
	if err := writeAll(b.sock.fd, simpleQuery("ROLLBACK")); err != nil {
		return err
	}
	if err := b.drainToReady(); err != nil {
		return err
	}
	return b.setNonblocking()
}

// setBlocking switches the socket to blocking mode with a timeout on each read
// and write.
func (b *backend) setBlocking(timeout time.Duration) error {
	if timeout <= 0 {
		return context.DeadlineExceeded
	}
	limit := syscall.NsecToTimeval(timeout.Nanoseconds())

	if err := syscall.SetNonblock(b.sock.fd, false); err != nil {
		return err
	}
	if err := syscall.SetsockoptTimeval(b.sock.fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &limit); err != nil {
		return err
	}
	return syscall.SetsockoptTimeval(b.sock.fd, syscall.SOL_SOCKET, syscall.SO_SNDTIMEO, &limit)
}

// setNonblocking undoes setBlocking, returning the socket to the state an event
// loop needs it in.
func (b *backend) setNonblocking() error {
	var none syscall.Timeval

	if err := syscall.SetsockoptTimeval(b.sock.fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &none); err != nil {
		return err
	}
	if err := syscall.SetsockoptTimeval(b.sock.fd, syscall.SOL_SOCKET, syscall.SO_SNDTIMEO, &none); err != nil {
		return err
	}
	return syscall.SetNonblock(b.sock.fd, true)
}

// drainToReady discards server messages until the next ReadyForQuery, recording
// the transaction status it carries. The socket must be in blocking mode.
func (b *backend) drainToReady() error {
	var buf [512]byte
	var header [headerSize]byte
	held, skip := 0, 0

	for {
		n, err := syscall.Read(b.sock.fd, buf[:])
		switch {
		case err == syscall.EINTR:
			continue
		case err != nil:
			// A timeout surfaces here as EAGAIN.
			return err
		case n == 0:
			return errors.New("gpoolproxy: server closed the connection during rollback")
		}

		for data := buf[:n]; len(data) > 0; {
			if skip > 0 {
				step := min(skip, len(data))
				data, skip = data[step:], skip-step
				continue
			}

			taken := copy(header[held:], data)
			data, held = data[taken:], held+taken
			if held < headerSize {
				continue
			}
			held = 0

			bodyLen := int(binary.BigEndian.Uint32(header[1:])) - 4
			if bodyLen < 0 || bodyLen > maxMessageLen {
				return errors.New("gpoolproxy: malformed message while draining")
			}
			if header[0] != msgReadyForQuery || bodyLen != 1 {
				skip = bodyLen
				continue
			}

			// The status byte may be the first of the next read.
			status, err := firstByte(b.sock.fd, data)
			if err != nil {
				return err
			}
			b.txStatus = status
			if status != txIdle {
				return fmt.Errorf("gpoolproxy: transaction status %q after rollback", status)
			}
			return nil
		}
	}
}

// firstByte returns the first byte of data, reading one from a blocking socket
// if data is empty.
func firstByte(fd int, data []byte) (byte, error) {
	if len(data) > 0 {
		return data[0], nil
	}

	var one [1]byte
	for {
		n, err := syscall.Read(fd, one[:])
		switch {
		case err == syscall.EINTR:
			continue
		case err != nil:
			return 0, err
		case n == 0:
			return 0, errors.New("gpoolproxy: server closed the connection during rollback")
		}
		return one[0], nil
	}
}

// writeAll writes a whole message to a blocking socket.
func writeAll(fd int, message []byte) error {
	for len(message) > 0 {
		n, err := syscall.Write(fd, message)
		switch {
		case err == syscall.EINTR:
			continue
		case err != nil:
			return err
		}
		message = message[n:]
	}
	return nil
}

// simpleQuery builds a Query message. Encoded by hand for the same reason as
// closeStatement: it is a fixed shape, and the steady state of this proxy has no
// other use for an encoder.
func simpleQuery(sql string) []byte {
	message := make([]byte, 0, headerSize+len(sql)+1)
	message = append(message, msgQuery)
	message = binary.BigEndian.AppendUint32(message, uint32(len(sql)+5))
	message = append(message, sql...)
	return append(message, 0)
}

// terminate is the message that asks the server for a clean disconnect.
var terminate = []byte{msgTerminate, 0, 0, 0, 4}

// upstreamUnavailable is what a client is told when the proxy could not get it a
// server connection. It names no host and repeats no error text, because the
// reason may include either and the client is not who should be told.
var upstreamUnavailable = errorResponse("FATAL", "08006", "the server is unavailable")

// errorResponse builds an ErrorResponse with the three fields every client
// expects.
func errorResponse(severity, code, text string) []byte {
	body := make([]byte, 0, 32+len(text))
	for _, field := range []struct {
		kind  byte
		value string
	}{{'S', severity}, {'V', severity}, {'C', code}, {'M', text}} {
		body = append(body, field.kind)
		body = append(body, field.value...)
		body = append(body, 0)
	}
	body = append(body, 0)

	message := make([]byte, 0, headerSize+len(body))
	message = append(message, 'E')
	message = binary.BigEndian.AppendUint32(message, uint32(len(body)+4))
	return append(message, body...)
}
