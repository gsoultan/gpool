// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"encoding/binary"
	"errors"
	"net"
)

// Receive returns the next message, handling the asynchronous ones on the way:
// a ParameterStatus is recorded, a NotificationResponse queued, a notice dropped.
//
// The body aliases the connection's buffer and is valid only until the next
// call. An ErrorResponse is returned as a message, for the caller to decide what
// it ends; use ParseError to read it.
//
// A timeout leaves the connection intact, and the same message is returned by
// the next call. Any other failure closes it.
func (c *Conn) Receive() (byte, []byte, error) {
	for {
		kind, body, err := c.next()
		if err != nil {
			return 0, nil, err
		}
		switch kind {
		case MsgParameterStatus:
			c.recordParameter(body)
		case MsgNotification:
			if note, ok := parseNotification(body); ok {
				c.notifications = append(c.notifications, note)
			}
		case MsgNoticeResponse:
		case MsgReadyForQuery:
			if len(body) != 1 {
				return 0, nil, c.fail(ErrProtocol)
			}
			c.txStatus = body[0]
			return kind, body, nil
		default:
			return kind, body, nil
		}
	}
}

// ParseError reads the body of an ErrorResponse.
func ParseError(body []byte) *Error {
	return parseError(body)
}

func (c *Conn) recordParameter(body []byte) {
	name, rest, ok := cutString(body)
	if !ok {
		return
	}
	value, _, ok := cutString(rest)
	if ok {
		c.params[name] = value
	}
}

// next reads one whole message into the buffer and consumes it.
func (c *Conn) next() (byte, []byte, error) {
	if c.closed.Load() {
		return 0, nil, ErrClosed
	}
	if err := c.fill(headerSize); err != nil {
		return 0, nil, err
	}
	header := c.rbuf[c.rpos:]
	length := int(binary.BigEndian.Uint32(header[1:5])) - 4
	if length < 0 {
		return 0, nil, c.fail(ErrProtocol)
	}
	if length > c.config.maxMessage() {
		return 0, nil, c.fail(ErrMessageTooLarge)
	}
	if err := c.fill(headerSize + length); err != nil {
		return 0, nil, err
	}
	kind := c.rbuf[c.rpos]
	body := c.rbuf[c.rpos+headerSize : c.rpos+headerSize+length : c.rpos+headerSize+length]
	c.rpos += headerSize + length
	return kind, body, nil
}

// fill makes sure at least n unconsumed bytes are buffered.
func (c *Conn) fill(n int) error {
	if c.rend-c.rpos >= n {
		return nil
	}
	c.makeRoom(n)
	for c.rend-c.rpos < n {
		read, err := c.netConn.Read(c.rbuf[c.rend:])
		c.rend += read
		if err == nil {
			continue
		}
		if c.rend-c.rpos >= n {
			return nil
		}
		if isTimeout(err) {
			return err
		}
		return c.fail(err)
	}
	return nil
}

// makeRoom moves the unconsumed bytes to the front of the buffer and grows it
// if n of them would still not fit. A buffer grown past shrinkAbove for a
// message that is now gone is let go first.
func (c *Conn) makeRoom(n int) {
	pending := c.rend - c.rpos
	if pending == 0 && len(c.rbuf) > shrinkAbove && n <= initialReadBuffer {
		c.rbuf = make([]byte, initialReadBuffer)
	}
	if len(c.rbuf) < n {
		grown := make([]byte, max(n, 2*len(c.rbuf)))
		copy(grown, c.rbuf[c.rpos:c.rend])
		c.rbuf, c.rpos, c.rend = grown, 0, pending
		return
	}
	if len(c.rbuf)-c.rpos < n || c.rpos == c.rend {
		copy(c.rbuf, c.rbuf[c.rpos:c.rend])
		c.rpos, c.rend = 0, pending
	}
}

// isTimeout reports whether err is a deadline expiring.
func isTimeout(err error) bool {
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

// IsTimeout reports whether err is a read or write deadline expiring, which
// leaves a connection usable.
func IsTimeout(err error) bool {
	return isTimeout(err)
}
