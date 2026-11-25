// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"context"
	"time"
)

// WaitForNotification returns the next notification, waiting for one to arrive
// on an idle connection until ctx ends.
//
// Unlike an exchange, the wait can end without harm: an idle connection is sent
// nothing but asynchronous messages, and the read buffer only ever consumes
// whole ones, so a context that ends leaves the connection usable.
func (c *Conn) WaitForNotification(ctx context.Context) (Notification, error) {
	if note, ok := c.popNotification(); ok {
		return note, nil
	}
	if err := ctx.Err(); err != nil {
		return Notification{}, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.netConn.SetReadDeadline(deadline)
	}
	netConn := c.netConn
	stop := context.AfterFunc(ctx, func() { _ = netConn.SetReadDeadline(time.Unix(1, 0)) })
	defer func() {
		stop()
		if !c.closed.Load() {
			_ = c.netConn.SetReadDeadline(time.Time{})
		}
	}()

	for {
		kind, body, err := c.next()
		if err != nil {
			if isTimeout(err) {
				awaitExpiry(ctx)
			}
			if isTimeout(err) && ctx.Err() != nil {
				return Notification{}, context.Cause(ctx)
			}
			if isTimeout(err) {
				// The deadline passed in the instant before ctx says so.
				return Notification{}, context.DeadlineExceeded
			}
			return Notification{}, err
		}
		switch kind {
		case MsgNotification:
			if note, ok := parseNotification(body); ok {
				return note, nil
			}
		case MsgParameterStatus:
			c.recordParameter(body)
		case MsgErrorResponse:
			// A FATAL sent to an idle session, such as an administrator's
			// pg_terminate_backend. The server closes the connection after it.
			return Notification{}, c.fail(ParseError(body))
		case MsgNoticeResponse:
		default:
			return Notification{}, c.fail(ErrProtocol)
		}
	}
}

// popNotification removes the oldest queued notification.
func (c *Conn) popNotification() (Notification, bool) {
	if len(c.notifications) == 0 {
		return Notification{}, false
	}
	note := c.notifications[0]
	c.notifications = c.notifications[1:]
	if len(c.notifications) == 0 {
		c.notifications = nil
	}
	return note, true
}
