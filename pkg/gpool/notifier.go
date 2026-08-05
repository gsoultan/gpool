// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"context"
)

// Notification is one asynchronous message delivered by LISTEN/NOTIFY.
type Notification struct {
	// Channel is the name the notification was sent on.
	Channel string
	// Payload is the message body, empty when NOTIFY was called without one.
	Payload string
	// PID is the backend process that sent it, which identifies self-notifications.
	PID uint32
}

// Notifier is PostgreSQL's LISTEN/NOTIFY, an optional capability on Conn.
//
// It is deliberately absent from Pool. A subscription belongs to the session that
// registered it, so it only means anything on a connection you continue to hold —
// a pool-level Listen would register on whichever connection happened to serve the
// call and then hand that connection to someone else.
//
// gpool tracks that a connection has listened and issues UNLISTEN ALL before
// returning it to the pool, so a subscription cannot leak to the next caller.
// That costs one round trip, on release, only for connections that actually listened.
//
//	conn, _ := pool.Acquire(ctx)
//	defer conn.Release()
//
//	listener, ok := conn.(gpool.Notifier)
//	if !ok {
//	    return errors.New("vendor does not support LISTEN/NOTIFY")
//	}
//	if err := listener.Listen(ctx, "events"); err != nil {
//	    return err
//	}
//	for {
//	    notification, err := listener.WaitForNotification(ctx)
//	    if err != nil {
//	        return err
//	    }
//	    handle(notification)
//	}
type Notifier interface {
	// Listen subscribes this connection to a channel.
	Listen(ctx context.Context, channel string) error
	// Unlisten cancels one subscription. Passing an empty channel cancels all of them.
	Unlisten(ctx context.Context, channel string) error
	// Notify sends a notification. Unlike the others this needs no subscription,
	// so any connection can send.
	Notify(ctx context.Context, channel, payload string) error
	// WaitForNotification blocks until a notification arrives or ctx ends.
	WaitForNotification(ctx context.Context) (Notification, error)
}
