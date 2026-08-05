// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"fmt"

	"github.com/gsoultan/gpool/pkg/gpool"
)

var _ gpool.Notifier = (*connWrapper)(nil)

// unlistenAll cancels every subscription on a connection. It is what stops a
// subscription outliving the caller that registered it.
const unlistenAll = "UNLISTEN *"

// Listen subscribes this connection to a channel.
//
// The subscription belongs to the session, so the connection is marked as carrying
// session state: releasing it will run UNLISTEN * before it can serve anyone else.
func (c *connWrapper) Listen(ctx context.Context, channel string) error {
	if err := c.live(); err != nil {
		return err
	}
	if channel == "" {
		return fmt.Errorf("%w: channel is required", ErrInvalidConfig)
	}

	// Marked before the statement runs: a LISTEN that fails mid-flight may still
	// have registered, and cleaning up an unsubscribed connection is harmless
	// where missing one is not.
	c.conn().listening = true

	if _, err := c.pgx().Exec(ctx, "LISTEN "+quoteIdentifier(channel)); err != nil {
		return fmt.Errorf("gpool/postgres: listen on %q: %w", channel, err)
	}
	return nil
}

// Unlisten cancels one subscription, or all of them when channel is empty.
func (c *connWrapper) Unlisten(ctx context.Context, channel string) error {
	if err := c.live(); err != nil {
		return err
	}

	statement := unlistenAll
	if channel != "" {
		statement = "UNLISTEN " + quoteIdentifier(channel)
	}

	if _, err := c.pgx().Exec(ctx, statement); err != nil {
		return fmt.Errorf("gpool/postgres: unlisten %q: %w", channel, err)
	}

	// Only a blanket unlisten is known to have cleared everything.
	if channel == "" {
		c.conn().listening = false
	}
	return nil
}

// Notify sends a notification. It needs no subscription, so any connection can send.
func (c *connWrapper) Notify(ctx context.Context, channel, payload string) error {
	if err := c.live(); err != nil {
		return err
	}
	if channel == "" {
		return fmt.Errorf("%w: channel is required", ErrInvalidConfig)
	}

	// pg_notify is the function form of NOTIFY, which takes the channel and
	// payload as bound parameters instead of as literals in the statement text.
	if _, err := c.pgx().Exec(ctx, "SELECT pg_notify($1, $2)", channel, payload); err != nil {
		return fmt.Errorf("gpool/postgres: notify %q: %w", channel, err)
	}
	return nil
}

// WaitForNotification blocks until a notification arrives or ctx ends.
func (c *connWrapper) WaitForNotification(ctx context.Context) (gpool.Notification, error) {
	if err := c.live(); err != nil {
		return gpool.Notification{}, err
	}

	notification, err := c.pgx().WaitForNotification(ctx)
	if err != nil {
		return gpool.Notification{}, err
	}
	return gpool.Notification{
		Channel: notification.Channel,
		Payload: notification.Payload,
		PID:     notification.PID,
	}, nil
}

// quoteIdentifier renders s as a quoted SQL identifier. LISTEN and UNLISTEN take a
// bare channel name rather than a parameter, so the name has to be quoted here.
func quoteIdentifier(s string) string {
	quoted := make([]byte, 0, len(s)+2)
	quoted = append(quoted, '"')
	for i := range len(s) {
		if s[i] == '"' {
			quoted = append(quoted, '"')
		}
		quoted = append(quoted, s[i])
	}
	return string(append(quoted, '"'))
}
