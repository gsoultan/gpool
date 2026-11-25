// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"context"
	"errors"
)

// ErrNoCopyBoth is a command that finished without entering CopyBoth mode.
var ErrNoCopyBoth = errors.New("gpool/postgres: the command did not start a copy in both directions")

// StartCopyBoth sends a command that switches the connection into CopyBoth
// mode, as START_REPLICATION does, and returns once the server has. From then
// on the server sends CopyData, read with Receive, and the client answers with
// CopyData of its own; no other command is legal until the copy ends.
//
// A server error is read through to ReadyForQuery and returned, leaving the
// connection usable.
func (c *Conn) StartCopyBoth(ctx context.Context, sql string) error {
	guard := c.Watch(ctx)
	return guard.Done(c.startCopyBoth(sql))
}

func (c *Conn) startCopyBoth(sql string) error {
	if err := c.Send(AppendQuery(c.Buffer(), sql)); err != nil {
		return err
	}
	var failure error
	for {
		kind, body, err := c.Receive()
		if err != nil {
			return err
		}
		switch kind {
		case MsgCopyBothResponse:
			return nil
		case MsgErrorResponse:
			if failure == nil {
				failure = parseError(body)
			}
		case MsgReadyForQuery:
			if failure == nil {
				failure = ErrNoCopyBoth
			}
			return failure
		}
	}
}
