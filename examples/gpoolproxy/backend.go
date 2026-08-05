// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// backend is one connection to the real PostgreSQL server, held at the raw
// protocol level so the proxy can move bytes between sockets without decoding
// them.
type backend struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

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

	broken atomic.Bool
}

// fail marks the connection unusable. Anything that interrupts the protocol
// mid-message leaves the stream unparseable, and no amount of cleanup recovers
// it — the pool has to throw the connection away.
func (b *backend) fail() {
	b.broken.Store(true)
}

// interrupt unblocks a goroutine parked in a read on this connection, so a
// session whose client has gone can be torn down without waiting on the server.
func (b *backend) interrupt() {
	_ = b.conn.SetDeadline(time.Unix(0, 0))
}

func (b *backend) rollback(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultCleanupTimeout)
	}
	if err := b.conn.SetDeadline(deadline); err != nil {
		return err
	}
	defer b.conn.SetDeadline(time.Time{})

	encoded, err := (&pgproto3.Query{String: "ROLLBACK"}).Encode(nil)
	if err != nil {
		return err
	}
	if _, err := b.writer.Write(encoded); err != nil {
		return err
	}
	if err := b.writer.Flush(); err != nil {
		return err
	}
	return b.drainToReady()
}

// drainToReady discards server messages until the next ReadyForQuery, recording
// the transaction status it carries.
func (b *backend) drainToReady() error {
	var header [headerSize]byte

	for {
		if _, err := io.ReadFull(b.reader, header[:]); err != nil {
			return err
		}
		bodyLen := int(uint32(header[1])<<24|uint32(header[2])<<16|uint32(header[3])<<8|uint32(header[4])) - 4
		if bodyLen < 0 || bodyLen > maxMessageLen {
			return errors.New("gpoolproxy: malformed message while draining")
		}

		if header[0] != msgReadyForQuery {
			if _, err := io.CopyN(io.Discard, b.reader, int64(bodyLen)); err != nil {
				return err
			}
			continue
		}

		status, err := b.reader.ReadByte()
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
