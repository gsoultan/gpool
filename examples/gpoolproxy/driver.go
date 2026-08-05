// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// backendDriver dials PostgreSQL for pooling.Core.
//
// That the engine can be driven by something which is not a database driver at
// all — no rows, no queries, just a socket — is the point of it being generic
// over the connection type rather than over an interface.
type backendDriver struct {
	config *pgconn.Config

	// params holds the ParameterStatus set the server reported, captured from a
	// real connection and replayed to clients during their own startup. A client
	// is entitled to server_version and friends before it will issue a query,
	// and inventing them would be a lie the proxy cannot keep consistent.
	params atomic.Pointer[map[string]string]
}

var _ interface {
	Connect(context.Context) (*backend, error)
} = (*backendDriver)(nil)

// Connect establishes one connection and takes it over at the byte level.
//
// pgconn does the handshake — TLS negotiation, SCRAM, parameter collection — and
// Hijack then surrenders the socket. Reimplementing that handshake to own the
// socket from the first byte would mean a second, less tested copy of it.
func (d *backendDriver) Connect(ctx context.Context) (*backend, error) {
	conn, err := pgconn.ConnectConfig(ctx, d.config.Copy())
	if err != nil {
		return nil, err
	}

	hijacked, err := conn.Hijack()
	if err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}

	// Taking the socket over is only sound if pgconn's own reader has nothing
	// left in it. After ReadyForQuery the server is silent, so this should always
	// hold; asserting it means a future pgx that reads ahead fails loudly here
	// rather than by silently dropping a message into a client's result set.
	if buffered := hijacked.Frontend.ReadBufferLen(); buffered != 0 {
		_ = hijacked.Conn.Close()
		return nil, fmt.Errorf("gpoolproxy: %d bytes buffered after handshake, cannot take over the connection", buffered)
	}

	if d.params.Load() == nil {
		statuses := make(map[string]string, len(hijacked.ParameterStatuses))
		for name, value := range hijacked.ParameterStatuses {
			statuses[name] = value
		}
		d.params.CompareAndSwap(nil, &statuses)
	}

	return &backend{
		conn:     hijacked.Conn,
		reader:   bufio.NewReaderSize(hijacked.Conn, backendBufSize),
		writer:   bufio.NewWriterSize(hijacked.Conn, backendBufSize),
		pid:      hijacked.PID,
		secret:   hijacked.SecretKey,
		txStatus: txIdle,
	}, nil
}

// Close terminates a connection, asking politely first so the server records a
// clean disconnect rather than an aborted one.
func (d *backendDriver) Close(ctx context.Context, b *backend) error {
	defer b.conn.Close()

	if b.broken.Load() {
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = b.conn.SetDeadline(deadline)
	}

	encoded, err := (&pgproto3.Terminate{}).Encode(nil)
	if err != nil {
		return err
	}
	_, err = b.conn.Write(encoded)
	return err
}

// Dead reports a connection the relay could not leave in a parseable state.
func (d *backendDriver) Dead(b *backend) bool {
	return b.broken.Load()
}

// NeedsCleanup is true only when the previous client left a transaction open.
// The common release — a client that ran its statement and got its
// ReadyForQuery('I') — does no I/O and builds no deadline context.
func (d *backendDriver) NeedsCleanup(b *backend) bool {
	return b.txStatus != txIdle
}

// Recyclable rolls back whatever the previous client left open.
//
// This is the whole pooling contract in one method: a client that disconnects
// mid-transaction must not hand the next one its locks, its snapshot, or a
// session that rejects every statement with 25P02. Rolling back costs one round
// trip; discarding the connection costs a full reconnect and a server backend.
func (d *backendDriver) Recyclable(ctx context.Context, b *backend) bool {
	if err := b.rollback(ctx); err != nil {
		b.fail()
		return false
	}
	return true
}

// cancel opens a fresh connection and asks the server to cancel whatever this
// backend is running. PostgreSQL requires the request on a separate connection,
// because the one being cancelled is by definition busy.
func (d *backendDriver) cancel(ctx context.Context, b *backend) error {
	dialer := &net.Dialer{}
	network, address := networkAddress(d.config)

	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	encoded, err := (&pgproto3.CancelRequest{ProcessID: b.pid, SecretKey: b.secret}).Encode(nil)
	if err != nil {
		return err
	}
	_, err = conn.Write(encoded)
	return err
}

// networkAddress renders a pgconn.Config's host and port as a dial target,
// distinguishing a Unix socket directory from a TCP host.
func networkAddress(config *pgconn.Config) (network, address string) {
	if len(config.Host) > 0 && config.Host[0] == '/' {
		return "unix", fmt.Sprintf("%s/.s.PGSQL.%d", config.Host, config.Port)
	}
	return "tcp", net.JoinHostPort(config.Host, fmt.Sprint(config.Port))
}
