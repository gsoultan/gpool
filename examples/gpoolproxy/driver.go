// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"

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

	// maxPrepared bounds the prepared statements each connection accumulates.
	maxPrepared int

	// socks is where a new connection's descriptor is recorded, so the event
	// loops can find it.
	socks *sockTable

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
	return d.dial(ctx, d.config.Copy())
}

// Replicate establishes a connection in replication mode, for one client and
// outside the pool.
//
// mode is the client's own replication parameter, passed through: "database"
// for logical replication, a boolean for physical. The server decides what it
// allows, as it would have had the client connected directly.
func (d *backendDriver) Replicate(ctx context.Context, mode string) (*backend, error) {
	config := d.config.Copy()
	if config.RuntimeParams == nil {
		config.RuntimeParams = map[string]string{}
	}
	config.RuntimeParams["replication"] = mode
	return d.dial(ctx, config)
}

func (d *backendDriver) dial(ctx context.Context, config *pgconn.Config) (*backend, error) {
	conn, err := pgconn.ConnectConfig(ctx, config)
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

	// From here the connection is a descriptor. pgconn did the part that is worth
	// not reimplementing; what remains is moving bytes, and an event loop does
	// that without a goroutine parked on every socket.
	fd, err := takeOver(hijacked.Conn)
	if err != nil {
		_ = hijacked.Conn.Close()
		return nil, err
	}

	b := &backend{
		pid:      hijacked.PID,
		secret:   hijacked.SecretKey,
		txStatus: txIdle,
		prepared: newStatements(d.maxPrepared),
	}
	b.sock.fd = fd
	b.sock.backend = b

	if !d.socks.put(&b.sock) {
		_ = syscall.Close(fd)
		return nil, errTooManyDescriptors
	}
	return b, nil
}

// Close terminates a connection, asking politely first so the server records a
// clean disconnect rather than an aborted one.
//
// The request is five bytes written to a non-blocking socket. If the kernel will
// not take even that, the connection is in no state to be asked, and closing it
// is the answer either way — so this cannot block whoever is discarding it.
func (d *backendDriver) Close(_ context.Context, b *backend) error {
	if !b.broken.Load() {
		_, _ = syscall.Write(b.sock.fd, terminate)
	}

	// Forgotten before it is closed, so the number cannot be reused while the
	// table still points at this connection. Closing also withdraws it from the
	// poller.
	d.socks.drop(&b.sock)
	return syscall.Close(b.sock.fd)
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

// cancel opens a fresh connection and asks the server to cancel whatever the
// backend with this key is running. PostgreSQL requires the request on a separate connection,
// because the one being cancelled is by definition busy.
func (d *backendDriver) cancel(ctx context.Context, pid uint32, secret []byte) error {
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

	encoded, err := (&pgproto3.CancelRequest{ProcessID: pid, SecretKey: secret}).Encode(nil)
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
