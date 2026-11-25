// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"context"
	"sync/atomic"
	"syscall"
)

// backendDriver dials PostgreSQL for pooling.Core.
//
// That the engine can be driven by something which is not a database driver at
// all — no rows, no queries, just a socket — is the point of it being generic
// over the connection type rather than over an interface.
type backendDriver struct {
	upstream *upstream

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
func (d *backendDriver) Connect(ctx context.Context) (*backend, error) {
	return d.dial(ctx)
}

// Replicate establishes a connection in replication mode, for one client and
// outside the pool.
//
// mode is the client's own replication parameter, passed through: "database"
// for logical replication, a boolean for physical. The server decides what it
// allows, as it would have had the client connected directly.
func (d *backendDriver) Replicate(ctx context.Context, mode string) (*backend, error) {
	return d.dial(ctx, [2]string{"replication", mode})
}

func (d *backendDriver) dial(ctx context.Context, extra ...[2]string) (*backend, error) {
	established, err := d.upstream.connect(ctx, extra...)
	if err != nil {
		return nil, err
	}

	if d.params.Load() == nil {
		d.params.CompareAndSwap(nil, &established.statuses)
	}

	// From here the connection is a descriptor. The login is over and what
	// remains is moving bytes, which an event loop does without a goroutine
	// parked on every socket.
	fd, err := takeOver(established.conn)
	if err != nil {
		_ = established.conn.Close()
		return nil, err
	}

	b := &backend{
		pid:      established.pid,
		secret:   established.secret,
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

// cancel asks the server to cancel what a backend is running.
func (d *backendDriver) cancel(ctx context.Context, pid uint32, secret []byte) error {
	return d.upstream.cancel(ctx, pid, secret)
}
