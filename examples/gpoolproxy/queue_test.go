// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	"github.com/gsoultan/nextar-engine/pkg/pooling"
)

// pairDriver hands the pool backends that are one end of a socket pair, so the
// queueing between sessions and the pool can be driven step by step with no
// PostgreSQL behind it. The test holds the other end and plays the server.
type pairDriver struct {
	socks *sockTable
	peers []int
}

var _ pooling.Driver[*backend] = (*pairDriver)(nil)

func (d *pairDriver) Connect(context.Context) (*backend, error) {
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}
	if err := syscall.SetNonblock(pair[0], true); err != nil {
		return nil, err
	}

	b := &backend{txStatus: txIdle, prepared: newStatements(0)}
	b.sock.fd = pair[0]
	b.sock.backend = b
	d.socks.put(&b.sock)
	d.peers = append(d.peers, pair[1])
	return b, nil
}

func (d *pairDriver) Close(_ context.Context, b *backend) error {
	d.socks.drop(&b.sock)
	return syscall.Close(b.sock.fd)
}

func (d *pairDriver) Dead(b *backend) bool         { return b.broken.Load() }
func (d *pairDriver) NeedsCleanup(b *backend) bool { return false }

func (d *pairDriver) Recyclable(context.Context, *backend) bool { return true }

// queueFixture is a proxy with one backend, no listener and no worker threads:
// the test is the only thing that moves.
type queueFixture struct {
	proxy  *Proxy
	driver *pairDriver
	worker *worker
}

func newQueueFixture(t *testing.T) *queueFixture {
	t.Helper()

	socks := newSockTable(descriptorLimit())
	driver := &pairDriver{socks: socks}
	core, err := pooling.New[*backend](driver, pooling.Config{MaxConns: 1})
	if err != nil {
		t.Fatalf("pooling.New() = %v", err)
	}
	poller, err := newPoller()
	if err != nil {
		t.Fatalf("newPoller() = %v", err)
	}

	proxy := &Proxy{core: core, socks: socks, poller: poller, clients: make(chan struct{}, 8), sessions: map[cancelKey]*session{}}
	proxy.ctx, proxy.stop = context.WithCancel(context.Background())

	// No worker threads means nobody to post to, and the pool is opened to its
	// ceiling up front so that nothing tries to dial in the background either.
	proxy.inboxClosed = true
	warm, err := core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	warm.Release()
	t.Cleanup(func() {
		proxy.stop()
		core.Close()
		poller.close()
		for _, peer := range driver.peers {
			_ = syscall.Close(peer)
		}
	})
	return &queueFixture{proxy: proxy, driver: driver, worker: newWorker(proxy)}
}

// client makes a session whose client has already sent the given bytes, and
// returns the test's end of its socket.
func (f *queueFixture) client(t *testing.T, sent []byte) (*session, int) {
	t.Helper()

	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("Socketpair() = %v", err)
	}
	if err := syscall.SetNonblock(pair[0], true); err != nil {
		t.Fatalf("SetNonblock() = %v", err)
	}
	t.Cleanup(func() { _ = syscall.Close(pair[1]) })

	s := &session{proxy: f.proxy}
	s.client.fd = pair[0]
	s.client.owner.Store(s)
	s.client.in = bytes.Clone(sent)
	s.stalled = true
	f.proxy.clients <- struct{}{}
	f.proxy.socks.put(&s.client)
	t.Cleanup(func() { f.worker.serve(s, s.close) })
	return s, pair[1]
}

// A session queued for a backend is claimed by whichever thread next releases
// one, and for a moment after that it is no longer waiting but does not yet hold
// anything. A session advanced in that moment — its client had sent more, or
// hung up — saw no backend and nothing to wait for, went looking, and queued
// itself a second time. When the connection it had been claimed for arrived it
// was attached to a session that believed it was still waiting, so the request
// was never sent; and the second place in the queue later brought a second
// backend, which overwrote the first and lost it to the pool for good.
//
// Under load this was a client that hung and a pool one connection short. Here
// the same steps are taken one at a time, because the window is otherwise a few
// instructions wide.
func TestSessionClaimedButNotYetGrantedDoesNotQueueAgain(t *testing.T) {
	f := newQueueFixture(t)
	query := simpleQuery("SELECT 1")

	// Every backend is out, so the session queues.
	holder, err := f.proxy.core.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	s, _ := f.client(t, query)
	f.worker.serve(s, s.advance)
	if !s.waiting.Load() || s.backend != nil {
		t.Fatalf("with no backend free the session should be queued: waiting=%v attached=%v", s.waiting.Load(), s.backend != nil)
	}

	// Another thread releases the backend and claims the session for it...
	holder.Release()
	handle, ok := f.proxy.core.TryAcquire()
	if !ok {
		t.Fatal("TryAcquire() after a release = miss")
	}
	if claimed := f.proxy.waiters.pop(); claimed != s {
		t.Fatalf("pop() = %p, want the queued session %p", claimed, s)
	}

	// ...and before it can deliver, the session is advanced for some other
	// reason.
	f.worker.serve(s, s.advance)
	if s.waiting.Load() {
		t.Fatal("a session already claimed for a backend queued itself again")
	}

	// The delivery.
	f.worker.grants = append(f.worker.grants, grant{to: s, handle: handle})
	f.worker.deliver()

	if s.backend == nil {
		t.Fatal("the session was granted a backend and does not hold it")
	}
	received := make([]byte, len(query)+1)
	n, err := syscall.Read(f.driver.peers[0], received)
	if err != nil || !bytes.Equal(received[:n], query) {
		t.Fatalf("the server received %q (%v), want the query the client was waiting to send", received[:max(n, 0)], err)
	}
	if got := f.proxy.core.Stat().ActiveConnections(); got != 1 {
		t.Fatalf("ActiveConnections() = %d, want 1", got)
	}
}

// The other half of the same handover: a backend returned at a transaction
// boundary goes to the session that has waited longest, and that session's held
// request is sent at once, by the thread that did the releasing. Nothing wakes
// up and nothing is posted — which is the property that keeps a saturated proxy
// from spending a context switch on every query.
func TestReleasedBackendGoesStraightToTheLongestWaiter(t *testing.T) {
	f := newQueueFixture(t)

	first, firstEnd := f.client(t, simpleQuery("SELECT 'first'"))
	f.worker.serve(first, first.advance)
	if first.backend == nil {
		t.Fatal("the first session did not get the free backend")
	}
	server := f.driver.peers[0]
	drain(t, server)

	second, _ := f.client(t, simpleQuery("SELECT 'second'"))
	third, _ := f.client(t, simpleQuery("SELECT 'third'"))
	f.worker.serve(second, second.advance)
	f.worker.serve(third, third.advance)
	if second.backend != nil || third.backend != nil {
		t.Fatal("a session was given a backend that another still holds")
	}

	// The server finishes the first session's transaction.
	ready := []byte{msgReadyForQuery, 0, 0, 0, 5, txIdle}
	if _, err := syscall.Write(server, ready); err != nil {
		t.Fatalf("Write() = %v", err)
	}
	f.worker.dispatch(first.backend.sock.fd, true, false, false)

	if first.backend != nil {
		t.Fatal("the first session kept its backend past a transaction boundary")
	}
	if second.backend == nil || third.backend != nil {
		t.Fatalf("the backend should have gone to the longest waiter: second=%v third=%v", second.backend != nil, third.backend != nil)
	}
	if got := drain(t, firstEnd); !bytes.Equal(got, ready) {
		t.Fatalf("the first client received %q, want its ReadyForQuery", got)
	}
	if got, want := drain(t, server), simpleQuery("SELECT 'second'"); !bytes.Equal(got, want) {
		t.Fatalf("the server received %q, want the second session's query %q", got, want)
	}
}

// drain reads whatever is waiting on the test's end of a socket pair.
func drain(t *testing.T, fd int) []byte {
	t.Helper()

	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatalf("SetNonblock() = %v", err)
	}
	buf := make([]byte, 4096)
	n, err := syscall.Read(fd, buf)
	if err != nil && err != syscall.EAGAIN {
		t.Fatalf("Read() = %v", err)
	}
	return buf[:max(n, 0)]
}
