// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/pooling"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// errClosing reports a client that finished authenticating after the proxy had
// begun to shut down.
var errClosing = errors.New("gpoolproxy: shutting down")

// Refusals of a replication client. They are distinct so the client can be told
// which, in SQLSTATE terms: asking for something this proxy is not configured to
// give, arriving when every place is taken, or the server not answering.
var (
	errReplicationDisabled    = errors.New("gpoolproxy: replication connections are not enabled")
	errReplicationFull        = errors.New("gpoolproxy: too many replication connections")
	errReplicationUnavailable = errors.New("gpoolproxy: server refused or did not answer a replication connection")
)

// cancelTimeout bounds the side connection a cancellation request opens.
const cancelTimeout = 5 * time.Second

// cancelKey identifies a session for cancellation. Every client is given a
// generated one rather than its backend's, because a client keeps its key for
// the whole session while the backend behind it changes every transaction.
type cancelKey struct {
	pid uint32
	// secret is a string rather than a []byte so the key stays comparable and can
	// be a map key. Protocol 3.2 made the server's own key variable length; the
	// four bytes issued here are what a 3.0 client's fixed-size CancelRequest
	// can carry.
	secret string
}

// Proxy accepts PostgreSQL clients and multiplexes them onto a pool of real
// server connections.
//
// This is the shape gpool cannot take as a library: bounding connections across
// separate applications needs a process that outlives all of them. What the
// library does provide is everything below the wire protocol — the pool here is
// the same pooling.Core every gpool vendor runs on.
type Proxy struct {
	config Config
	core   *pooling.Core[*backend]
	driver *backendDriver
	users  userlist
	tls    *tls.Config

	// decoy authenticates nobody. It gives an unknown username the same work and
	// the same timing as a known one.
	decoy verifier

	listener net.Listener
	clients  chan struct{}

	// poller is the readiness source every worker waits on, and socks is how a
	// worker turns the descriptor number it reports back into a socket.
	poller  *poller
	socks   *sockTable
	workers sync.WaitGroup
	started bool

	// inbox carries work onto a worker from a goroutine that is not one: a
	// handshake that finished with a query already in hand, a dial that
	// completed. wakeR and wakeW are the pipe that gets a worker's attention.
	// It is not on the path of a query.
	inboxMu     sync.Mutex
	inbox       []func(*worker)
	inboxClosed bool
	wakeR       int
	wakeW       int
	stopping    atomic.Bool

	// waiters is the sessions queued for a backend, and growing counts the
	// goroutines currently dialing one for them.
	waiters waiters
	growing atomic.Int32

	// replicating counts the dedicated connections held by replication clients,
	// which are outside the pool and so outside its accounting.
	replicating atomic.Int32

	mu       sync.Mutex
	sessions map[cancelKey]*session

	wg      sync.WaitGroup
	ctx     context.Context
	stop    context.CancelFunc
	closing atomic.Bool
}

// NewProxy prepares a proxy. Nothing is accepted until Serve is called.
func NewProxy(config Config) (*Proxy, error) {
	config = config.WithDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}

	users, err := loadUserlist(config.Userlist)
	if err != nil {
		return nil, err
	}
	decoy, err := deriveVerifier(randomSecret(), defaultSCRAMIterations)
	if err != nil {
		return nil, err
	}

	// Parsed once and cloned per connection. pgconn.ParseConfig re-reads the
	// environment, service files and ~/.pgpass every call, which is not work to
	// repeat for every backend the pool opens.
	upstream, err := pgconn.ParseConfig(config.Upstream)
	if err != nil {
		return nil, err
	}

	socks := newSockTable(descriptorLimit())
	driver := &backendDriver{config: upstream, maxPrepared: config.MaxPreparedStatements, socks: socks}
	core, err := pooling.New(driver, config.Pool)
	if err != nil {
		return nil, err
	}

	tlsConfig, err := clientTLS(config)
	if err != nil {
		core.Close()
		return nil, err
	}

	ctx, stop := context.WithCancel(context.Background())
	return &Proxy{
		config:   config,
		core:     core,
		driver:   driver,
		users:    users,
		tls:      tlsConfig,
		decoy:    decoy,
		socks:    socks,
		clients:  make(chan struct{}, config.MaxClients),
		sessions: make(map[cancelKey]*session),
		ctx:      ctx,
		stop:     stop,
	}, nil
}

// Listen binds the listening socket, so a caller can learn the address before
// serving. Useful when the port is chosen by the kernel.
func (p *Proxy) Listen() error {
	listener, err := net.Listen("tcp", p.config.Listen)
	if err != nil {
		return err
	}
	p.listener = listener
	return nil
}

// Addr reports the address being served.
func (p *Proxy) Addr() net.Addr {
	return p.listener.Addr()
}

// Serve accepts clients until Close. It binds first if Listen has not been called.
func (p *Proxy) Serve() error {
	if p.listener == nil {
		if err := p.Listen(); err != nil {
			return err
		}
	}
	if err := p.startWorkers(); err != nil {
		return err
	}
	p.warm(p.ctx)

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if p.closing.Load() {
				return nil
			}
			return err
		}
		p.accept(conn)
	}
}

// accept starts a session, or turns the client away if the proxy is full.
//
// Refusing is the right answer at the limit. A session is cheap but not free, and
// each one is a descriptor, so accepting without bound converts a burst of
// clients into exhaustion of one or the other — the failure mode a pooler exists
// to prevent.
func (p *Proxy) accept(conn net.Conn) {
	select {
	case p.clients <- struct{}{}:
	default:
		p.refuse(conn)
		return
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	key, err := newCancelKey()
	if err != nil {
		<-p.clients
		conn.Close()
		return
	}

	// The goroutine lasts only as long as the handshake. An adopted client keeps
	// its slot until its session closes; anything else gives it back here.
	p.wg.Go(func() {
		if !newHandshake(p, conn, key).serve() {
			<-p.clients
		}
	})
}

// startWorkers brings up the poller and one worker per core. It is idempotent.
func (p *Proxy) startWorkers() error {
	if p.started {
		return nil
	}

	poller, err := newPoller()
	if err != nil {
		return err
	}
	var pipe [2]int
	if err := syscall.Pipe(pipe[:]); err != nil {
		poller.close()
		return err
	}
	for _, fd := range pipe {
		syscall.CloseOnExec(fd)
		_ = syscall.SetNonblock(fd, true)
	}
	if err := poller.addLevel(pipe[0]); err != nil {
		poller.close()
		_ = syscall.Close(pipe[0])
		_ = syscall.Close(pipe[1])
		return err
	}

	p.poller, p.wakeR, p.wakeW = poller, pipe[0], pipe[1]
	p.started = true

	for range runtime.GOMAXPROCS(0) {
		p.workers.Go(newWorker(p).run)
	}
	return nil
}

// post runs work on a worker. It reports false once the proxy has shut its
// workers down, in which case the work will never run and the caller keeps
// whatever it was handing over.
func (p *Proxy) post(work func(*worker)) bool {
	p.inboxMu.Lock()
	defer p.inboxMu.Unlock()

	if p.inboxClosed {
		return false
	}
	p.inbox = append(p.inbox, work)

	// Written under the lock that Close takes before it closes the pipe, so the
	// byte cannot land in whatever socket has since been given the number.
	_, _ = syscall.Write(p.wakeW, []byte{0})
	return true
}

// runInbox runs posted work on the worker that was woken for it.
func (p *Proxy) runInbox(w *worker) {
	// While stopping, the pipe is left readable on purpose: it is what wakes
	// every other worker so that each sees the proxy is stopping.
	if p.stopping.Load() {
		return
	}

	var scratch [64]byte
	for {
		n, err := syscall.Read(p.wakeR, scratch[:])
		if n < len(scratch) || err != nil {
			break
		}
	}
	p.drainInbox(w)
}

func (p *Proxy) drainInbox(w *worker) {
	p.inboxMu.Lock()
	posted := p.inbox
	p.inbox = nil
	p.inboxMu.Unlock()

	for _, work := range posted {
		work(w)
	}
}

// adopt turns an authenticated connection into a session the workers serve.
// early is whatever the client sent beyond its handshake.
// replicate opens a dedicated server connection for a replication client.
//
// It is not taken from the pool and never returns to it. Logical replication
// cannot be pooled: CopyBoth never yields a transaction boundary, and a slot
// admits one walsender. So the proxy does for such a client what PgBouncer does,
// which is get out of the way — one client, one server connection, bytes passed
// through until either end hangs up.
//
// The count is taken before dialing so that a burst of clients cannot open more
// connections than the bound while their dials are still in flight.
func (p *Proxy) replicate(ctx context.Context, mode string) (*backend, error) {
	limit := int32(p.config.MaxReplicationClients)
	if limit == 0 {
		return nil, errReplicationDisabled
	}
	if p.replicating.Add(1) > limit {
		p.replicating.Add(-1)
		return nil, errReplicationFull
	}

	ctx, cancel := context.WithTimeout(ctx, replicationDialTimeout)
	defer cancel()

	target, err := p.driver.Replicate(ctx, mode)
	if err != nil {
		p.replicating.Add(-1)
		// The cause is not wrapped: a dial error can quote the connection
		// string, and this one travels towards a client.
		return nil, errReplicationUnavailable
	}
	return target, nil
}

// endReplication closes a dedicated connection and gives its place back.
func (p *Proxy) endReplication(target *backend) {
	_ = p.driver.Close(context.Background(), target)
	p.replicating.Add(-1)
}

// adopt hands an authenticated client to the event loop. pinned, when not nil,
// is a dedicated server connection the session holds for its whole life; if
// adopt fails it is still the caller's to close.
func (p *Proxy) adopt(conn net.Conn, key cancelKey, early []byte, pinned *backend) error {
	if p.closing.Load() {
		return errClosing
	}

	fd, err := takeOver(conn)
	if err != nil {
		return err
	}

	s := &session{
		proxy:      p,
		key:        key,
		statements: statements{limit: p.config.MaxPreparedStatements},
	}
	s.client.fd = fd
	s.client.owner.Store(s)
	if len(early) > 0 {
		s.client.in = append([]byte(nil), early...)
		s.stalled = true
	}

	if pinned != nil {
		s.pinned = true
		s.backend = pinned
		pinned.sock.owner.Store(s)
	}

	p.register(s)
	if !p.socks.put(&s.client) {
		p.unregister(s)
		_ = syscall.Close(fd)
		return errTooManyDescriptors
	}

	// The pinned connection is listed before the client is, so that if it
	// cannot be, nothing is yet live to undo. From here its events may arrive on
	// any worker, which is why the session was complete before this line.
	if pinned != nil {
		if err := pinned.enlist(p.poller); err != nil {
			p.socks.drop(&s.client)
			p.unregister(s)
			_ = syscall.Close(fd)
			return err
		}
	}

	// Registering reports the socket's current readiness, so a query that
	// arrives before this returns is not missed.
	if err := p.poller.add(fd); err != nil {
		// A worker may already be serving the pinned connection's first event,
		// so the session is retired under its lock rather than out from under it.
		s.mu.Lock()
		s.closed = true
		s.backend = nil
		s.mu.Unlock()
		if pinned != nil {
			pinned.delist(p.poller)
			pinned.sock.owner.Store(nil)
		}

		p.socks.drop(&s.client)
		p.unregister(s)
		_ = syscall.Close(fd)
		return err
	}

	// What the client sent early is already out of the kernel, so no event will
	// ever announce it.
	if len(early) > 0 {
		p.post(func(w *worker) { w.serve(s, s.advance) })
	}
	return nil
}

// departed is called by a session as it closes.
func (p *Proxy) departed(s *session) {
	p.unregister(s)
	<-p.clients
}

// background runs work that would block an event loop on a goroutine the proxy
// waits for at shutdown.
func (p *Proxy) background(work func()) {
	p.wg.Go(work)
}

// handoff offers a just-released backend to the longest-waiting session.
//
// from is the worker doing the releasing, or nil off a worker. A worker delivers
// the connection itself as soon as it has finished with the session that
// released it, so on the path every query takes there is no wake-up and no
// thread switch between one client finishing and the next one starting.
func (p *Proxy) handoff(from *worker) {
	for !p.waiters.empty() {
		handle, ok := p.core.TryAcquire()
		if !ok {
			p.grow()
			return
		}
		if s := p.waiters.pop(); s != nil {
			p.grant(s, handle, from)
			return
		}
		// Everyone queued had already left. Returned rather than kept, and the
		// queue looked at again in case someone joined in the meantime.
		handle.Release()
	}
}

// grant gives a checked-out backend to a session that was waiting for one.
func (p *Proxy) grant(s *session, handle pooling.Handle[*backend], from *worker) {
	matched := grant{to: s, handle: handle}
	if from != nil {
		from.grants = append(from.grants, matched)
		return
	}

	delivered := p.post(func(w *worker) {
		w.grants = append(w.grants, matched)
		w.deliver()
	})
	if !delivered {
		handle.Release()
	}
}

// restore returns a backend that turned out not to be needed, and passes it on.
func (p *Proxy) restore(handle pooling.Handle[*backend], from *worker) {
	handle.Release()
	p.handoff(from)
}

// grow dials another backend for the sessions that are waiting, if the pool has
// room for one.
//
// A waiter is normally served by the next release. That only works when there is
// a next release: a pool that has not yet opened its connections, or has just
// lost them, has nothing to release, and the engine only dials from Acquire —
// which blocks, and so cannot be called from a worker.
func (p *Proxy) grow() {
	limit := p.core.MaxConns()
	if p.core.Stat().TotalConnections() >= limit {
		return
	}
	if p.growing.Add(1) > limit {
		p.growing.Add(-1)
		return
	}
	p.background(p.dial)
}

// dial acquires backends the blocking way on behalf of queued sessions, for as
// long as there are sessions queued and room to grow.
func (p *Proxy) dial() {
	defer func() {
		p.growing.Add(-1)
		// Someone may have queued and been turned away by the limit on dialers
		// in the instant this one decided to stop.
		if !p.waiters.empty() && !p.closing.Load() {
			p.handoff(nil)
		}
	}()

	for !p.waiters.empty() && !p.closing.Load() {
		handle, err := p.core.Acquire(p.ctx)
		s := p.waiters.pop()

		switch {
		case err != nil && s != nil:
			// One failed dial answers one waiter. The rest each get their own
			// attempt, so a server that comes back mid-queue serves whoever is
			// still waiting.
			p.post(func(w *worker) { w.serve(s, s.refused) })
		case err != nil:
			return
		case s == nil:
			handle.Release()
		default:
			p.grant(s, handle, nil)
		}

		if p.core.Stat().TotalConnections() >= p.core.MaxConns() {
			return
		}
	}
}

// sendCancel asks the server to cancel whatever the backend with this key is
// running.
func (p *Proxy) sendCancel(pid uint32, secret []byte) {
	ctx, done := context.WithTimeout(p.ctx, cancelTimeout)
	defer done()
	_ = p.driver.cancel(ctx, pid, secret)
}

// refuse tells a client the proxy is full, in the protocol's own terms, so it
// reports a reason rather than an unexplained disconnect.
func (p *Proxy) refuse(conn net.Conn) {
	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(cancelTimeout))
	encoded, err := (&pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     "53300", // too_many_connections
		Message:  "too many clients already",
	}).Encode(nil)
	if err != nil {
		return
	}
	_, _ = conn.Write(encoded)
}

// register makes a session reachable by its cancellation key.
func (p *Proxy) register(s *session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessions[s.key] = s
}

func (p *Proxy) unregister(s *session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, s.key)
}

// cancel forwards a client's cancellation request to whichever backend is
// currently serving that session.
//
// The key a client holds is the proxy's, not the server's, so the request has to
// be translated. A key that matches nothing is ignored in silence: the request
// arrives unauthenticated, so answering it would confirm which keys are live.
func (p *Proxy) cancel(pid uint32, secret string) {
	p.mu.Lock()
	target := p.sessions[cancelKey{pid: pid, secret: secret}]
	p.mu.Unlock()

	if target == nil {
		return
	}
	target.mu.Lock()
	target.cancel()
	target.mu.Unlock()
}

// parameters reports the server settings replayed to clients during startup.
func (p *Proxy) parameters() map[string]string {
	if statuses := p.driver.params.Load(); statuses != nil {
		return *statuses
	}
	return nil
}

// warm opens one backend and immediately lets it go, so the server's settings
// are known before any client is told about them.
//
// Those settings are captured from a real connection rather than invented, which
// leaves nothing to say until the pool has opened one. A client that connected
// first was handed an empty set — survivable over the extended protocol, and
// refused outright by pgx's simple protocol, which will not run without
// standard_conforming_strings.
//
// Best effort, and bounded: a proxy started before its server must still start,
// and a server that is unreachable now may not be later. Whoever finds the set
// still empty asks again.
func (p *Proxy) warm(ctx context.Context) {
	if p.parameters() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, warmTimeout)
	defer cancel()

	handle, err := p.core.Acquire(ctx)
	if err != nil {
		return
	}
	handle.Release()

	// Released outside a session, so nobody else will offer it to the queue.
	p.handoff(nil)
}

// upgrade wraps a client connection in TLS.
func (p *Proxy) upgrade(conn net.Conn) *tls.Conn {
	return tls.Server(conn, p.tls)
}

// Stat reports the pool behind the proxy: real PostgreSQL connections, not
// clients.
func (p *Proxy) Stat() gpool.Stat {
	return p.core.Stat()
}

// Clients reports how many client sessions are currently connected.
func (p *Proxy) Clients() int {
	return len(p.clients)
}

// Close stops accepting, ends every session, and drains the pool. It is
// idempotent.
func (p *Proxy) Close() {
	if p.closing.Swap(true) {
		return
	}

	p.stop()
	if p.listener != nil {
		_ = p.listener.Close()
	}

	if p.started {
		p.shutdown()
	}

	p.wg.Wait()
	p.core.Close()
}

// clientTLS builds the TLS configuration for the client side, if one was asked for.
func clientTLS(config Config) (*tls.Config, error) {
	if config.TLSCert == "" {
		return nil, nil
	}

	certificate, err := tls.LoadX509KeyPair(config.TLSCert, config.TLSKey)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// newCancelKey generates a session's cancellation key.
//
// It must be unguessable: anyone who can reach the listener and produce a valid
// key can cancel a stranger's query, and the request carries no other proof.
func newCancelKey() (cancelKey, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return cancelKey{}, err
	}
	return cancelKey{
		pid:    binary.BigEndian.Uint32(buf[:4]),
		secret: string(buf[4:]),
	}, nil
}

// randomSecret produces the throwaway password behind the decoy verifier.
func randomSecret() string {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand does not fail on any supported platform; if it somehow
		// does, a decoy nobody can authenticate against is still the safe answer.
		return string(buf[:])
	}
	return string(buf[:])
}

// shutdown stops the workers and closes every session.
func (p *Proxy) shutdown() {
	// The wake pipe is written once and, while stopping, never drained, so it
	// stays readable until every worker has woken and seen the flag.
	p.stopping.Store(true)
	p.inboxMu.Lock()
	_, _ = syscall.Write(p.wakeW, []byte{0})
	p.inboxMu.Unlock()
	p.workers.Wait()

	// With no workers left, this goroutine is the only one serving sessions and
	// stands in for one. Work posted before the door closed is still owed: a
	// backend on its way to a session has to be given back by someone.
	last := newWorker(p)
	p.inboxMu.Lock()
	p.inboxClosed = true
	p.inboxMu.Unlock()
	p.drainInbox(last)

	// Handshakes and dials still running finish against a closed inbox, and
	// only then is the set of sessions final.
	p.wg.Wait()

	for fd := range p.socks.slots {
		sk := p.socks.get(fd)
		if sk == nil || sk.backend != nil {
			continue
		}
		if s := sk.owner.Load(); s != nil {
			last.serve(s, s.close)
		}
	}

	p.poller.close()
	_ = syscall.Close(p.wakeR)
	_ = syscall.Close(p.wakeW)
}
