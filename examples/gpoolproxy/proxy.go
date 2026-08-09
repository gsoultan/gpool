// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/pooling"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
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

	driver := &backendDriver{config: upstream, maxPrepared: config.MaxPreparedStatements}
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
// Refusing is the right answer at the limit. Every session costs two goroutines
// and two relay buffers, so accepting without bound converts a burst of clients
// into memory exhaustion — the failure mode a pooler exists to prevent.
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

	p.wg.Go(func() {
		defer func() { <-p.clients }()
		newSession(p, conn, key).serve()
	})
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

	source, unlock := target.currentBackend()
	defer unlock()
	if source == nil {
		return
	}

	ctx, done := context.WithTimeout(p.ctx, cancelTimeout)
	defer done()
	_ = p.driver.cancel(ctx, source)
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

	// Sessions are parked reading from clients that may never send again, so
	// they have to be woken rather than waited for.
	p.mu.Lock()
	for _, s := range p.sessions {
		_ = s.conn.SetDeadline(time.Unix(0, 0))
	}
	p.mu.Unlock()

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
