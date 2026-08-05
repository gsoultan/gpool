// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// Names under which NewEngine registers the components it is given.
const (
	DefaultPool       = "default"
	DefaultSubscriber = "default"
)

// Engine is an optional facade that owns any number of named pools and CDC
// subscribers so a host application can shut everything down through a single
// Close. All methods are safe for concurrent use, and Close is idempotent.
//
// Multiple pools is the supported way to reach several databases from one process:
// register one pool per database and select it by name. Nothing is shared between
// them — separate connection strings, separate capacity, separate lifecycles — so
// one database saturating or failing cannot starve another.
type Engine struct {
	mu          sync.RWMutex
	pools       map[string]Pool
	subscribers map[string]cdc.Subscriber

	closeOnce sync.Once
	closeErr  error
}

// NewEngine creates a new engine. Both arguments may be nil. A non-nil pool is
// registered under DefaultPool and a non-nil subscriber under DefaultSubscriber.
//
// Use AddPool to attach further databases.
func NewEngine(pool Pool, subscriber cdc.Subscriber) *Engine {
	e := &Engine{
		pools:       make(map[string]Pool),
		subscribers: make(map[string]cdc.Subscriber),
	}
	if pool != nil {
		e.pools[DefaultPool] = pool
	}
	if subscriber != nil {
		e.subscribers[DefaultSubscriber] = subscriber
	}
	return e
}

// Pool returns the default pool, or the named one when a name is given.
// It returns nil when nothing is registered under that name.
func (e *Engine) Pool(name ...string) Pool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.pools[nameOrDefault(name, DefaultPool)]
}

// AddPool registers a pool under the given name, replacing any existing
// registration without closing it. A nil pool is ignored.
func (e *Engine) AddPool(name string, pool Pool) {
	if pool == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.pools[name] = pool
}

// RemovePool unregisters a pool and closes it.
// Removing an unknown name is a no-op.
func (e *Engine) RemovePool(name string) {
	e.mu.Lock()
	pool, ok := e.pools[name]
	delete(e.pools, name)
	e.mu.Unlock()

	if ok && pool != nil {
		pool.Close()
	}
}

// Pools returns the names of every registered pool.
func (e *Engine) Pools() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return keysOf(e.pools)
}

// Subscriber returns the default CDC subscriber, or the named one when a name is
// given. It returns nil when nothing is registered under that name.
func (e *Engine) Subscriber(name ...string) cdc.Subscriber {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.subscribers[nameOrDefault(name, DefaultSubscriber)]
}

// AddSubscriber registers a CDC subscriber under the given name, replacing any
// existing registration without closing it. A nil subscriber is ignored.
func (e *Engine) AddSubscriber(name string, subscriber cdc.Subscriber) {
	if subscriber == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.subscribers[name] = subscriber
}

// RemoveSubscriber unregisters a CDC subscriber and closes it.
// Removing an unknown name is a no-op and returns nil.
func (e *Engine) RemoveSubscriber(name string) error {
	e.mu.Lock()
	subscriber, ok := e.subscribers[name]
	delete(e.subscribers, name)
	e.mu.Unlock()

	if !ok || subscriber == nil {
		return nil
	}
	if err := subscriber.Close(); err != nil {
		return fmt.Errorf("gpool: closing subscriber %q: %w", name, err)
	}
	return nil
}

// Subscribers returns the names of every registered subscriber.
func (e *Engine) Subscribers() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return keysOf(e.subscribers)
}

// Close closes every subscriber and every pool, joining any errors. It is
// idempotent: repeat calls return the result of the first without closing anything
// again. One component failing to close does not stop the others.
func (e *Engine) Close() error {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		subscribers, pools := e.subscribers, e.pools
		e.subscribers = make(map[string]cdc.Subscriber)
		e.pools = make(map[string]Pool)
		e.mu.Unlock()

		errs := make([]error, 0, len(subscribers))
		for name, subscriber := range subscribers {
			if subscriber == nil {
				continue
			}
			if err := subscriber.Close(); err != nil {
				errs = append(errs, fmt.Errorf("gpool: closing subscriber %q: %w", name, err))
			}
		}

		// Pools close last so a subscriber can still drain through one if a future
		// vendor shares transport between the two.
		for _, pool := range pools {
			if pool != nil {
				pool.Close()
			}
		}

		e.closeErr = errors.Join(errs...)
	})
	return e.closeErr
}

// nameOrDefault resolves an optional name argument.
func nameOrDefault(name []string, fallback string) string {
	if len(name) > 0 {
		return name[0]
	}
	return fallback
}

// keysOf returns a map's keys. The caller must hold the lock.
func keysOf[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	return names
}
