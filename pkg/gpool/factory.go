// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import (
	"fmt"
	"sync"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// Vendor represents a database vendor name.
type Vendor string

const (
	// Postgres is the PostgreSQL database vendor.
	Postgres Vendor = "postgres"
)

// PoolFactory is a function that creates a connection pool.
type PoolFactory func(config any) (Pool, error)

// SubscriberFactory is a function that creates a CDC subscriber.
type SubscriberFactory func(config any) (cdc.Subscriber, error)

// registry holds vendor factories. Registration normally happens from a vendor
// package's init(), but the API is exported, so every access is guarded.
var registry = struct {
	mu          sync.RWMutex
	pools       map[Vendor]PoolFactory
	subscribers map[Vendor]SubscriberFactory
}{
	pools:       make(map[Vendor]PoolFactory),
	subscribers: make(map[Vendor]SubscriberFactory),
}

// RegisterPool registers a pool factory for a vendor. It is safe for concurrent use.
// Registering the same vendor twice replaces the previous factory.
func RegisterPool(vendor Vendor, factory PoolFactory) error {
	if vendor == "" {
		return ErrEmptyVendor
	}
	if factory == nil {
		return fmt.Errorf("%w: pool factory for vendor %q", ErrNilFactory, vendor)
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.pools[vendor] = factory
	return nil
}

// RegisterSubscriber registers a CDC subscriber factory for a vendor. It is safe for concurrent use.
// Registering the same vendor twice replaces the previous factory.
func RegisterSubscriber(vendor Vendor, factory SubscriberFactory) error {
	if vendor == "" {
		return ErrEmptyVendor
	}
	if factory == nil {
		return fmt.Errorf("%w: subscriber factory for vendor %q", ErrNilFactory, vendor)
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.subscribers[vendor] = factory
	return nil
}

// NewPool creates a new pool for the specified vendor and configuration.
func NewPool(vendor Vendor, config any) (Pool, error) {
	registry.mu.RLock()
	factory, ok := registry.pools[vendor]
	registry.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: no pool factory for vendor %q (did you import the vendor package?)", ErrVendorNotRegistered, vendor)
	}
	return factory(config)
}

// NewSubscriber creates a new subscriber for the specified vendor and configuration.
func NewSubscriber(vendor Vendor, config any) (cdc.Subscriber, error) {
	registry.mu.RLock()
	factory, ok := registry.subscribers[vendor]
	_, pooled := registry.pools[vendor]
	registry.mu.RUnlock()

	if !ok {
		// A vendor whose pool is registered was imported successfully, so telling
		// the caller to import it would send them after a package that does not
		// exist. The two cases need different answers.
		if pooled {
			return nil, fmt.Errorf("%w: vendor %q", ErrNoCDCSupport, vendor)
		}
		return nil, fmt.Errorf("%w: no subscriber factory for vendor %q (did you import the vendor package?)", ErrVendorNotRegistered, vendor)
	}
	return factory(config)
}

// Vendors returns the vendors that currently have a registered pool factory.
func Vendors() []Vendor {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	vendors := make([]Vendor, 0, len(registry.pools))
	for v := range registry.pools {
		vendors = append(vendors, v)
	}
	return vendors
}
