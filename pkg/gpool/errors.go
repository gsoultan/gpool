// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool

import "errors"

var (
	// ErrVendorNotRegistered is returned when no factory has been registered for the requested vendor.
	// Vendors register themselves from init(), so this usually means the vendor package was not imported.
	ErrVendorNotRegistered = errors.New("gpool: vendor not registered")

	// ErrNilFactory is returned by RegisterPool and RegisterSubscriber when the factory is nil.
	ErrNilFactory = errors.New("gpool: factory must not be nil")

	// ErrEmptyVendor is returned when an empty vendor name is registered or requested.
	ErrEmptyVendor = errors.New("gpool: vendor name must not be empty")
)
