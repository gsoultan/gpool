// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool_test

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

func TestRegisterAndCreatePool(t *testing.T) {
	const vendor = gpool.Vendor("test-pool-roundtrip")

	want := &fakePool{}
	if err := gpool.RegisterPool(vendor, func(any) (gpool.Pool, error) { return want, nil }); err != nil {
		t.Fatalf("RegisterPool() = %v", err)
	}

	got, err := gpool.NewPool(vendor, nil)
	if err != nil {
		t.Fatalf("NewPool() = %v", err)
	}
	if got != want {
		t.Fatal("NewPool() returned a different pool than the factory produced")
	}

	if !slices.Contains(gpool.Vendors(), vendor) {
		t.Errorf("Vendors() = %v, want it to contain %q", gpool.Vendors(), vendor)
	}
}

func TestRegisterAndCreateSubscriber(t *testing.T) {
	const vendor = gpool.Vendor("test-subscriber-roundtrip")

	want := &fakeSubscriber{}
	if err := gpool.RegisterSubscriber(vendor, func(any) (cdc.Subscriber, error) { return want, nil }); err != nil {
		t.Fatalf("RegisterSubscriber() = %v", err)
	}

	got, err := gpool.NewSubscriber(vendor, nil)
	if err != nil {
		t.Fatalf("NewSubscriber() = %v", err)
	}
	if got != want {
		t.Fatal("NewSubscriber() returned a different subscriber than the factory produced")
	}
}

func TestRegisterRejectsBadInput(t *testing.T) {
	t.Parallel()

	if err := gpool.RegisterPool("", func(any) (gpool.Pool, error) { return nil, nil }); !errors.Is(err, gpool.ErrEmptyVendor) {
		t.Errorf("RegisterPool(\"\") = %v, want ErrEmptyVendor", err)
	}
	if err := gpool.RegisterPool("x", nil); !errors.Is(err, gpool.ErrNilFactory) {
		t.Errorf("RegisterPool(nil factory) = %v, want ErrNilFactory", err)
	}
	if err := gpool.RegisterSubscriber("", nil); !errors.Is(err, gpool.ErrEmptyVendor) {
		t.Errorf("RegisterSubscriber(\"\") = %v, want ErrEmptyVendor", err)
	}
	if err := gpool.RegisterSubscriber("x", nil); !errors.Is(err, gpool.ErrNilFactory) {
		t.Errorf("RegisterSubscriber(nil factory) = %v, want ErrNilFactory", err)
	}
}

// The usual cause is forgetting to import the vendor package, so the error says so.
func TestUnregisteredVendorIsReported(t *testing.T) {
	t.Parallel()

	_, err := gpool.NewPool("never-registered", nil)
	if !errors.Is(err, gpool.ErrVendorNotRegistered) {
		t.Fatalf("NewPool() = %v, want ErrVendorNotRegistered", err)
	}

	_, err = gpool.NewSubscriber("never-registered", nil)
	if !errors.Is(err, gpool.ErrVendorNotRegistered) {
		t.Fatalf("NewSubscriber() = %v, want ErrVendorNotRegistered", err)
	}
}

func TestFactoryErrorIsPropagated(t *testing.T) {
	const vendor = gpool.Vendor("test-factory-error")

	sentinel := errors.New("bad config")
	if err := gpool.RegisterPool(vendor, func(any) (gpool.Pool, error) { return nil, sentinel }); err != nil {
		t.Fatalf("RegisterPool() = %v", err)
	}

	if _, err := gpool.NewPool(vendor, nil); !errors.Is(err, sentinel) {
		t.Fatalf("NewPool() = %v, want the factory's error", err)
	}
}

// The registry is exported, so registration can race a lookup. Unsynchronised maps
// would fail here under -race, or crash outright with a concurrent map access.
func TestRegistryIsRaceFree(t *testing.T) {
	var wg sync.WaitGroup

	const workers = 8
	for i := range workers {
		vendor := gpool.Vendor(fmt.Sprintf("test-race-%d", i))

		wg.Go(func() {
			_ = gpool.RegisterPool(vendor, func(any) (gpool.Pool, error) { return &fakePool{}, nil })
		})
		wg.Go(func() {
			_ = gpool.RegisterSubscriber(vendor, func(any) (cdc.Subscriber, error) { return &fakeSubscriber{}, nil })
		})
		wg.Go(func() {
			_, _ = gpool.NewPool(vendor, nil)
		})
		wg.Go(func() {
			_, _ = gpool.NewSubscriber(vendor, nil)
		})
		wg.Go(func() {
			_ = gpool.Vendors()
		})
	}
	wg.Wait()
}
