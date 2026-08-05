// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool_test

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/gsoultan/gpool/pkg/gpool"
)

func TestEngineRegistersTheInitialSubscriber(t *testing.T) {
	t.Parallel()

	pool := &fakePool{}
	subscriber := &fakeSubscriber{}
	engine := gpool.NewEngine(pool, subscriber)

	if engine.Pool() != pool {
		t.Error("Pool() returned a different pool")
	}
	if engine.Subscriber() != subscriber {
		t.Error("Subscriber() should return the initial subscriber")
	}
	if engine.Subscriber(gpool.DefaultSubscriber) != subscriber {
		t.Error("the initial subscriber should be registered under DefaultSubscriber")
	}
	if engine.Subscriber("missing") != nil {
		t.Error("an unknown name should return nil")
	}
}

// Close used to dereference the pool unconditionally, so an engine built for CDC
// only panicked on shutdown.
func TestEngineToleratesNilComponents(t *testing.T) {
	t.Parallel()

	engine := gpool.NewEngine(nil, nil)

	if engine.Pool() != nil {
		t.Error("Pool() = non-nil, want nil")
	}
	if engine.Subscriber() != nil {
		t.Error("Subscriber() = non-nil, want nil")
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestEngineCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	pool := &fakePool{}
	subscriber := &fakeSubscriber{}
	engine := gpool.NewEngine(pool, subscriber)

	for range 3 {
		if err := engine.Close(); err != nil {
			t.Fatalf("Close() = %v", err)
		}
	}

	if got := pool.closes.Load(); got != 1 {
		t.Errorf("pool closed %d times, want 1", got)
	}
	if got := subscriber.closes.Load(); got != 1 {
		t.Errorf("subscriber closed %d times, want 1", got)
	}
}

// One failing subscriber must not stop the others, or the pool, from being closed.
func TestEngineCloseJoinsErrorsAndClosesEverything(t *testing.T) {
	t.Parallel()

	pool := &fakePool{}
	failing := &fakeSubscriber{closeErr: errors.New("stream stuck")}
	healthy := &fakeSubscriber{}

	engine := gpool.NewEngine(pool, failing)
	engine.AddSubscriber("healthy", healthy)

	err := engine.Close()
	if err == nil {
		t.Fatal("Close() = nil, want the subscriber's error")
	}
	if !errors.Is(err, failing.closeErr) {
		t.Errorf("Close() = %v, want it to wrap the subscriber's error", err)
	}
	if got := healthy.closes.Load(); got != 1 {
		t.Errorf("healthy subscriber closed %d times, want 1", got)
	}
	if got := pool.closes.Load(); got != 1 {
		t.Errorf("pool closed %d times, want 1", got)
	}

	// The recorded error is replayed rather than the work being redone.
	if second := engine.Close(); !errors.Is(second, failing.closeErr) {
		t.Errorf("second Close() = %v, want the first result", second)
	}
	if got := pool.closes.Load(); got != 1 {
		t.Errorf("pool closed %d times after a second Close, want 1", got)
	}
}

func TestEngineAddAndRemoveSubscriber(t *testing.T) {
	t.Parallel()

	engine := gpool.NewEngine(&fakePool{}, nil)
	t.Cleanup(func() { _ = engine.Close() })

	subscriber := &fakeSubscriber{}
	engine.AddSubscriber("analytics", subscriber)

	if engine.Subscriber("analytics") != subscriber {
		t.Fatal("Subscriber(analytics) did not return the registered subscriber")
	}
	if names := engine.Subscribers(); !slices.Contains(names, "analytics") {
		t.Errorf("Subscribers() = %v, want it to contain analytics", names)
	}

	if err := engine.RemoveSubscriber("analytics"); err != nil {
		t.Fatalf("RemoveSubscriber() = %v", err)
	}
	if got := subscriber.closes.Load(); got != 1 {
		t.Errorf("removed subscriber closed %d times, want 1", got)
	}
	if engine.Subscriber("analytics") != nil {
		t.Error("Subscriber(analytics) should be nil after removal")
	}

	// Removing an unknown name is a no-op.
	if err := engine.RemoveSubscriber("analytics"); err != nil {
		t.Errorf("removing an unknown subscriber = %v, want nil", err)
	}
}

func TestEngineIgnoresNilSubscriber(t *testing.T) {
	t.Parallel()

	engine := gpool.NewEngine(nil, nil)
	engine.AddSubscriber("nothing", nil)

	if engine.Subscriber("nothing") != nil {
		t.Error("a nil subscriber should not be registered")
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestEngineIsRaceFree(t *testing.T) {
	t.Parallel()

	engine := gpool.NewEngine(&fakePool{}, &fakeSubscriber{})

	var wg sync.WaitGroup
	for i := range 8 {
		name := string(rune('a' + i))

		wg.Go(func() { engine.AddSubscriber(name, &fakeSubscriber{}) })
		wg.Go(func() { _ = engine.Subscriber(name) })
		wg.Go(func() { _ = engine.Subscribers() })
		wg.Go(func() { _ = engine.RemoveSubscriber(name) })
		wg.Go(func() { engine.AddPool(name, &fakePool{}) })
		wg.Go(func() { _ = engine.Pool(name) })
		wg.Go(func() { _ = engine.Pools() })
		wg.Go(func() { engine.RemovePool(name) })
	}
	wg.Wait()

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

// Reaching several databases from one process means one pool per database, kept
// apart by name. Nothing is shared between them.
func TestEngineHoldsSeveralPools(t *testing.T) {
	t.Parallel()

	primary := &fakePool{}
	analytics := &fakePool{}
	archive := &fakePool{}

	engine := gpool.NewEngine(primary, nil)
	engine.AddPool("analytics", analytics)
	engine.AddPool("archive", archive)

	if engine.Pool() != primary {
		t.Error("Pool() should return the pool passed to NewEngine")
	}
	if engine.Pool(gpool.DefaultPool) != primary {
		t.Error("the constructor pool should be registered under DefaultPool")
	}
	if engine.Pool("analytics") != analytics {
		t.Error("Pool(analytics) returned the wrong pool")
	}
	if engine.Pool("archive") != archive {
		t.Error("Pool(archive) returned the wrong pool")
	}
	if engine.Pool("missing") != nil {
		t.Error("an unknown name should return nil")
	}

	names := engine.Pools()
	if len(names) != 3 {
		t.Fatalf("Pools() = %v, want 3 entries", names)
	}
	for _, want := range []string{gpool.DefaultPool, "analytics", "archive"} {
		if !slices.Contains(names, want) {
			t.Errorf("Pools() = %v, want it to contain %q", names, want)
		}
	}
}

func TestEngineRemovePoolClosesIt(t *testing.T) {
	t.Parallel()

	analytics := &fakePool{}
	engine := gpool.NewEngine(&fakePool{}, nil)
	engine.AddPool("analytics", analytics)

	engine.RemovePool("analytics")

	if got := analytics.closes.Load(); got != 1 {
		t.Errorf("removed pool closed %d times, want 1", got)
	}
	if engine.Pool("analytics") != nil {
		t.Error("Pool(analytics) should be nil after removal")
	}

	// Removing an unknown name is a no-op.
	engine.RemovePool("analytics")
	if got := analytics.closes.Load(); got != 1 {
		t.Errorf("pool closed %d times after a second removal, want 1", got)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestEngineCloseClosesEveryPool(t *testing.T) {
	t.Parallel()

	primary := &fakePool{}
	analytics := &fakePool{}
	archive := &fakePool{}

	engine := gpool.NewEngine(primary, nil)
	engine.AddPool("analytics", analytics)
	engine.AddPool("archive", archive)

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	for name, pool := range map[string]*fakePool{"default": primary, "analytics": analytics, "archive": archive} {
		if got := pool.closes.Load(); got != 1 {
			t.Errorf("pool %q closed %d times, want 1", name, got)
		}
	}

	// Still idempotent with several pools registered.
	if err := engine.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if got := analytics.closes.Load(); got != 1 {
		t.Errorf("pool closed %d times after a second Close, want 1", got)
	}
}

// Replacing a registration must not close the pool being displaced: the caller may
// still hold it, and closing it silently would break in-flight work.
func TestEngineAddPoolReplacesWithoutClosing(t *testing.T) {
	t.Parallel()

	first := &fakePool{}
	second := &fakePool{}

	engine := gpool.NewEngine(nil, nil)
	engine.AddPool("shard", first)
	engine.AddPool("shard", second)

	if engine.Pool("shard") != second {
		t.Error("AddPool should replace the previous registration")
	}
	if got := first.closes.Load(); got != 0 {
		t.Errorf("displaced pool closed %d times, want 0", got)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestEngineIgnoresNilPool(t *testing.T) {
	t.Parallel()

	engine := gpool.NewEngine(nil, nil)
	engine.AddPool("nothing", nil)

	if engine.Pool("nothing") != nil {
		t.Error("a nil pool should not be registered")
	}
	if names := engine.Pools(); len(names) != 0 {
		t.Errorf("Pools() = %v, want empty", names)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}
