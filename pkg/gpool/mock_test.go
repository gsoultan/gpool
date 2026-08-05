// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool_test

import (
	"context"
	"iter"
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// fakePool is a gpool.Pool that only records whether it was closed.
type fakePool struct {
	closes atomic.Int32
}

var _ gpool.Pool = (*fakePool)(nil)

func (p *fakePool) Close() { p.closes.Add(1) }

func (p *fakePool) Acquire(context.Context) (gpool.Conn, error) { return nil, nil }
func (p *fakePool) Stat() gpool.Stat                            { return nil }

func (p *fakePool) Exec(context.Context, string, ...any) (gpool.Result, error) { return nil, nil }
func (p *fakePool) Query(context.Context, string, ...any) (gpool.Rows, error)  { return nil, nil }
func (p *fakePool) QueryRow(context.Context, string, ...any) gpool.Row         { return nil }

// fakeSubscriber is a cdc.Subscriber that records how often it was closed and can
// be scripted to fail on close.
type fakeSubscriber struct {
	closes   atomic.Int32
	closeErr error
}

var (
	_ cdc.Subscriber = (*fakeSubscriber)(nil)
	// ReplicationManager is optional now, so a vendor that has it must still be
	// reachable by assertion. This proves the assertion path stays compilable.
	_ cdc.ReplicationManager = (*fakeSubscriber)(nil)
)

func (s *fakeSubscriber) Close() error {
	s.closes.Add(1)
	return s.closeErr
}

func (s *fakeSubscriber) Subscribe(context.Context) (cdc.EventStream, error) { return nil, nil }

func (s *fakeSubscriber) SubscribeFrom(context.Context, cdc.Position) (cdc.EventStream, error) {
	return nil, nil
}

func (s *fakeSubscriber) AddTables(context.Context, ...string) error    { return nil }
func (s *fakeSubscriber) RemoveTables(context.Context, ...string) error { return nil }
func (s *fakeSubscriber) SyncTables(context.Context, ...string) error   { return nil }
func (s *fakeSubscriber) IsTracking(string) bool                        { return false }
func (s *fakeSubscriber) GetTables() []string                           { return nil }

func (s *fakeSubscriber) VerifyTable(context.Context, string) (bool, error) { return false, nil }

func (s *fakeSubscriber) CreateSlot(context.Context, string) error                   { return nil }
func (s *fakeSubscriber) DropSlot(context.Context, string) error                     { return nil }
func (s *fakeSubscriber) CreatePublication(context.Context, string, ...string) error { return nil }
func (s *fakeSubscriber) DropPublication(context.Context, string) error              { return nil }

// fakeStream satisfies cdc.EventStream for interface-shape assertions.
type fakeStream struct{}

var _ cdc.EventStream = fakeStream{}

func (fakeStream) All() iter.Seq[cdc.Event] { return func(func(cdc.Event) bool) {} }
func (fakeStream) Close() error             { return nil }
func (fakeStream) Err() error               { return nil }
