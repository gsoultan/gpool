// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"sync/atomic"

	"github.com/gsoultan/gpool/pkg/vendors/postgres/wire"
)

// fakeRows is a scripted resultSet that counts how often it is closed, which is
// how the tests detect a double-close reaching the connection.
type fakeRows struct {
	remaining int
	closes    atomic.Int32
	scans     atomic.Int32
	err       error
	tag       string
	fields    []wire.Field
	raw       [][]byte
}

var _ resultSet = (*fakeRows)(nil)

func (r *fakeRows) Close()               { r.closes.Add(1) }
func (r *fakeRows) Err() error           { return r.err }
func (r *fakeRows) Next() bool           { return r.advance() }
func (r *fakeRows) Tag() string          { return r.tag }
func (r *fakeRows) Fields() []wire.Field { return r.fields }
func (r *fakeRows) RawValues() [][]byte  { return r.raw }

func (r *fakeRows) advance() bool {
	if r.remaining <= 0 {
		return false
	}
	r.remaining--
	return true
}

func (r *fakeRows) Scan(...any) error {
	r.scans.Add(1)
	return r.err
}

// fakeTx is a scripted transaction counting how often it was settled, which is
// how the tests detect a commit and a deferred rollback both reaching the server.
type fakeTx struct {
	commits   atomic.Int32
	rollbacks atomic.Int32
	err       error
}

var _ transaction = (*fakeTx)(nil)

func (t *fakeTx) Commit(context.Context) error {
	t.commits.Add(1)
	return t.err
}

func (t *fakeTx) Rollback(context.Context) error {
	t.rollbacks.Add(1)
	return t.err
}

func (t *fakeTx) Exec(context.Context, string, []any) (string, error) { return "", t.err }

func (t *fakeTx) Query(context.Context, string, []any) (resultSet, error) {
	return &fakeRows{}, t.err
}
