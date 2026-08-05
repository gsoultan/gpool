// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRows is a scripted pgx.Rows that counts how often it is closed, which is how
// the tests detect a double-close reaching the driver.
type fakeRows struct {
	remaining int
	closes    atomic.Int32
	scans     atomic.Int32
	err       error
	tag       pgconn.CommandTag
	fields    []pgconn.FieldDescription
	raw       [][]byte
}

var _ pgx.Rows = (*fakeRows)(nil)

func (r *fakeRows) Close()          { r.closes.Add(1) }
func (r *fakeRows) Err() error      { return r.err }
func (r *fakeRows) Next() bool      { return r.advance() }
func (r *fakeRows) Conn() *pgx.Conn { return nil }

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

func (r *fakeRows) CommandTag() pgconn.CommandTag                { return r.tag }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }
func (r *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                          { return r.raw }

// fakeRow is a scripted pgx.Row counting scans.
type fakeRow struct {
	scans atomic.Int32
	err   error
}

var _ pgx.Row = (*fakeRow)(nil)

func (r *fakeRow) Scan(...any) error {
	r.scans.Add(1)
	return r.err
}

// fakeTx is a scripted pgx.Tx counting how often the transaction was settled, which
// is how the tests detect a commit and a deferred rollback both reaching the driver.
type fakeTx struct {
	commits   atomic.Int32
	rollbacks atomic.Int32
	err       error
}

var _ pgx.Tx = (*fakeTx)(nil)

func (t *fakeTx) Commit(context.Context) error {
	t.commits.Add(1)
	return t.err
}

func (t *fakeTx) Rollback(context.Context) error {
	t.rollbacks.Add(1)
	return t.err
}

func (t *fakeTx) Begin(context.Context) (pgx.Tx, error) { return nil, t.err }
func (t *fakeTx) Conn() *pgx.Conn                       { return nil }
func (t *fakeTx) LargeObjects() pgx.LargeObjects        { return pgx.LargeObjects{} }

func (t *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, t.err
}

func (t *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }

func (t *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, t.err
}

func (t *fakeTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, t.err
}

func (t *fakeTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &fakeRows{}, t.err
}

func (t *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row { return &fakeRow{} }
