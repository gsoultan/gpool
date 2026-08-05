// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package sqldriver

import (
	"context"
	"database/sql/driver"
	"io"
	"sync/atomic"
)

// fakeConnector hands out scripted connections.
type fakeConnector struct {
	connects atomic.Int32
	rows     [][]driver.Value
	columns  []string
	queryErr error
	execErr  error

	// omitResetter drops the SessionResetter implementation, so tests can cover a
	// driver that does not offer one.
	omitResetter bool
}

var _ driver.Connector = (*fakeConnector)(nil)

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	c.connects.Add(1)
	if c.omitResetter {
		return &bareConn{owner: c}, nil
	}
	return &fakeDriverConn{owner: c}, nil
}

func (c *fakeConnector) Driver() driver.Driver { return nil }

// bareConn implements only the minimum a driver.Conn must, so the adapter's
// fallbacks are exercised rather than its fast paths.
type bareConn struct {
	owner  *fakeConnector
	closed atomic.Int32
}

var _ driver.Conn = (*bareConn)(nil)

func (c *bareConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{owner: c.owner, columns: len(c.owner.columns)}, nil
}
func (c *bareConn) Close() error              { c.closed.Add(1); return nil }
func (c *bareConn) Begin() (driver.Tx, error) { return &fakeTx{}, nil }

// fakeDriverConn implements the optional interfaces a real driver would.
type fakeDriverConn struct {
	owner *fakeConnector

	closed   atomic.Int32
	resets   atomic.Int32
	rollback atomic.Int32
	commits  atomic.Int32
	invalid  atomic.Bool
	resetErr error
}

var (
	_ driver.Conn               = (*fakeDriverConn)(nil)
	_ driver.ExecerContext      = (*fakeDriverConn)(nil)
	_ driver.QueryerContext     = (*fakeDriverConn)(nil)
	_ driver.ConnBeginTx        = (*fakeDriverConn)(nil)
	_ driver.Pinger             = (*fakeDriverConn)(nil)
	_ driver.Validator          = (*fakeDriverConn)(nil)
	_ driver.SessionResetter    = (*fakeDriverConn)(nil)
	_ driver.ConnPrepareContext = (*fakeDriverConn)(nil)
)

func (c *fakeDriverConn) Prepare(string) (driver.Stmt, error) {
	return &fakeStmt{owner: c.owner, columns: len(c.owner.columns)}, nil
}

func (c *fakeDriverConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return &fakeStmt{owner: c.owner, columns: len(c.owner.columns)}, nil
}

func (c *fakeDriverConn) Close() error { c.closed.Add(1); return nil }

func (c *fakeDriverConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeDriverConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &fakeTx{conn: c}, nil
}

func (c *fakeDriverConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if c.owner.execErr != nil {
		return nil, c.owner.execErr
	}
	return fakeResult{affected: 1, lastID: 7}, nil
}

func (c *fakeDriverConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.owner.queryErr != nil {
		return nil, c.owner.queryErr
	}
	return &fakeRows{columns: c.owner.columns, remaining: c.owner.rows}, nil
}

func (c *fakeDriverConn) Ping(context.Context) error { return nil }
func (c *fakeDriverConn) IsValid() bool              { return !c.invalid.Load() }

func (c *fakeDriverConn) ResetSession(context.Context) error {
	c.resets.Add(1)
	return c.resetErr
}

// fakeTx records how it was settled.
type fakeTx struct {
	conn *fakeDriverConn
}

var _ driver.Tx = (*fakeTx)(nil)

func (t *fakeTx) Commit() error {
	if t.conn != nil {
		t.conn.commits.Add(1)
	}
	return nil
}

func (t *fakeTx) Rollback() error {
	if t.conn != nil {
		t.conn.rollback.Add(1)
	}
	return nil
}

// fakeStmt covers the prepare-then-execute fallback path.
type fakeStmt struct {
	owner   *fakeConnector
	columns int
	closed  atomic.Int32
}

var (
	_ driver.Stmt             = (*fakeStmt)(nil)
	_ driver.StmtExecContext  = (*fakeStmt)(nil)
	_ driver.StmtQueryContext = (*fakeStmt)(nil)
)

func (s *fakeStmt) Close() error  { s.closed.Add(1); return nil }
func (s *fakeStmt) NumInput() int { return -1 }

func (s *fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return fakeResult{affected: 1}, nil
}

func (s *fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	return &fakeRows{columns: s.owner.columns, remaining: s.owner.rows}, nil
}

func (s *fakeStmt) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return fakeResult{affected: 1}, nil
}

func (s *fakeStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return &fakeRows{columns: s.owner.columns, remaining: s.owner.rows}, nil
}

// fakeRows replays a scripted result set.
type fakeRows struct {
	columns   []string
	remaining [][]driver.Value
	closes    atomic.Int32
}

var _ driver.Rows = (*fakeRows)(nil)

func (r *fakeRows) Columns() []string { return r.columns }
func (r *fakeRows) Close() error      { r.closes.Add(1); return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if len(r.remaining) == 0 {
		return io.EOF
	}
	copy(dest, r.remaining[0])
	r.remaining = r.remaining[1:]
	return nil
}

// fakeResult is a scripted driver.Result.
type fakeResult struct {
	affected int64
	lastID   int64
}

var _ driver.Result = fakeResult{}

func (r fakeResult) RowsAffected() (int64, error) { return r.affected, nil }
func (r fakeResult) LastInsertId() (int64, error) { return r.lastID, nil }
