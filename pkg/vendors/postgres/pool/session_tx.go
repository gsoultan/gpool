// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import "context"

// sessionTx is a transaction on a live connection: BEGIN has run, and the
// statements that follow go to the same session.
type sessionTx struct {
	conn *pgConn
}

var _ transaction = sessionTx{}

// Commit sends COMMIT. A transaction that had already failed is rolled back by
// the server, which says so in the command tag rather than with an error.
func (t sessionTx) Commit(ctx context.Context) error {
	tag, err := t.conn.session.exec(ctx, "COMMIT", nil)
	if err != nil {
		return err
	}
	if tag == "ROLLBACK" {
		return ErrTxCommitRollback
	}
	return nil
}

func (t sessionTx) Rollback(ctx context.Context) error {
	_, err := t.conn.session.exec(ctx, "ROLLBACK", nil)
	return err
}

func (t sessionTx) Exec(ctx context.Context, sql string, args []any) (string, error) {
	return t.conn.session.exec(ctx, sql, args)
}

func (t sessionTx) Query(ctx context.Context, sql string, args []any) (resultSet, error) {
	cursor, err := t.conn.session.query(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	return cursor, nil
}
