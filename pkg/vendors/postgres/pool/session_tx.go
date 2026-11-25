// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"
	"errors"

	"github.com/gsoultan/caduceus"
)

// sessionTx is a transaction on a live connection. caduceus holds its BEGIN
// back until the first statement and sends the two together, and it is the
// caduceus transaction that knows whether it is still the one open on the
// connection, so everything here goes through it and not around it.
type sessionTx struct {
	tx *caduceus.Tx
}

var _ transaction = sessionTx{}

// Commit sends COMMIT, or nothing for a transaction that ran no statement. A
// transaction that had already failed is rolled back by the server, which says
// so in the command tag rather than with an error.
func (t sessionTx) Commit(ctx context.Context) error {
	return inOurWords(t.tx.Commit(ctx))
}

func (t sessionTx) Rollback(ctx context.Context) error {
	return inOurWords(t.tx.Rollback(ctx))
}

func (t sessionTx) Exec(ctx context.Context, sql string, args []any) (string, error) {
	args, err := bindable(args)
	if err != nil {
		return "", err
	}
	tag, err := t.tx.Exec(ctx, sql, args...)
	return tag, inOurWords(err)
}

func (t sessionTx) Query(ctx context.Context, sql string, args []any) (resultSet, error) {
	args, err := bindable(args)
	if err != nil {
		return nil, err
	}
	cursor, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, inOurWords(err)
	}
	return cursor, nil
}

// inOurWords maps caduceus's transaction sentinels onto this package's, so a
// caller matches on one vocabulary rather than reaching through to the driver.
// caduceus.ErrTxClosed is how a transaction answers once the pool has unwound
// its connection for someone else.
func inOurWords(err error) error {
	switch {
	case errors.Is(err, caduceus.ErrTxCommitRollback):
		return ErrTxCommitRollback
	case errors.Is(err, caduceus.ErrTxClosed):
		return ErrTxClosed
	}
	return err
}
