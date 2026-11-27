// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"context"

	"github.com/gsoultan/caduceus"
	"github.com/gsoultan/caduceus/wire"
	"github.com/gsoultan/gpool/pkg/gpool"
)

// cursor is a result being read off a connection.
type cursor = caduceus.Rows

// session is one connection's caduceus driver, and the wire connection under
// it, which the pool inspects for liveness and transaction status.
//
// It is also where gpool's argument conventions meet caduceus's: a
// database/sql driver.Valuer, such as sql.NullString, is resolved to its value
// here, since caduceus does not import database/sql.
type session struct {
	db   *caduceus.Conn
	conn *wire.Conn
}

// newSession runs queries on conn, keeping up to statements prepared on the
// server, or none. With none kept there it remembers what the server said of
// up to descriptions statements, or of none. caduceus reads zero as its
// default, so none is passed as a negative.
func newSession(conn *wire.Conn, statements, descriptions int) *session {
	options := caduceus.Options{StatementCacheCapacity: statements, DescriptionCacheCapacity: descriptions}
	if statements <= 0 {
		options.StatementCacheCapacity = -1
	}
	if descriptions <= 0 {
		options.DescriptionCacheCapacity = -1
	}
	return &session{db: caduceus.New(conn, options), conn: conn}
}

func (s *session) busy() bool { return s.db.Busy() }

func (s *session) forgetStatements() { s.db.ForgetStatements() }

func (s *session) exec(ctx context.Context, sql string, args []any) (caduceus.CommandTag, error) {
	args, err := bindable(args)
	if err != nil {
		return "", err
	}
	return s.db.Exec(ctx, sql, args...)
}

func (s *session) query(ctx context.Context, sql string, args []any) (*cursor, error) {
	args, err := bindable(args)
	if err != nil {
		return nil, err
	}
	return s.db.Query(ctx, sql, args...)
}

func (s *session) queryInto(ctx context.Context, sql string, args []any, c *cursor) error {
	args, err := bindable(args)
	if err != nil {
		return err
	}
	return s.db.QueryInto(ctx, c, sql, args...)
}

func (s *session) batch(ctx context.Context, queries []gpool.BatchQuery) (*caduceus.BatchResults, error) {
	converted := make([]caduceus.Query, len(queries))
	for i, query := range queries {
		args, err := bindable(query.Arguments)
		if err != nil {
			return nil, err
		}
		converted[i] = caduceus.Query{SQL: query.SQL, Arguments: args}
	}
	return s.db.SendBatch(ctx, converted)
}

func (s *session) copyFrom(ctx context.Context, request gpool.CopyRequest) (int64, error) {
	return s.db.CopyFrom(ctx, request.Table, request.Columns, copySource{request.Rows})
}
