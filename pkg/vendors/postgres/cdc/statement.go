// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// verifyTableSQL checks whether a table is covered by a publication. The schema
// parameter is optional: an empty value matches the table in any schema, which is
// how an unqualified name behaves.
const verifyTableSQL = `SELECT 1 FROM pg_publication_tables
WHERE pubname = $1 AND tablename = $3 AND ($2 = '' OR schemaname = $2)
LIMIT 1`

func createPublicationSQL(name string, tables []string) string {
	return fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		quoteIdentifier(name), quoteQualifiedNames(tables))
}

func setPublicationTablesSQL(name string, tables []string) string {
	return fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s",
		quoteIdentifier(name), quoteQualifiedNames(tables))
}

func addPublicationTablesSQL(name string, tables []string) string {
	return fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s",
		quoteIdentifier(name), quoteQualifiedNames(tables))
}

func dropPublicationTablesSQL(name string, tables []string) string {
	return fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s",
		quoteIdentifier(name), quoteQualifiedNames(tables))
}

func dropPublicationSQL(name string) string {
	return fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", quoteIdentifier(name))
}

// exec runs a statement that returns no rows, draining the result either way.
func exec(ctx context.Context, conn *pgconn.PgConn, sql string) error {
	return conn.Exec(ctx, sql).Close()
}
