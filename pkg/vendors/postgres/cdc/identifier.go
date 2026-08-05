// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// SQLSTATE codes used to classify catalog errors. Matching on codes rather than on
// message text keeps behaviour correct on servers with a non-English lc_messages.
const (
	codeDuplicateObject = "42710"
	codeDuplicateTable  = "42P07"
	codeUndefinedObject = "42704"
	codeUndefinedTable  = "42P01"
)

// quoteIdentifier renders s as a single quoted SQL identifier.
func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteQualifiedName renders a possibly schema-qualified table name.
//
// Quoting "public.users" as one identifier would name a table literally called
// "public.users" rather than users in schema public, so the parts are split and
// quoted separately. A name with no dot is left for the server to resolve against
// its search_path.
func quoteQualifiedName(s string) string {
	schema, table := splitQualifiedName(s)
	if schema == "" {
		return quoteIdentifier(table)
	}
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

// splitQualifiedName splits "schema.table" into its parts. An unqualified name
// returns an empty schema.
func splitQualifiedName(s string) (schema, table string) {
	before, after, found := strings.Cut(s, ".")
	if !found || before == "" || after == "" {
		return "", s
	}
	return before, after
}

// quoteQualifiedNames renders a list of table names for use in a table list.
func quoteQualifiedNames(tables []string) string {
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = quoteQualifiedName(t)
	}
	return strings.Join(quoted, ", ")
}

// quoteLiteral renders s as a SQL string literal.
func quoteLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

// hasSQLState reports whether err carries any of the given SQLSTATE codes.
func hasSQLState(err error, codes ...string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	for _, code := range codes {
		if pgErr.Code == code {
			return true
		}
	}
	return false
}

// isDuplicateObject reports whether err means the object already exists.
func isDuplicateObject(err error) bool {
	return hasSQLState(err, codeDuplicateObject, codeDuplicateTable)
}

// isUndefinedObject reports whether err means the object does not exist.
func isUndefinedObject(err error) bool {
	return hasSQLState(err, codeUndefinedObject, codeUndefinedTable)
}

// dedupe returns the input with empty and repeated entries removed, order preserved.
func dedupe(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// difference returns the entries of values that are absent from other.
func difference(values, other []string) []string {
	if len(values) == 0 {
		return nil
	}

	exclude := make(map[string]struct{}, len(other))
	for _, v := range other {
		exclude[v] = struct{}{}
	}

	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := exclude[v]; ok {
			continue
		}
		out = append(out, v)
	}
	return out
}

// intersection returns the entries of values that are also present in other.
func intersection(values, other []string) []string {
	if len(values) == 0 || len(other) == 0 {
		return nil
	}

	include := make(map[string]struct{}, len(other))
	for _, v := range other {
		include[v] = struct{}{}
	}

	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := include[v]; ok {
			out = append(out, v)
		}
	}
	return out
}
