// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"fmt"
	"regexp"
	"strings"
)

// captureInstancePattern is what SQL Server accepts for a capture instance name.
//
// The name reaches SQL as part of a function name — cdc.fn_cdc_get_all_changes_X
// — where it cannot be a bound parameter and cannot be quoted into safety. It is
// read back from the catalog rather than taken from a caller, but validating it
// anyway is what stops a table named to look like SQL from becoming SQL.
var captureInstancePattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,128}$`)

// identifierPattern bounds the schema and table names that reach sp_cdc_enable_table.
// They are passed as parameters there, so this guards the capture instance name
// derived from them rather than the DDL itself.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,127}$`)

const (
	// databaseEnabledSQL reports whether the current database has CDC at all.
	databaseEnabledSQL = `SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()`

	// captureInstancesSQL lists every table with capture enabled, and the instance
	// that captures it.
	captureInstancesSQL = `SELECT s.name, t.name, ct.capture_instance
FROM cdc.change_tables ct
JOIN sys.tables t ON ct.source_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
ORDER BY s.name, t.name`

	// maxLSNSQL is the high-water mark of what the capture job has written.
	maxLSNSQL = `SELECT sys.fn_cdc_get_max_lsn()`

	// minLSNSQL is the oldest change still retained for an instance. Reading from
	// before it returns nothing rather than failing, which is why it is checked.
	minLSNSQL = `SELECT sys.fn_cdc_get_min_lsn(@p1)`

	// incrementLSNSQL steps one past an LSN, so a window that is inclusive at both
	// ends does not redeliver its last change on the next poll.
	//
	// The CONVERT is load-bearing. The capture functions take binary(10), the
	// driver sends a byte slice as varbinary, and the mismatch does not raise an
	// error — it returns NULL, which then reaches the next function as a missing
	// argument and surfaces as "an insufficient number of arguments were supplied",
	// several calls away from the actual cause.
	incrementLSNSQL = `SELECT sys.fn_cdc_increment_lsn(CONVERT(binary(10), @p1))`

	// enableDatabaseSQL turns on CDC for the database. It is a no-op if already on.
	enableDatabaseSQL = `IF (SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) = 0
EXEC sys.sp_cdc_enable_db`
)

// changesSQL reads one capture instance's changes in (from, to].
//
// "all update old" rather than "all": it makes an update arrive as a pair, the
// before image and the after image, which is what lets Event.Before be populated
// the way the other vendors populate it.
//
// The commit time is mapped here rather than in Go because only the server can
// do it — the mapping lives in a system table, not in the LSN.
func changesSQL(instance string) string {
	return fmt.Sprintf(`SELECT sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS __$commit_time, *
FROM cdc.fn_cdc_get_all_changes_%s(CONVERT(binary(10), @p1), CONVERT(binary(10), @p2), N'all update old')
ORDER BY __$start_lsn, __$seqval`, instance)
}

// splitQualified separates "schema.table" and checks both halves are identifiers.
func splitQualified(table string) (schema, name string, err error) {
	schema, name, found := strings.Cut(strings.TrimSpace(table), ".")
	if !found {
		schema, name = "dbo", schema
	}
	if !identifierPattern.MatchString(schema) || !identifierPattern.MatchString(name) {
		return "", "", fmt.Errorf("%w: %q is not a schema-qualified identifier", ErrInvalidConfig, table)
	}
	return schema, name, nil
}

// instanceName is the capture instance sp_cdc_enable_table creates by default.
func instanceName(schema, table string) string {
	return schema + "_" + table
}
