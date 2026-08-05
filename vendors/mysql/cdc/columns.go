// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/go-mysql-org/go-mysql/replication"
)

// columnNamesSQL reads a table's columns in declaration order, which is the
// order a binlog row's values arrive in.
const columnNamesSQL = `SELECT COLUMN_NAME FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION`

// columns resolves the column names for a binlog row.
//
// A binlog row is a list of values with no names attached. Where they come from
// depends on the source's configuration, and the difference is worth
// understanding before deploying:
//
//   - binlog_row_metadata = FULL puts the names in the binary log itself. They
//     are then correct by construction, including for rows written before a
//     later ALTER TABLE, and no query is needed. This is the setting to use.
//   - binlog_row_metadata = MINIMAL, the default, puts nothing in the log, so
//     the names have to be read from information_schema — which describes the
//     table as it is *now*, not as it was when the row was written.
//
// Under MINIMAL a resumed stream reading rows from before an ALTER TABLE is
// genuinely ambiguous. Rather than hand the consumer values under names that
// may not be theirs, a column count that disagrees is reported as
// ErrSchemaMismatch.
type columns struct {
	db *sql.DB

	mu    sync.Mutex
	cache map[string][]string
}

func newColumns(db *sql.DB) *columns {
	return &columns{db: db, cache: make(map[string][]string)}
}

// names returns the column names for a table map event carrying want values.
func (c *columns) names(ctx context.Context, table *replication.TableMapEvent, want int) ([]string, error) {
	schema, name := string(table.Schema), string(table.Table)

	// The log's own metadata needs no round trip and survives a later ALTER.
	if fromLog := table.ColumnNameString(); len(fromLog) == want {
		return fromLog, nil
	}

	key := qualify(schema, name)
	if cached, ok := c.lookup(key); ok {
		if len(cached) == want {
			return cached, nil
		}
		// The table has been altered since it was cached. Drop it and ask again
		// before concluding anything.
		c.forget(key)
	}

	loaded, err := c.load(ctx, schema, name)
	if err != nil {
		return nil, err
	}
	if len(loaded) != want {
		return nil, fmt.Errorf("%w: %s has %d columns but the binlog row has %d; "+
			"set binlog_row_metadata=FULL so the names travel with the row",
			ErrSchemaMismatch, key, len(loaded), want)
	}

	c.store(key, loaded)
	return loaded, nil
}

func (c *columns) lookup(key string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	names, ok := c.cache[key]
	return names, ok
}

func (c *columns) store(key string, names []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[key] = names
}

func (c *columns) forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, key)
}

// load reads a table's column names from the catalog.
func (c *columns) load(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, columnNamesSQL, schema, table)
	if err != nil {
		return nil, fmt.Errorf("gpool/mysql/cdc: reading columns of %s: %w", qualify(schema, table), err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("gpool/mysql/cdc: reading columns of %s: %w", qualify(schema, table), err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gpool/mysql/cdc: reading columns of %s: %w", qualify(schema, table), err)
	}
	return names, nil
}
