// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"slices"
	"strings"
	"sync"
)

// filter is the set of tables a stream delivers.
//
// MySQL has no per-consumer subscription, so unlike a PostgreSQL publication
// this narrows nothing at the server: the whole binary log still crosses the
// network and the filter is applied here. It is shared between the subscriber
// and its open stream so that adding a table takes effect on a running stream,
// which is what makes TableManager useful rather than decorative.
type filter struct {
	mu     sync.RWMutex
	tables map[string]struct{}
}

func newFilter(tables []string) *filter {
	f := &filter{tables: make(map[string]struct{}, len(tables))}
	f.set(tables)
	return f
}

// allows reports whether changes to a table should be delivered. An empty filter
// allows everything, which is the only sensible reading of "no tables named".
func (f *filter) allows(schema, table string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if len(f.tables) == 0 {
		return true
	}
	_, ok := f.tables[qualify(schema, table)]
	return ok
}

func (f *filter) set(tables []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	clear(f.tables)
	for _, table := range tables {
		f.tables[normalize(table)] = struct{}{}
	}
}

func (f *filter) add(tables []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, table := range tables {
		f.tables[normalize(table)] = struct{}{}
	}
}

func (f *filter) remove(tables []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, table := range tables {
		delete(f.tables, normalize(table))
	}
}

func (f *filter) has(table string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	_, ok := f.tables[normalize(table)]
	return ok
}

// list returns the tracked tables, sorted so the result is stable.
func (f *filter) list() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	tables := make([]string, 0, len(f.tables))
	for table := range f.tables {
		tables = append(tables, table)
	}
	slices.Sort(tables)
	return tables
}

// qualify joins a schema and table as they are matched.
func qualify(schema, table string) string {
	return strings.ToLower(schema + "." + table)
}

// normalize renders a configured name the way qualify renders a binlog one.
//
// Case is folded because MySQL's own table-name case sensitivity depends on
// lower_case_table_names and therefore on the host filesystem. Matching case
// sensitively would make the same configuration behave differently on Linux and
// macOS.
func normalize(table string) string {
	return strings.ToLower(strings.TrimSpace(table))
}
