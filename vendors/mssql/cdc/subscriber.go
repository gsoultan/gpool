// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// captureInstance is one table SQL Server is capturing, and the instance that
// captures it.
type captureInstance struct {
	schema string
	table  string
	name   string
}

// SQLServer streams changes from SQL Server's change tables.
//
// It implements cdc.Subscriber and not cdc.ReplicationManager: there is no slot
// and no publication here. What SQL Server does have — per-table capture
// instances — is what TableManager already describes, so AddTables really does
// enable capture on the server rather than filtering on the client.
type SQLServer struct {
	config Config
	db     *sql.DB

	mu        sync.Mutex
	tables    []string
	closed    bool
	streaming bool
	stream    *sqlEventStream
}

var _ cdc.Subscriber = (*SQLServer)(nil)

// New creates a subscriber. It opens the connection used to read the change
// tables; no polling starts until Subscribe.
func New(config Config) (*SQLServer, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlserver", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: opening the connection: %w", ErrInvalidConfig, err)
	}
	// One connection: this reads change tables on a timer and answers occasional
	// catalog queries. Holding more against a server it is already polling is waste.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &SQLServer{
		config: config,
		db:     db,
		tables: slices.Clone(config.Tables),
	}, nil
}

// Subscribe starts streaming from the current end of the change tables.
//
// Nothing already captured is delivered. SQL Server keeps no record of where a
// consumer got to, so there is no earlier position to resume from unless the
// consumer kept one — see SubscribeFrom.
func (s *SQLServer) Subscribe(ctx context.Context) (cdc.EventStream, error) {
	if err := s.requireCDC(ctx); err != nil {
		return nil, err
	}

	var lsn []byte
	if err := s.db.QueryRowContext(ctx, maxLSNSQL).Scan(&lsn); err != nil {
		return nil, fmt.Errorf("gpool/mssql/cdc: reading the maximum LSN: %w", err)
	}

	// A NULL maximum means the capture job has not written anything anywhere in
	// this database yet, which is the normal state for the first seconds after
	// enabling capture. It is not an error and it is not "CDC is off" — the log
	// is simply empty, so its beginning and its end are the same place, and
	// starting from zero is starting from now. The per-instance clamp in the poll
	// loop keeps that honest as instances begin producing.
	if len(lsn) != lsnLen {
		return s.subscribe(ctx, make([]byte, lsnLen))
	}

	// One past the last change, so "from now on" does not redeliver the change
	// that happened to be last.
	var from []byte
	if err := s.db.QueryRowContext(ctx, incrementLSNSQL, lsn).Scan(&from); err != nil {
		return nil, fmt.Errorf("gpool/mssql/cdc: advancing past %s: %w", position(lsn), err)
	}
	if len(from) != lsnLen {
		return nil, fmt.Errorf("gpool/mssql/cdc: advancing past %s returned %d bytes, want %d",
			position(lsn), len(from), lsnLen)
	}
	return s.subscribe(ctx, from)
}

// SubscribeFrom starts streaming from a position this vendor produced earlier.
//
// The position must still be inside the retained history. SQL Server's cleanup
// job discards changes on a fixed schedule — three days by default — regardless
// of any consumer, so a position older than that names changes that no longer
// exist. That is refused rather than quietly started from whatever remains.
func (s *SQLServer) SubscribeFrom(ctx context.Context, after cdc.Position) (cdc.EventStream, error) {
	if after == cdc.NoPosition {
		return s.Subscribe(ctx)
	}
	if err := s.requireCDC(ctx); err != nil {
		return nil, err
	}

	from, err := parsePosition(after)
	if err != nil {
		return nil, err
	}
	if err := s.checkRetained(ctx, from, after); err != nil {
		return nil, err
	}
	return s.subscribe(ctx, from)
}

// checkRetained refuses a position the cleanup job has already passed.
func (s *SQLServer) checkRetained(ctx context.Context, from []byte, after cdc.Position) error {
	instances, err := s.trackedInstances(ctx)
	if err != nil {
		return err
	}

	for _, instance := range instances {
		var oldest []byte
		if err := s.db.QueryRowContext(ctx, minLSNSQL, instance.name).Scan(&oldest); err != nil {
			return fmt.Errorf("gpool/mssql/cdc: reading the oldest retained LSN for %s: %w",
				describe(instance.schema, instance.table), err)
		}
		if oldest == nil {
			continue
		}
		if compareLSN(from, oldest) < 0 {
			return fmt.Errorf("%w: asked to resume from %s, but %s retains nothing before %s",
				ErrPositionExpired, after, describe(instance.schema, instance.table), position(oldest))
		}
	}
	return nil
}

func (s *SQLServer) subscribe(ctx context.Context, from []byte) (cdc.EventStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrClosed
	}
	if s.streaming {
		return nil, ErrAlreadySubscribed
	}

	stream := newEventStream(ctx, &sqlEventStream{
		db:       s.db,
		subject:  s,
		interval: s.config.PollInterval,
		timeout:  s.config.QueryTimeout,
		events:   make(chan cdc.Event, s.config.Buffer),
		onClose:  s.releaseStream,
		from:     from,
	})
	s.stream = stream
	s.streaming = true
	return stream, nil
}

func (s *SQLServer) releaseStream() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stream = nil
	s.streaming = false
}

// requireCDC reports a database with no change data capture, which is otherwise
// indistinguishable from one where nothing has changed.
func (s *SQLServer) requireCDC(ctx context.Context) error {
	if err := s.usable(); err != nil {
		return err
	}

	var enabled bool
	if err := s.db.QueryRowContext(ctx, databaseEnabledSQL).Scan(&enabled); err != nil {
		return fmt.Errorf("gpool/mssql/cdc: checking whether CDC is enabled: %w", err)
	}
	if !enabled {
		return fmt.Errorf("%w: run sys.sp_cdc_enable_db, or call AddTables which does it", ErrCDCNotEnabled)
	}
	return nil
}

// trackedInstances lists the capture instances this subscriber streams from.
//
// The catalog is the source of truth rather than the configured list: capture is
// server-side state, and a table named in the config but never enabled has no
// changes to read.
func (s *SQLServer) trackedInstances(ctx context.Context) ([]captureInstance, error) {
	rows, err := s.db.QueryContext(ctx, captureInstancesSQL)
	if err != nil {
		return nil, fmt.Errorf("gpool/mssql/cdc: listing capture instances: %w", err)
	}
	defer rows.Close()

	wanted := s.trackedSet()
	var instances []captureInstance

	for rows.Next() {
		var found captureInstance
		if err := rows.Scan(&found.schema, &found.table, &found.name); err != nil {
			return nil, err
		}
		if !captureInstancePattern.MatchString(found.name) {
			// The name is interpolated into a function name, where it cannot be
			// bound or quoted. Anything that does not look like an identifier is
			// skipped rather than concatenated into SQL.
			continue
		}
		if len(wanted) > 0 && !wanted[qualify(found.schema, found.table)] {
			continue
		}
		instances = append(instances, found)
	}
	return instances, rows.Err()
}

// trackedSet is the configured table set, or empty meaning "every captured table".
func (s *SQLServer) trackedSet() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.tables) == 0 {
		return nil
	}
	set := make(map[string]bool, len(s.tables))
	for _, table := range s.tables {
		schema, name, err := splitQualified(table)
		if err == nil {
			set[qualify(schema, name)] = true
		}
	}
	return set
}

// AddTables enables capture on each table, which is DDL against the server
// rather than a client-side filter.
func (s *SQLServer) AddTables(ctx context.Context, tables ...string) error {
	if len(tables) == 0 {
		return ErrNoTables
	}
	if err := s.usable(); err != nil {
		return err
	}

	if _, err := s.db.ExecContext(ctx, enableDatabaseSQL); err != nil {
		return fmt.Errorf("gpool/mssql/cdc: enabling CDC on the database: %w", err)
	}

	for _, table := range tables {
		schema, name, err := splitQualified(table)
		if err != nil {
			return err
		}
		if err := s.enableTable(ctx, schema, name); err != nil {
			return err
		}
		s.track(qualify(schema, name))
	}
	return nil
}

// enableTable turns on capture for one table, tolerating one that already has it.
func (s *SQLServer) enableTable(ctx context.Context, schema, table string) error {
	const stmt = `IF NOT EXISTS (
	SELECT 1 FROM cdc.change_tables ct
	JOIN sys.tables t ON ct.source_object_id = t.object_id
	JOIN sys.schemas s ON t.schema_id = s.schema_id
	WHERE s.name = @p1 AND t.name = @p2)
EXEC sys.sp_cdc_enable_table @source_schema = @p1, @source_name = @p2,
	@role_name = NULL, @supports_net_changes = 0`

	if _, err := s.db.ExecContext(ctx, stmt, schema, table); err != nil {
		return fmt.Errorf("gpool/mssql/cdc: enabling capture on %s: %w", describe(schema, table), err)
	}
	return nil
}

// RemoveTables disables capture, which discards that table's change history.
func (s *SQLServer) RemoveTables(ctx context.Context, tables ...string) error {
	if len(tables) == 0 {
		return ErrNoTables
	}
	if err := s.usable(); err != nil {
		return err
	}

	const stmt = `IF EXISTS (
	SELECT 1 FROM cdc.change_tables ct
	JOIN sys.tables t ON ct.source_object_id = t.object_id
	JOIN sys.schemas s ON t.schema_id = s.schema_id
	WHERE s.name = @p1 AND t.name = @p2)
EXEC sys.sp_cdc_disable_table @source_schema = @p1, @source_name = @p2, @capture_instance = N'all'`

	for _, table := range tables {
		schema, name, err := splitQualified(table)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, stmt, schema, name); err != nil {
			return fmt.Errorf("gpool/mssql/cdc: disabling capture on %s: %w", describe(schema, name), err)
		}
		s.untrack(qualify(schema, name))
	}
	return nil
}

// SyncTables reconciles the captured set to exactly these tables.
func (s *SQLServer) SyncTables(ctx context.Context, tables ...string) error {
	if err := s.usable(); err != nil {
		return err
	}

	wanted := make(map[string]bool, len(tables))
	for _, table := range tables {
		schema, name, err := splitQualified(table)
		if err != nil {
			return err
		}
		wanted[qualify(schema, name)] = true
	}

	current, err := s.trackedInstances(ctx)
	if err != nil {
		return err
	}
	for _, instance := range current {
		if !wanted[qualify(instance.schema, instance.table)] {
			if err := s.RemoveTables(ctx, describe(instance.schema, instance.table)); err != nil {
				return err
			}
		}
	}
	if len(tables) > 0 {
		return s.AddTables(ctx, tables...)
	}
	return nil
}

// IsTracking reports whether a table is in this subscriber's set.
func (s *SQLServer) IsTracking(table string) bool {
	schema, name, err := splitQualified(table)
	if err != nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.ContainsFunc(s.tables, func(t string) bool {
		ts, tn, err := splitQualified(t)
		return err == nil && qualify(ts, tn) == qualify(schema, name)
	})
}

// GetTables returns the tracked set. Empty means every captured table.
func (s *SQLServer) GetTables() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.tables)
}

// VerifyTable reports whether the server is actually capturing the table, which
// is stronger than asking whether the config mentions it.
func (s *SQLServer) VerifyTable(ctx context.Context, table string) (bool, error) {
	if err := s.usable(); err != nil {
		return false, err
	}

	schema, name, err := splitQualified(table)
	if err != nil {
		return false, err
	}

	const stmt = `SELECT COUNT(*) FROM cdc.change_tables ct
JOIN sys.tables t ON ct.source_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @p1 AND t.name = @p2`

	var found int
	if err := s.db.QueryRowContext(ctx, stmt, schema, name).Scan(&found); err != nil {
		return false, fmt.Errorf("gpool/mssql/cdc: verifying %s: %w", describe(schema, name), err)
	}
	return found > 0, nil
}

// Close stops any open stream and releases the connection. It is idempotent.
func (s *SQLServer) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	stream := s.stream
	s.stream = nil
	s.mu.Unlock()

	// Closing the stream runs releaseStream, which takes s.mu, so the lock is
	// dropped first.
	if stream != nil {
		_ = stream.Close()
	}
	return s.db.Close()
}

func (s *SQLServer) usable() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *SQLServer) track(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !slices.Contains(s.tables, name) {
		s.tables = append(s.tables, name)
	}
}

func (s *SQLServer) untrack(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tables = slices.DeleteFunc(s.tables, func(t string) bool {
		ts, tn, err := splitQualified(t)
		return err == nil && qualify(ts, tn) == name
	})
}

// qualify renders a schema and table the way the tracked set matches them.
//
// Case is folded because a SQL Server collation is usually case-insensitive for
// identifiers, so matching case sensitively would make the same configuration
// behave differently against two servers holding the same schema.
func qualify(schema, table string) string {
	return strings.ToLower(schema + "." + table)
}
