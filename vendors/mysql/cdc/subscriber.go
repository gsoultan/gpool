// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	driver "github.com/go-sql-driver/mysql"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// verifyTableSQL checks that a table exists, which is as much as MySQL can
// confirm: there is no subscription object to check membership of.
const verifyTableSQL = `SELECT 1 FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1`

// MySQL streams changes from a MySQL or MariaDB binary log.
//
// It implements cdc.Subscriber and deliberately not cdc.ReplicationManager:
// there are no slots or publications to administer, and a caller asserting for
// that interface should find it absent rather than find four methods that only
// return errors.
type MySQL struct {
	config  Config
	dsn     *driver.Config
	db      *sql.DB
	columns *columns
	filter  *filter

	mu        sync.Mutex
	closed    bool
	streaming bool
	stream    *mysqlEventStream
}

var _ cdc.Subscriber = (*MySQL)(nil)

// New creates a subscriber. It opens the control connection used to resolve
// column names and read the log's current position; no binlog connection is
// made until Subscribe.
func New(config Config) (*MySQL, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	dsn, err := config.parseDSN()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: opening the control connection: %w", ErrInvalidConfig, err)
	}
	// The control connection answers occasional catalog queries. One connection
	// is enough, and holding more against a server this consumer is already
	// replicating from is waste.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &MySQL{
		config:  config,
		dsn:     dsn,
		db:      db,
		columns: newColumns(db),
		filter:  newFilter(config.Tables),
	}, nil
}

// Subscribe starts streaming from the current end of the binary log.
//
// Nothing before this moment is delivered. MySQL keeps no record of where a
// consumer got to, so there is no earlier position to resume from unless the
// consumer kept one — see SubscribeFrom, which is what makes a restart lossless.
func (m *MySQL) Subscribe(ctx context.Context) (cdc.EventStream, error) {
	start, err := m.currentPosition(ctx)
	if err != nil {
		return nil, err
	}
	return m.subscribe(ctx, start)
}

// SubscribeFrom starts streaming from a position this vendor produced earlier.
//
// The position must still be within the source's retained binary logs. MySQL
// expires them on age and size regardless of any consumer, so a position older
// than binlog_expire_logs_seconds names changes that no longer exist; the source
// refuses the dump rather than serving a stream with a hole in it.
func (m *MySQL) SubscribeFrom(ctx context.Context, after cdc.Position) (cdc.EventStream, error) {
	if after == cdc.NoPosition {
		return m.Subscribe(ctx)
	}

	start, err := parsePosition(after, m.config.Flavor)
	if err != nil {
		return nil, err
	}
	return m.subscribe(ctx, start)
}

// subscribe opens the binlog connection and starts the reader.
func (m *MySQL) subscribe(ctx context.Context, start position) (cdc.EventStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrClosed
	}
	if m.streaming {
		return nil, ErrAlreadySubscribed
	}

	syncer := replication.NewBinlogSyncer(m.syncerConfig())

	streamer, err := m.startSync(syncer, start)
	if err != nil {
		syncer.Close()
		return nil, err
	}

	stream := newEventStream(ctx, &mysqlEventStream{
		syncer:   syncer,
		streamer: streamer,
		columns:  m.columns,
		filter:   m.filter,
		events:   make(chan cdc.Event, m.config.Buffer),
		onClose:  m.releaseStream,
		start:    start,
	})
	m.stream = stream
	m.streaming = true
	return stream, nil
}

func (m *MySQL) startSync(syncer *replication.BinlogSyncer, start position) (*replication.BinlogStreamer, error) {
	if start.isGTID() {
		// The syncer keeps the set it is given and advances it from its own
		// goroutine as transactions arrive. Handing it the same object the
		// stream's tracker updates is a data race on the set's internal map,
		// which -race catches and which would otherwise corrupt a position.
		streamer, err := syncer.StartSyncGTID(start.gtid.Clone())
		if err != nil {
			return nil, fmt.Errorf("gpool/mysql/cdc: starting replication at GTID %s: %w", start.gtid, err)
		}
		return streamer, nil
	}

	streamer, err := syncer.StartSync(start.file)
	if err != nil {
		return nil, fmt.Errorf("gpool/mysql/cdc: starting replication at %s: %w", start.file, err)
	}
	return streamer, nil
}

// syncerConfig translates the vendor config into the binlog reader's.
func (m *MySQL) syncerConfig() replication.BinlogSyncerConfig {
	host, port := splitHostPort(m.dsn.Addr)

	return replication.BinlogSyncerConfig{
		ServerID:        m.config.ServerID,
		Flavor:          m.config.Flavor,
		Host:            host,
		Port:            port,
		User:            m.dsn.User,
		Password:        m.dsn.Passwd,
		TLSConfig:       m.dsn.TLS,
		HeartbeatPeriod: m.config.HeartbeatPeriod,
		ReadTimeout:     m.config.ReadTimeout,
		// gpool writes nothing to a log of its own and must not let a dependency
		// do it either: a library that prints to stderr is not something a host
		// program can turn off.
		Logger: slog.New(slog.DiscardHandler),
		// Decimals as strings rather than float64, which cannot represent them.
		UseDecimal: false,
		ParseTime:  true,
	}
}

// releaseStream is called by the stream once it has closed itself. It must run
// without m.mu held by the caller, which is why Close drops the lock first.
func (m *MySQL) releaseStream() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stream = nil
	m.streaming = false
}

// currentPosition reads where the binary log has reached.
//
// GTID is preferred where the source has it enabled: a file and offset name a
// place in one server's logs and mean nothing after a failover, whereas a GTID
// set identifies the transactions themselves.
func (m *MySQL) currentPosition(ctx context.Context) (position, error) {
	if set, ok := m.currentGTIDSet(ctx); ok {
		return position{gtid: set}, nil
	}

	file, err := m.currentFilePosition(ctx)
	if err != nil {
		return position{}, err
	}
	return position{file: file}, nil
}

// currentGTIDSet reports the executed GTID set, if the source uses GTIDs.
func (m *MySQL) currentGTIDSet(ctx context.Context) (mysql.GTIDSet, bool) {
	query := "SELECT @@GLOBAL.gtid_executed"
	if m.config.Flavor == mysql.MariaDBFlavor {
		query = "SELECT @@GLOBAL.gtid_binlog_pos"
	}

	var executed sql.NullString
	if err := m.db.QueryRowContext(ctx, query).Scan(&executed); err != nil {
		// The variable does not exist on a server without GTID support, which is
		// not an error — it just means positions are file and offset here.
		return nil, false
	}
	if !executed.Valid || executed.String == "" {
		return nil, false
	}

	set, err := mysql.ParseGTIDSet(m.config.Flavor, executed.String)
	if err != nil {
		return nil, false
	}
	return set, true
}

// currentFilePosition reads the current binlog file and offset.
func (m *MySQL) currentFilePosition(ctx context.Context) (mysql.Position, error) {
	// SHOW MASTER STATUS was renamed in MySQL 8.4 and removed in 9.0, so the new
	// name is tried first and the old one kept for everything before it.
	var lastErr error
	for _, query := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		found, err := m.scanStatus(ctx, query)
		if err == nil {
			return found, nil
		}
		lastErr = err
	}
	return mysql.Position{}, lastErr
}

func (m *MySQL) scanStatus(ctx context.Context, query string) (mysql.Position, error) {
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return mysql.Position{}, err
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return mysql.Position{}, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return mysql.Position{}, err
		}
		// An empty result means binary logging is off, which no amount of
		// retrying fixes and which CDC cannot work without.
		return mysql.Position{}, fmt.Errorf("%w: the source has no binary log; enable log_bin", ErrInvalidConfig)
	}

	// The column set differs between versions and flavours, so the two that
	// matter are found by name rather than by position.
	values := make([]sql.NullString, len(names))
	targets := make([]any, len(names))
	for i := range values {
		targets[i] = &values[i]
	}
	if err := rows.Scan(targets...); err != nil {
		return mysql.Position{}, err
	}

	var found mysql.Position
	for i, name := range names {
		switch strings.ToLower(name) {
		case "file":
			found.Name = values[i].String
		case "position":
			var offset uint32
			if _, err := fmt.Sscanf(values[i].String, "%d", &offset); err == nil {
				found.Pos = offset
			}
		}
	}
	if found.Name == "" {
		return mysql.Position{}, fmt.Errorf("%w: %q reported no binlog file", ErrInvalidConfig, query)
	}
	return found, nil
}

// AddTables adds tables to the captured set, taking effect on an open stream.
//
// Nothing is sent to the server. MySQL has no per-consumer subscription to
// widen, so the whole binary log was already crossing the network and this only
// changes what is decoded and delivered.
func (m *MySQL) AddTables(_ context.Context, tables ...string) error {
	if len(tables) == 0 {
		return ErrNoTables
	}
	if err := m.usable(); err != nil {
		return err
	}
	m.filter.add(tables)
	return nil
}

// RemoveTables drops tables from the captured set.
func (m *MySQL) RemoveTables(_ context.Context, tables ...string) error {
	if len(tables) == 0 {
		return ErrNoTables
	}
	if err := m.usable(); err != nil {
		return err
	}
	m.filter.remove(tables)
	return nil
}

// SyncTables replaces the captured set outright.
func (m *MySQL) SyncTables(_ context.Context, tables ...string) error {
	if err := m.usable(); err != nil {
		return err
	}
	m.filter.set(tables)
	return nil
}

// IsTracking reports whether a table is in the captured set.
func (m *MySQL) IsTracking(table string) bool {
	return m.filter.has(table)
}

// GetTables returns the captured set. An empty result means every table is
// captured, which is what an empty filter does.
func (m *MySQL) GetTables() []string {
	return m.filter.list()
}

// VerifyTable reports whether the table exists on the source.
//
// This is weaker than the PostgreSQL vendor's check, and unavoidably so: there
// is no publication to confirm membership of, so existence is the only fact
// available.
func (m *MySQL) VerifyTable(ctx context.Context, table string) (bool, error) {
	if err := m.usable(); err != nil {
		return false, err
	}

	schema, name := splitQualified(table, m.dsn.DBName)
	var found int
	err := m.db.QueryRowContext(ctx, verifyTableSQL, schema, name).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("gpool/mysql/cdc: verifying table %q: %w", table, err)
	}
	return true, nil
}

// Close stops any open stream and releases the control connection. It is idempotent.
func (m *MySQL) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	stream := m.stream
	m.stream = nil
	m.mu.Unlock()

	// Closing the stream runs releaseStream, which takes m.mu, so the lock is
	// dropped first.
	if stream != nil {
		_ = stream.Close()
	}
	return m.db.Close()
}

func (m *MySQL) usable() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrClosed
	}
	return nil
}

// splitQualified separates "schema.table", falling back to the DSN's database
// for a bare table name.
func splitQualified(table, fallback string) (schema, name string) {
	if before, after, found := strings.Cut(table, "."); found {
		return before, after
	}
	return fallback, table
}

// splitHostPort separates a "host:port" address, defaulting to MySQL's port.
func splitHostPort(addr string) (string, uint16) {
	host, port, found := strings.Cut(addr, ":")
	if !found {
		return addr, 3306
	}

	var parsed uint16
	if _, err := fmt.Sscanf(port, "%d", &parsed); err != nil || parsed == 0 {
		return host, 3306
	}
	return host, parsed
}
