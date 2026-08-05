// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// outputPlugin is the logical decoding plugin. pgoutput ships with PostgreSQL,
	// so it needs no server-side installation.
	outputPlugin = "pgoutput"

	// protoVersion is the pgoutput protocol version. Version 1 is understood by
	// every server from PostgreSQL 10 onwards.
	protoVersion = 1

	// connCloseTimeout bounds the graceful close of a management connection.
	connCloseTimeout = 5 * time.Second
)

// Postgres is a logical replication subscriber implementing cdc.Subscriber.
//
// It keeps two kinds of connection strictly apart. Catalog queries and publication
// DDL go over a regular control connection. Streaming runs on its own walsender
// connection owned by the stream.
//
// They cannot be shared: after START_REPLICATION the walsender connection is in
// CopyBoth mode, where an ordinary query is protocol-illegal, and pgconn.PgConn is
// not safe for concurrent use in the first place. Running table management against
// the streaming connection corrupts the replication protocol.
type Postgres struct {
	config Config

	mu        sync.Mutex
	ctrl      *pgconn.PgConn
	stream    *pgEventStream
	closed    bool
	streaming bool
}

var (
	_ cdc.Subscriber = (*Postgres)(nil)
	// Slot and publication administration is optional on Subscriber now, so the
	// proof that PostgreSQL still offers it has to be explicit — otherwise
	// dropping a method here would compile and only fail at the caller's type
	// assertion, at runtime.
	_ cdc.ReplicationManager = (*Postgres)(nil)
)

// New creates a PostgreSQL CDC subscriber. It validates the configuration but does
// not dial the database; connections are established on first use.
func New(config Config) (*Postgres, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	config = config.withDefaults()
	config.Tables = dedupe(config.Tables)
	return &Postgres{config: config}, nil
}

// Subscribe starts change data capture from the slot's confirmed position, which
// replays everything the slot retained while this consumer was away.
func (p *Postgres) Subscribe(ctx context.Context) (cdc.EventStream, error) {
	return p.SubscribeFrom(ctx, cdc.NoPosition)
}

// SubscribeFrom starts change data capture after a recorded position.
// Only one stream may be open at a time.
//
// Passing NoPosition defers to the slot, which is what a PostgreSQL consumer
// normally wants: the server has been holding WAL for exactly this purpose. An
// explicit position is for a consumer keeping its own bookkeeping, and it is
// honoured over Config.StartLSN.
func (p *Postgres) SubscribeFrom(ctx context.Context, after cdc.Position) (cdc.EventStream, error) {
	start := p.config.StartLSN
	if after != cdc.NoPosition {
		parsed, err := parsePosition(after)
		if err != nil {
			return nil, err
		}
		start = parsed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrClosed
	}
	if p.streaming {
		return nil, ErrAlreadySubscribed
	}

	if p.config.CreatePublication {
		if err := p.ensurePublication(ctx); err != nil {
			return nil, err
		}
	}
	if p.config.CreateSlot {
		if err := p.ensureSlot(ctx, p.config.SlotName); err != nil {
			return nil, err
		}
	}

	if after != cdc.NoPosition {
		if err := p.checkResumable(ctx, start, after); err != nil {
			return nil, err
		}
	}

	conn, err := p.dialReplication(ctx)
	if err != nil {
		return nil, err
	}
	if err := p.startReplication(ctx, conn, start); err != nil {
		closeConn(conn)
		return nil, err
	}

	stream := newEventStream(ctx, conn, p.config, p.releaseStream)
	p.stream = stream
	p.streaming = true
	return stream, nil
}

// checkResumable refuses a start position the slot has already moved past.
//
// The server would accept it and silently begin at confirmed_flush_lsn instead,
// so a consumer resuming from its own older bookkeeping would receive a stream
// missing everything between the two positions, with nothing to distinguish it
// from a complete one. The caller is told the two positions so it can decide:
// accept the gap by calling Subscribe, or treat it as the data loss it is.
func (p *Postgres) checkResumable(ctx context.Context, start uint64, after cdc.Position) error {
	confirmed, ok, err := p.slotConfirmed(ctx)
	if err != nil || !ok || start >= confirmed {
		return err
	}
	return fmt.Errorf("%w: asked to resume after %s, but slot %q has confirmed %s",
		ErrPositionBehindSlot, after, p.config.SlotName, position(confirmed))
}

// slotConfirmed reports the slot's confirmed position, and whether it has one.
func (p *Postgres) slotConfirmed(ctx context.Context) (uint64, bool, error) {
	conn, err := p.controlConn(ctx)
	if err != nil {
		return 0, false, err
	}

	result := conn.ExecParams(ctx, slotConfirmedSQL, [][]byte{[]byte(p.config.SlotName)}, nil, nil, nil)

	var text string
	if result.NextRow() {
		// Values are only valid until the next NextRow, so this copies rather
		// than aliasing the reader's buffer.
		if values := result.Values(); len(values) > 0 && values[0] != nil {
			text = string(values[0])
		}
	}
	if _, err := result.Close(); err != nil {
		return 0, false, fmt.Errorf("gpool/postgres/cdc: reading slot %q: %w", p.config.SlotName, err)
	}
	if text == "" {
		return 0, false, nil
	}

	lsn, err := parsePosition(cdc.Position(text))
	if err != nil {
		return 0, false, err
	}
	return lsn, true, nil
}

// releaseStream is called by the stream once it has closed itself. It must run
// without p.mu held by the caller, which is why Close drops the lock first.
func (p *Postgres) releaseStream() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stream = nil
	p.streaming = false
}

// startReplication issues START_REPLICATION for the configured slot.
func (p *Postgres) startReplication(ctx context.Context, conn *pgconn.PgConn, start uint64) error {
	// A zero start position tells the server to resume from the slot's
	// confirmed_flush_lsn. Starting from the server's current WAL head instead
	// would discard everything the slot retained while this consumer was away,
	// which is the whole point of holding a slot.
	startLSN := pglogrepl.LSN(start)

	options := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			fmt.Sprintf("proto_version '%d'", protoVersion),
			fmt.Sprintf("publication_names %s", quoteLiteral(p.config.PublicationName)),
		},
	}

	if err := pglogrepl.StartReplication(ctx, conn, p.config.SlotName, startLSN, options); err != nil {
		return fmt.Errorf("gpool/postgres/cdc: starting replication on slot %q: %w", p.config.SlotName, err)
	}
	return nil
}

// AddTables adds tables to the subscription. Tables already tracked are skipped,
// because ALTER PUBLICATION ADD TABLE fails on a table the publication already has.
func (p *Postgres) AddTables(ctx context.Context, tables ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	missing := difference(dedupe(tables), p.config.Tables)
	if len(missing) == 0 {
		return nil
	}

	conn, err := p.controlConn(ctx)
	if err != nil {
		return err
	}
	if err := exec(ctx, conn, addPublicationTablesSQL(p.config.PublicationName, missing)); err != nil {
		return fmt.Errorf("gpool/postgres/cdc: adding tables to publication %q: %w", p.config.PublicationName, err)
	}

	// The local view is updated only after the server accepted the change, so
	// IsTracking never reports a table the publication does not actually have.
	p.config.Tables = append(p.config.Tables, missing...)
	return nil
}

// RemoveTables removes tables from the subscription. Tables not tracked are skipped.
func (p *Postgres) RemoveTables(ctx context.Context, tables ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	present := intersection(dedupe(tables), p.config.Tables)
	if len(present) == 0 {
		return nil
	}

	conn, err := p.controlConn(ctx)
	if err != nil {
		return err
	}
	if err := exec(ctx, conn, dropPublicationTablesSQL(p.config.PublicationName, present)); err != nil {
		return fmt.Errorf("gpool/postgres/cdc: removing tables from publication %q: %w", p.config.PublicationName, err)
	}

	p.config.Tables = difference(p.config.Tables, present)
	return nil
}

// SyncTables reconciles the publication to exactly the given list.
func (p *Postgres) SyncTables(ctx context.Context, tables ...string) error {
	wanted := dedupe(tables)
	if len(wanted) == 0 {
		return ErrNoTables
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	conn, err := p.controlConn(ctx)
	if err != nil {
		return err
	}
	if err := exec(ctx, conn, setPublicationTablesSQL(p.config.PublicationName, wanted)); err != nil {
		return fmt.Errorf("gpool/postgres/cdc: syncing tables on publication %q: %w", p.config.PublicationName, err)
	}

	p.config.Tables = wanted
	return nil
}

// IsTracking reports whether the table is in the local tracking list. It does not
// query the server; use VerifyTable for that.
func (p *Postgres) IsTracking(table string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, t := range p.config.Tables {
		if t == table {
			return true
		}
	}
	return false
}

// GetTables returns a copy of the local tracking list.
func (p *Postgres) GetTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	tables := make([]string, len(p.config.Tables))
	copy(tables, p.config.Tables)
	return tables
}

// VerifyTable reports whether the publication in the database actually covers the table.
func (p *Postgres) VerifyTable(ctx context.Context, table string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return false, ErrClosed
	}

	conn, err := p.controlConn(ctx)
	if err != nil {
		return false, err
	}

	schema, name := splitQualifiedName(table)
	params := [][]byte{
		[]byte(p.config.PublicationName),
		[]byte(schema),
		[]byte(name),
	}

	result := conn.ExecParams(ctx, verifyTableSQL, params, nil, nil, nil)
	found := result.NextRow()
	if _, err := result.Close(); err != nil {
		return false, fmt.Errorf("gpool/postgres/cdc: verifying table %q: %w", table, err)
	}
	return found, nil
}

// CreateSlot creates a logical replication slot. Creating an existing slot is a no-op.
func (p *Postgres) CreateSlot(ctx context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}
	return p.ensureSlot(ctx, name)
}

// DropSlot drops a logical replication slot. Dropping a missing slot is a no-op.
//
// The retained WAL position goes with it: a subscriber that later recreates a slot
// of the same name starts from the new slot's position, not the old one's.
func (p *Postgres) DropSlot(ctx context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	conn, err := p.dialReplication(ctx)
	if err != nil {
		return err
	}
	defer closeConn(conn)

	err = pglogrepl.DropReplicationSlot(ctx, conn, name, pglogrepl.DropReplicationSlotOptions{})
	if err != nil && !isUndefinedObject(err) {
		return fmt.Errorf("gpool/postgres/cdc: dropping replication slot %q: %w", name, err)
	}
	return nil
}

// CreatePublication creates a publication for the given tables.
// Creating an existing publication is a no-op and does not reconcile its table list.
func (p *Postgres) CreatePublication(ctx context.Context, name string, tables ...string) error {
	wanted := dedupe(tables)
	if len(wanted) == 0 {
		return ErrNoTables
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	conn, err := p.controlConn(ctx)
	if err != nil {
		return err
	}

	err = exec(ctx, conn, createPublicationSQL(name, wanted))
	if err != nil && !isDuplicateObject(err) {
		return fmt.Errorf("gpool/postgres/cdc: creating publication %q: %w", name, err)
	}
	return nil
}

// DropPublication drops a publication. Dropping a missing publication is a no-op.
func (p *Postgres) DropPublication(ctx context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	conn, err := p.controlConn(ctx)
	if err != nil {
		return err
	}
	if err := exec(ctx, conn, dropPublicationSQL(name)); err != nil {
		return fmt.Errorf("gpool/postgres/cdc: dropping publication %q: %w", name, err)
	}
	return nil
}

// Close releases every resource the subscriber holds, including any open stream.
// It is idempotent.
func (p *Postgres) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	stream, ctrl := p.stream, p.ctrl
	p.stream, p.ctrl = nil, nil
	p.mu.Unlock()

	// Closed outside the lock: the stream's completion callback takes p.mu, so
	// closing it while holding the lock would deadlock.
	var errs []error
	if stream != nil {
		if err := stream.Close(); err != nil {
			errs = append(errs, fmt.Errorf("gpool/postgres/cdc: closing stream: %w", err))
		}
	}
	if ctrl != nil {
		ctx, cancel := context.WithTimeout(context.Background(), connCloseTimeout)
		if err := ctrl.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("gpool/postgres/cdc: closing control connection: %w", err))
		}
		cancel()
	}
	return errors.Join(errs...)
}

// ensurePublication creates the configured publication, or reconciles an existing
// one to the configured table list. The caller must hold p.mu.
func (p *Postgres) ensurePublication(ctx context.Context) error {
	if len(p.config.Tables) == 0 {
		return ErrNoTables
	}

	conn, err := p.controlConn(ctx)
	if err != nil {
		return err
	}

	err = exec(ctx, conn, createPublicationSQL(p.config.PublicationName, p.config.Tables))
	if err == nil {
		return nil
	}
	if !isDuplicateObject(err) {
		return fmt.Errorf("gpool/postgres/cdc: creating publication %q: %w", p.config.PublicationName, err)
	}

	if err := exec(ctx, conn, setPublicationTablesSQL(p.config.PublicationName, p.config.Tables)); err != nil {
		return fmt.Errorf("gpool/postgres/cdc: reconciling publication %q: %w", p.config.PublicationName, err)
	}
	return nil
}

// ensureSlot creates a replication slot if it does not already exist.
// The caller must hold p.mu.
func (p *Postgres) ensureSlot(ctx context.Context, name string) error {
	conn, err := p.dialReplication(ctx)
	if err != nil {
		return err
	}
	defer closeConn(conn)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, name, outputPlugin, pglogrepl.CreateReplicationSlotOptions{})
	if err != nil && !isDuplicateObject(err) {
		return fmt.Errorf("gpool/postgres/cdc: creating replication slot %q: %w", name, err)
	}
	return nil
}

// controlConn returns the regular connection used for catalog queries and
// publication DDL, dialing or redialing it as needed. The caller must hold p.mu,
// which is what keeps it single-threaded.
func (p *Postgres) controlConn(ctx context.Context) (*pgconn.PgConn, error) {
	if p.ctrl != nil && !p.ctrl.IsClosed() {
		return p.ctrl, nil
	}
	if p.ctrl != nil {
		closeConn(p.ctrl)
		p.ctrl = nil
	}

	conn, err := pgconn.Connect(ctx, p.config.ConnString)
	if err != nil {
		return nil, fmt.Errorf("gpool/postgres/cdc: connecting control connection: %w", err)
	}
	p.ctrl = conn
	return conn, nil
}

// dialReplication opens a walsender connection. Slot commands and streaming both
// require replication=database, which an ordinary connection cannot provide.
func (p *Postgres) dialReplication(ctx context.Context) (*pgconn.PgConn, error) {
	connConfig, err := pgconn.ParseConfig(p.config.ConnString)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing ConnString: %w", ErrInvalidConfig, err)
	}
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string, 1)
	}
	connConfig.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("gpool/postgres/cdc: connecting replication connection: %w", err)
	}
	return conn, nil
}

// closeConn closes a management connection with a bounded, cancellation-immune
// context so shutdown still gets a chance to terminate gracefully.
func closeConn(conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), connCloseTimeout)
	defer cancel()
	_ = conn.Close(ctx)
}
