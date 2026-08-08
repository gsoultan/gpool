// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	mysqlpool "github.com/gsoultan/gpool/vendors/mysql"
	mysqlcdc "github.com/gsoultan/gpool/vendors/mysql/cdc"
)

// These tests need a MySQL or MariaDB with binary logging in ROW format:
//
//	podman run -d --rm --name mysql -p 3306:3306 \
//	  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=gpool mysql:8.4 \
//	  --log-bin=mysql-bin --binlog-format=ROW --server-id=1 \
//	  --gtid-mode=ON --enforce-gtid-consistency=ON
//
//	MYSQL_DSN='root:root@tcp(127.0.0.1:53306)/gpool' \
//	MARIADB_DSN='root:root@tcp(127.0.0.1:53307)/gpool' go test ./...
//
// MariaDB is not MySQL with a different name here. Its GTIDs use an entirely
// different syntax, it exposes the executed set as gtid_binlog_pos rather than
// gtid_executed, and it writes its own binlog event type — which is why Config
// has a Flavor at all. A suite that only ran against MySQL would leave every one
// of those paths unexecuted.
type target struct {
	name   string
	vendor gpool.Vendor
	flavor string
	dsn    string
}

func targets(t *testing.T) []target {
	t.Helper()

	candidates := []target{
		{"mysql", mysqlpool.MySQL, "mysql", os.Getenv("MYSQL_DSN")},
		{"mariadb", mysqlpool.MariaDB, "mariadb", os.Getenv("MARIADB_DSN")},
	}

	var configured []target
	for _, candidate := range candidates {
		if candidate.dsn != "" {
			configured = append(configured, candidate)
		}
	}
	if len(configured) == 0 {
		t.Skip("neither MYSQL_DSN nor MARIADB_DSN is set")
	}
	return configured
}

// eachTarget runs body against every configured server, as its own subtest so a
// MariaDB-only failure is named rather than hidden behind a passing MySQL run.
func eachTarget(t *testing.T, body func(*testing.T, target)) {
	t.Helper()

	for _, server := range targets(t) {
		t.Run(server.name, func(t *testing.T) { body(t, server) })
	}
}

type fixture struct {
	db     *sql.DB
	server target
	table  string
	other  string
}

func newFixture(t *testing.T, server target) *fixture {
	t.Helper()

	f := &fixture{server: server}

	db, err := sql.Open("mysql", f.server.dsn)
	if err != nil {
		t.Fatalf("sql.Open() = %v", err)
	}
	if err := db.PingContext(t.Context()); err != nil {
		t.Skipf("MySQL is not reachable: %v", err)
	}
	f.db = db
	t.Cleanup(func() { _ = db.Close() })

	var enabled string
	if err := db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.binlog_format").Scan(&enabled); err != nil {
		t.Skipf("cannot read binlog_format, CDC needs binary logging: %v", err)
	}
	if !strings.EqualFold(enabled, "ROW") {
		t.Skipf("binlog_format is %q, CDC needs ROW", enabled)
	}

	suffix := time.Now().UnixNano() % 1_000_000_000
	f.table = fmt.Sprintf("gpool_cdc_%d", suffix)
	f.other = fmt.Sprintf("gpool_other_%d", suffix)

	for _, name := range []string{f.table, f.other} {
		create := fmt.Sprintf(
			"CREATE TABLE %s (id BIGINT PRIMARY KEY AUTO_INCREMENT, email VARCHAR(255), score INT)", name)
		if _, err := db.ExecContext(t.Context(), create); err != nil {
			t.Fatalf("creating %s = %v", name, err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, name := range []string{f.table, f.other} {
			_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
		}
	})
	return f
}

// serverID has to differ per subscriber. Two consumers sharing one are treated
// by the source as the same replica reconnecting, and it disconnects the first.
func (f *fixture) config(t *testing.T, tables ...string) mysqlcdc.Config {
	t.Helper()

	return mysqlcdc.Config{
		DSN:      f.server.dsn,
		Flavor:   f.server.flavor,
		ServerID: uint32(time.Now().UnixNano()%900_000) + 100_000,
		Tables:   tables,
	}
}

func (f *fixture) subscribe(t *testing.T, tables ...string) cdc.Subscriber {
	t.Helper()

	subscriber, err := gpool.NewSubscriber(f.server.vendor, f.config(t, tables...))
	if err != nil {
		t.Fatalf("NewSubscriber() = %v", err)
	}
	t.Cleanup(func() { _ = subscriber.Close() })
	return subscriber
}

func (f *fixture) exec(t *testing.T, query string, args ...any) {
	t.Helper()

	if _, err := f.db.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("%s = %v", query, err)
	}
}

// collect drains up to want events, or gives up at the deadline.
func collect(t *testing.T, stream cdc.EventStream, want int, timeout time.Duration) []cdc.Event {
	t.Helper()

	events := make([]cdc.Event, 0, want)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for event := range stream.All() {
			events = append(events, event)
			if len(events) == want {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		_ = stream.Close()
		<-done
	}
	return events
}

func TestMySQLCDCStreamsChanges(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)
		subscriber := f.subscribe(t, "gpool."+f.table)

		stream, err := subscriber.Subscribe(t.Context())
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}

		collected := make(chan []cdc.Event, 1)
		go func() { collected <- collect(t, stream, 3, 30*time.Second) }()

		f.exec(t, fmt.Sprintf("INSERT INTO %s (email, score) VALUES (?, ?)", f.table), "a@example.com", 1)
		f.exec(t, fmt.Sprintf("UPDATE %s SET email = ?, score = ?", f.table), "b@example.com", 2)
		f.exec(t, fmt.Sprintf("DELETE FROM %s", f.table))

		events := <-collected
		if len(events) != 3 {
			t.Fatalf("got %d events, want 3: %+v", len(events), events)
		}

		wantOps := []cdc.Op{cdc.OpInsert, cdc.OpUpdate, cdc.OpDelete}
		for i, event := range events {
			if event.Op != wantOps[i] {
				t.Errorf("event %d op = %v, want %v", i, event.Op, wantOps[i])
			}
			if event.Table != f.table {
				t.Errorf("event %d table = %q, want %q", i, event.Table, f.table)
			}
			if event.Position == cdc.NoPosition {
				t.Errorf("event %d has no position", i)
			}
			// The binlog event header timestamps every event; a zero value means
			// the header was read but the field never reached the consumer.
			if event.Timestamp.IsZero() {
				t.Errorf("event %d has no commit timestamp", i)
			}
			if age := time.Since(event.Timestamp); age > time.Hour || age < -time.Hour {
				t.Errorf("event %d commit timestamp is %s, %s from now", i, event.Timestamp, age)
			}
		}

		// Column names have to be resolved even under binlog_row_metadata=MINIMAL,
		// which is the default and carries no names in the log at all.
		if got := events[0].After["email"]; got != "a@example.com" {
			t.Errorf("insert After[email] = %v, want a@example.com", got)
		}
		if got := events[1].After["email"]; got != "b@example.com" {
			t.Errorf("update After[email] = %v", got)
		}
		// ROW format carries the full before image by default.
		if got := events[1].Before["email"]; got != "a@example.com" {
			t.Errorf("update Before[email] = %v, want a@example.com", got)
		}
		if got := events[2].Before["email"]; got != "b@example.com" {
			t.Errorf("delete Before[email] = %v", got)
		}
	})
}

// MySQL records nothing on a consumer's behalf, so a position the consumer kept
// is the only thing that makes a restart lossless. This is the capability the
// whole interface change existed to make expressible.
func TestMySQLCDCResumesFromARecordedPosition(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)
		subscriber := f.subscribe(t, "gpool."+f.table)

		stream, err := subscriber.Subscribe(t.Context())
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}

		collected := make(chan []cdc.Event, 1)
		go func() { collected <- collect(t, stream, 2, 30*time.Second) }()

		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "r1")
		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "r2")

		first := <-collected
		if len(first) != 2 {
			t.Fatalf("got %d events, want 2", len(first))
		}
		checkpoint := first[1].Position
		if err := stream.Close(); err != nil {
			t.Fatalf("Close() = %v", err)
		}

		// Written while nothing is streaming. Only a recorded position finds them.
		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "r3")
		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "r4")

		resumed, err := subscriber.SubscribeFrom(t.Context(), checkpoint)
		if err != nil {
			t.Fatalf("SubscribeFrom(%q) = %v", checkpoint, err)
		}
		defer resumed.Close()

		// A position marks the start of the transaction that produced the change, so
		// resuming replays that change: at-least-once, exactly as Event.Position
		// specifies. What must not happen is a gap.
		var got []string
		for _, event := range collect(t, resumed, 3, 30*time.Second) {
			got = append(got, fmt.Sprint(event.After["email"]))
		}
		t.Logf("resuming after %q delivered %v", checkpoint, got)

		if !slices.Contains(got, "r3") || !slices.Contains(got, "r4") {
			t.Errorf("resuming after %q delivered %v, want it to reach r3 and r4", checkpoint, got)
		}
		if slices.Contains(got, "r1") {
			t.Errorf("resuming after %q replayed r1, so it did not resume forward: %v", checkpoint, got)
		}
	})
}

// Subscribe starts at the end of the log, so changes made while nothing was
// streaming are gone. That is the opposite of PostgreSQL and it is the single
// most important thing for a consumer to know about MySQL CDC.
func TestMySQLCDCSubscribeStartsAtTheEndOfTheLog(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)
		subscriber := f.subscribe(t, "gpool."+f.table)

		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "written-before")

		stream, err := subscriber.Subscribe(t.Context())
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}
		defer stream.Close()

		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "written-after")

		events := collect(t, stream, 1, 30*time.Second)
		if len(events) != 1 {
			t.Fatalf("got %d events, want 1", len(events))
		}
		if got := events[0].After["email"]; got != "written-after" {
			t.Errorf("Subscribe() delivered %v; it must start at the end of the log, not replay history", got)
		}
	})
}

// The filter is applied by the consumer, since MySQL has no subscription to
// narrow, so it still has to actually narrow.
func TestMySQLCDCFiltersToTrackedTables(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)
		subscriber := f.subscribe(t, "gpool."+f.table)

		stream, err := subscriber.Subscribe(t.Context())
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}
		defer stream.Close()

		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.other), "ignored")
		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "kept")

		events := collect(t, stream, 1, 30*time.Second)
		if len(events) != 1 {
			t.Fatalf("got %d events, want 1", len(events))
		}
		if events[0].Table != f.table {
			t.Errorf("delivered a change to %q, which is not tracked", events[0].Table)
		}
		if got := events[0].After["email"]; got != "kept" {
			t.Errorf("After[email] = %v, want kept", got)
		}
	})
}

// Adding a table has to reach a stream that is already running, or TableManager
// is decorative.
func TestMySQLCDCAddTablesAffectsARunningStream(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)
		subscriber := f.subscribe(t, "gpool."+f.table)

		stream, err := subscriber.Subscribe(t.Context())
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}
		defer stream.Close()

		if err := subscriber.AddTables(t.Context(), "gpool."+f.other); err != nil {
			t.Fatalf("AddTables() = %v", err)
		}
		if !subscriber.IsTracking("gpool." + f.other) {
			t.Error("IsTracking() = false after AddTables")
		}

		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.other), "now-tracked")

		events := collect(t, stream, 1, 30*time.Second)
		if len(events) != 1 || events[0].Table != f.other {
			t.Fatalf("got %+v, want one change to %s", events, f.other)
		}
	})
}

// The point of demoting ReplicationManager off Subscriber: a vendor with no
// slots or publications must not appear to have them. If this assertion ever
// succeeds, someone has added four methods that can only fail.
func TestMySQLCDCOffersNoReplicationManager(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)
		subscriber := f.subscribe(t)

		if _, ok := subscriber.(cdc.ReplicationManager); ok {
			t.Error("the MySQL subscriber claims ReplicationManager, but MySQL has no slots or publications")
		}
	})
}

func TestMySQLCDCRejectsAForeignPosition(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)
		subscriber := f.subscribe(t)

		// A PostgreSQL LSN.
		if _, err := subscriber.SubscribeFrom(t.Context(), "0/1A2B3C4D"); err == nil {
			t.Fatal("SubscribeFrom() accepted a PostgreSQL LSN")
		}
	})
}

// A source with GTIDs on should produce GTID positions, not file offsets: a
// file and offset name a place in one server's logs and survive no failover.
func TestMySQLCDCPrefersGTIDPositions(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)

		// MariaDB has no gtid_mode: GTIDs are always on, and the executed set lives
		// in a differently named variable.
		if f.server.flavor == "mysql" {
			var mode string
			if err := f.db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil {
				t.Skipf("cannot read gtid_mode: %v", err)
			}
			if !strings.EqualFold(mode, "ON") {
				t.Skipf("gtid_mode is %q", mode)
			}
		}

		subscriber := f.subscribe(t, "gpool."+f.table)
		stream, err := subscriber.Subscribe(t.Context())
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}
		defer stream.Close()

		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "gtid")

		events := collect(t, stream, 1, 30*time.Second)
		if len(events) != 1 {
			t.Fatalf("got %d events, want 1", len(events))
		}
		if !strings.HasPrefix(string(events[0].Position), "gtid:") {
			t.Errorf("Position = %q, want a gtid: position on a GTID-enabled source", events[0].Position)
		}
	})
}

// Registration is what makes the factory resolve, and it has to cover both names
// the pool vendor registers or a MariaDB caller gets a pool but no subscriber.
func TestMySQLCDCRegistersUnderBothVendorNames(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)

		for _, vendor := range []gpool.Vendor{mysqlpool.MySQL, mysqlpool.MariaDB} {
			subscriber, err := gpool.NewSubscriber(vendor, f.config(t))
			if err != nil {
				t.Fatalf("NewSubscriber(%q) = %v", vendor, err)
			}
			_ = subscriber.Close()
		}
	})
}

// Changes committed together must be reported as one transaction, so a consumer
// can apply them atomically.
func TestMySQLCDCGroupsChangesByTransaction(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)
		subscriber := f.subscribe(t, "gpool."+f.table)

		stream, err := subscriber.Subscribe(t.Context())
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}
		defer stream.Close()

		collected := make(chan []cdc.Event, 1)
		go func() { collected <- collect(t, stream, 4, 30*time.Second) }()

		tx, err := f.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("BeginTx() = %v", err)
		}
		for _, email := range []string{"t1", "t2", "t3"} {
			if _, err := tx.ExecContext(t.Context(),
				fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), email); err != nil {
				t.Fatalf("INSERT %s = %v", email, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() = %v", err)
		}
		f.exec(t, fmt.Sprintf("INSERT INTO %s (email) VALUES (?)", f.table), "alone")

		events := <-collected
		if len(events) != 4 {
			t.Fatalf("got %d events, want 4", len(events))
		}

		first := events[0].Transaction
		if first == cdc.NoPosition {
			t.Fatal("events carry no transaction identity")
		}
		for i, event := range events[:3] {
			if event.Transaction != first {
				t.Errorf("event %d is in transaction %q, want %q", i, event.Transaction, first)
			}
		}
		if events[3].Transaction == first {
			t.Error("the fourth change reports the batch's transaction, but was committed separately")
		}
	})
}

// Under binlog_row_metadata=MINIMAL — the default — a binlog row carries values
// with no names, so the names come from information_schema, which describes the
// table as it is now rather than as it was when the row was written. A stream
// resuming across an ALTER TABLE therefore meets rows whose column count no
// longer matches, and that is genuinely ambiguous: reporting it is the only
// honest option, because guessing would hand the consumer values under names
// that may not be theirs.
func TestMySQLCDCReportsASchemaMismatch(t *testing.T) {
	eachTarget(t, func(t *testing.T, server target) {
		f := newFixture(t, server)

		var metadata string
		if err := f.db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.binlog_row_metadata").Scan(&metadata); err != nil {
			t.Skipf("cannot read binlog_row_metadata: %v", err)
		}
		// Only FULL puts the names in the log. MINIMAL and MariaDB's NO_LOG both
		// leave the catalog as the only source, which is the case this exercises.
		if strings.EqualFold(metadata, "FULL") {
			t.Skipf("binlog_row_metadata is FULL; the names travel with the row and cannot disagree")
		}

		subscriber := f.subscribe(t, "gpool."+f.table)
		stream, err := subscriber.Subscribe(t.Context())
		if err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}

		// One row written under the three-column schema.
		f.exec(t, fmt.Sprintf("INSERT INTO %s (id, email, score) VALUES (1, 'before', 10)", f.table))
		first := collect(t, stream, 1, 30*time.Second)
		if len(first) != 1 {
			t.Fatalf("got %d events, want 1", len(first))
		}
		checkpoint := first[0].Position
		_ = stream.Close()

		// Now the table loses a column, so the catalog no longer describes the
		// row that is about to be replayed.
		f.exec(t, fmt.Sprintf("ALTER TABLE %s DROP COLUMN score", f.table))

		// A *fresh* subscriber, deliberately. The one that already streamed the row
		// has the old column names cached, and those names are still the right ones
		// for that row — so it decodes it correctly and reports nothing. The
		// mismatch belongs to a consumer that starts cold and has only the catalog
		// to go on, which is what a restarted process is.
		fresh, err := gpool.NewSubscriber(server.vendor, f.config(t, "gpool."+f.table))
		if err != nil {
			t.Fatalf("NewSubscriber() = %v", err)
		}
		t.Cleanup(func() { _ = fresh.Close() })

		resumed, err := fresh.SubscribeFrom(t.Context(), checkpoint)
		if err != nil {
			t.Fatalf("SubscribeFrom(%q) = %v", checkpoint, err)
		}
		defer resumed.Close()

		// Draining is how a consumer finds out; Err is how it learns why. Bounded,
		// because a stream that neither delivers nor ends is itself the failure and
		// must not present as a hung test.
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			for range resumed.All() {
			}
		}()
		select {
		case <-drained:
		case <-time.After(30 * time.Second):
			t.Fatal("the stream neither ended nor reported a mismatch")
		}

		if err := resumed.Err(); !errors.Is(err, mysqlcdc.ErrSchemaMismatch) {
			t.Fatalf("stream ended with %v, want ErrSchemaMismatch", err)
		}
		t.Logf("reported: %v", resumed.Err())
	})
}
