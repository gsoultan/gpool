// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc_test

import (
	"context"
	"database/sql"
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
//	MYSQL_DSN='root:root@tcp(127.0.0.1:3306)/gpool' go test ./...
func dsn(t *testing.T) string {
	t.Helper()

	value := os.Getenv("MYSQL_DSN")
	if value == "" {
		t.Skip("MYSQL_DSN not set")
	}
	return value
}

type fixture struct {
	db    *sql.DB
	dsn   string
	table string
	other string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	f := &fixture{dsn: dsn(t)}

	db, err := sql.Open("mysql", f.dsn)
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
		DSN:      f.dsn,
		ServerID: uint32(time.Now().UnixNano()%900_000) + 100_000,
		Tables:   tables,
	}
}

func (f *fixture) subscribe(t *testing.T, tables ...string) cdc.Subscriber {
	t.Helper()

	subscriber, err := gpool.NewSubscriber(mysqlpool.MySQL, f.config(t, tables...))
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
	f := newFixture(t)
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
}

// MySQL records nothing on a consumer's behalf, so a position the consumer kept
// is the only thing that makes a restart lossless. This is the capability the
// whole interface change existed to make expressible.
func TestMySQLCDCResumesFromARecordedPosition(t *testing.T) {
	f := newFixture(t)
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
}

// Subscribe starts at the end of the log, so changes made while nothing was
// streaming are gone. That is the opposite of PostgreSQL and it is the single
// most important thing for a consumer to know about MySQL CDC.
func TestMySQLCDCSubscribeStartsAtTheEndOfTheLog(t *testing.T) {
	f := newFixture(t)
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
}

// The filter is applied by the consumer, since MySQL has no subscription to
// narrow, so it still has to actually narrow.
func TestMySQLCDCFiltersToTrackedTables(t *testing.T) {
	f := newFixture(t)
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
}

// Adding a table has to reach a stream that is already running, or TableManager
// is decorative.
func TestMySQLCDCAddTablesAffectsARunningStream(t *testing.T) {
	f := newFixture(t)
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
}

// The point of demoting ReplicationManager off Subscriber: a vendor with no
// slots or publications must not appear to have them. If this assertion ever
// succeeds, someone has added four methods that can only fail.
func TestMySQLCDCOffersNoReplicationManager(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t)

	if _, ok := subscriber.(cdc.ReplicationManager); ok {
		t.Error("the MySQL subscriber claims ReplicationManager, but MySQL has no slots or publications")
	}
}

func TestMySQLCDCRejectsAForeignPosition(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t)

	// A PostgreSQL LSN.
	if _, err := subscriber.SubscribeFrom(t.Context(), "0/1A2B3C4D"); err == nil {
		t.Fatal("SubscribeFrom() accepted a PostgreSQL LSN")
	}
}

// A source with GTIDs on should produce GTID positions, not file offsets: a
// file and offset name a place in one server's logs and survive no failover.
func TestMySQLCDCPrefersGTIDPositions(t *testing.T) {
	f := newFixture(t)

	var mode string
	if err := f.db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil {
		t.Skipf("cannot read gtid_mode: %v", err)
	}
	if !strings.EqualFold(mode, "ON") {
		t.Skipf("gtid_mode is %q", mode)
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
}

// Registration is what makes the factory resolve, and it has to cover both names
// the pool vendor registers or a MariaDB caller gets a pool but no subscriber.
func TestMySQLCDCRegistersUnderBothVendorNames(t *testing.T) {
	f := newFixture(t)

	for _, vendor := range []gpool.Vendor{mysqlpool.MySQL, mysqlpool.MariaDB} {
		subscriber, err := gpool.NewSubscriber(vendor, f.config(t))
		if err != nil {
			t.Fatalf("NewSubscriber(%q) = %v", vendor, err)
		}
		_ = subscriber.Close()
	}
}
