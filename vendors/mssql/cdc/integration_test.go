// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	mssqlpool "github.com/gsoultan/gpool/vendors/mssql"
	mssqlcdc "github.com/gsoultan/gpool/vendors/mssql/cdc"
)

// These tests need SQL Server with change data capture available, which means a
// database of its own — sp_cdc_enable_db refuses to run on master — and a
// running SQL Server Agent, because the capture job is what fills the change
// tables. `.junie/scripts/testdbs.sh up mssql` provides both.
//
//	MSSQL_CDC_DSN='sqlserver://sa:pass@127.0.0.1:51433?database=gpoolcdc' go test ./...
//
// They are slower than the other vendors' CDC tests and cannot be made faster
// here: SQL Server delivers on the capture job's schedule, about five seconds,
// rather than as transactions commit.
const captureWait = 90 * time.Second

func dsn(t *testing.T) string {
	t.Helper()

	value := os.Getenv("MSSQL_CDC_DSN")
	if value == "" {
		t.Skip("MSSQL_CDC_DSN not set")
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

	db, err := sql.Open("sqlserver", f.dsn)
	if err != nil {
		t.Fatalf("sql.Open() = %v", err)
	}
	if err := db.PingContext(t.Context()); err != nil {
		t.Skipf("SQL Server is not reachable: %v", err)
	}
	f.db = db
	t.Cleanup(func() { _ = db.Close() })

	var agentRunning int
	err = db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM sys.dm_server_services WHERE servicename LIKE '%Agent%' AND status_desc = 'Running'`).
		Scan(&agentRunning)
	if err != nil || agentRunning == 0 {
		t.Skip("SQL Server Agent is not running; the capture job would never fill the change tables")
	}

	suffix := time.Now().UnixNano() % 1_000_000_000
	f.table = fmt.Sprintf("gpool_cdc_%d", suffix)
	f.other = fmt.Sprintf("gpool_other_%d", suffix)

	for _, name := range []string{f.table, f.other} {
		create := fmt.Sprintf("CREATE TABLE dbo.%s (id INT PRIMARY KEY, email NVARCHAR(255), score INT)", name)
		if _, err := db.ExecContext(t.Context(), create); err != nil {
			t.Fatalf("creating %s = %v", name, err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for _, name := range []string{f.table, f.other} {
			// Capture must go first: a change table outlives its source and keeps
			// the capture instance around, which leaks across runs.
			_, _ = db.ExecContext(ctx, fmt.Sprintf(`IF EXISTS (
	SELECT 1 FROM cdc.change_tables ct JOIN sys.tables t ON ct.source_object_id = t.object_id
	WHERE t.name = '%s')
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = '%s', @capture_instance = N'all'`, name, name))
			_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS dbo.%s", name))
		}
	})
	return f
}

func (f *fixture) config(tables ...string) mssqlcdc.Config {
	return mssqlcdc.Config{
		DSN:          f.dsn,
		Tables:       tables,
		PollInterval: 500 * time.Millisecond,
	}
}

func (f *fixture) subscribe(t *testing.T, tables ...string) cdc.Subscriber {
	t.Helper()

	subscriber, err := gpool.NewSubscriber(mssqlpool.SQLServer, f.config(tables...))
	if err != nil {
		t.Fatalf("NewSubscriber() = %v", err)
	}
	t.Cleanup(func() { _ = subscriber.Close() })

	// Capture is server-side DDL here, not a client filter, so it has to be
	// switched on before anything is streamed.
	if len(tables) > 0 {
		if err := subscriber.AddTables(t.Context(), tables...); err != nil {
			t.Fatalf("AddTables() = %v", err)
		}
	}
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
	// A stream that ended early did so for a reason, and reporting "got 0 events"
	// without it sends the reader looking in the wrong place.
	if len(events) < want {
		if err := stream.Err(); err != nil {
			t.Errorf("stream ended after %d of %d events: %v", len(events), want, err)
		}
	}
	return events
}

func TestSQLServerCDCStreamsChanges(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t, "dbo."+f.table)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer stream.Close()

	collected := make(chan []cdc.Event, 1)
	go func() { collected <- collect(t, stream, 3, captureWait) }()

	f.exec(t, fmt.Sprintf("INSERT INTO dbo.%s (id, email, score) VALUES (1, 'a@example.com', 1)", f.table))
	f.exec(t, fmt.Sprintf("UPDATE dbo.%s SET email = 'b@example.com', score = 2 WHERE id = 1", f.table))
	f.exec(t, fmt.Sprintf("DELETE FROM dbo.%s WHERE id = 1", f.table))

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
		if event.Timestamp.IsZero() {
			t.Errorf("event %d has no commit timestamp", i)
		}
	}

	if got := events[0].After["email"]; got != "a@example.com" {
		t.Errorf("insert After[email] = %v, want a@example.com", got)
	}
	if got := events[1].After["email"]; got != "b@example.com" {
		t.Errorf("update After[email] = %v", got)
	}
	// "all update old" is what makes the before image available; without it an
	// update arrives as an after image alone.
	if got := events[1].Before["email"]; got != "a@example.com" {
		t.Errorf("update Before[email] = %v, want a@example.com", got)
	}
	if got := events[2].Before["email"]; got != "b@example.com" {
		t.Errorf("delete Before[email] = %v", got)
	}
}

// SQL Server keeps no per-consumer position, so a position the consumer recorded
// is the only thing that makes a restart lossless — the same contract MySQL has,
// reached through a completely different mechanism.
func TestSQLServerCDCResumesFromARecordedPosition(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t, "dbo."+f.table)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	collected := make(chan []cdc.Event, 1)
	go func() { collected <- collect(t, stream, 2, captureWait) }()

	f.exec(t, fmt.Sprintf("INSERT INTO dbo.%s (id, email) VALUES (1, 'r1')", f.table))
	f.exec(t, fmt.Sprintf("INSERT INTO dbo.%s (id, email) VALUES (2, 'r2')", f.table))

	first := <-collected
	if len(first) != 2 {
		t.Fatalf("got %d events, want 2", len(first))
	}
	checkpoint := first[1].Position
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	// Written while nothing is streaming. Only a recorded position finds them.
	f.exec(t, fmt.Sprintf("INSERT INTO dbo.%s (id, email) VALUES (3, 'r3')", f.table))
	f.exec(t, fmt.Sprintf("INSERT INTO dbo.%s (id, email) VALUES (4, 'r4')", f.table))

	resumed, err := subscriber.SubscribeFrom(t.Context(), checkpoint)
	if err != nil {
		t.Fatalf("SubscribeFrom(%q) = %v", checkpoint, err)
	}
	defer resumed.Close()

	// The window is inclusive of the position, so the change it came from is
	// replayed: at-least-once, exactly as Event.Position specifies. What must not
	// happen is a gap.
	var got []string
	for _, event := range collect(t, resumed, 3, captureWait) {
		got = append(got, fmt.Sprint(event.After["email"]))
	}
	t.Logf("resuming after %q delivered %v", checkpoint, got)

	for _, want := range []string{"r3", "r4"} {
		if !contains(got, want) {
			t.Errorf("resuming after %q delivered %v, want it to reach %s", checkpoint, got, want)
		}
	}
	if contains(got, "r1") {
		t.Errorf("resuming after %q replayed r1, so it did not resume forward: %v", checkpoint, got)
	}
}

// Capture is per-table server-side state, so a table that was never enabled must
// not appear in the stream.
func TestSQLServerCDCStreamsOnlyCapturedTables(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t, "dbo."+f.table)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer stream.Close()

	f.exec(t, fmt.Sprintf("INSERT INTO dbo.%s (id, email) VALUES (1, 'ignored')", f.other))
	f.exec(t, fmt.Sprintf("INSERT INTO dbo.%s (id, email) VALUES (1, 'kept')", f.table))

	events := collect(t, stream, 1, captureWait)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Table != f.table {
		t.Errorf("delivered a change to %q, which is not captured", events[0].Table)
	}
	if got := events[0].After["email"]; got != "kept" {
		t.Errorf("After[email] = %v, want kept", got)
	}
}

// AddTables is DDL against the server here rather than a client-side filter, so
// VerifyTable can report what the server is really doing.
func TestSQLServerCDCAddTablesEnablesCaptureOnTheServer(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t)

	captured, err := subscriber.VerifyTable(t.Context(), "dbo."+f.other)
	if err != nil {
		t.Fatalf("VerifyTable() = %v", err)
	}
	if captured {
		t.Fatalf("%s is captured before AddTables", f.other)
	}

	if err := subscriber.AddTables(t.Context(), "dbo."+f.other); err != nil {
		t.Fatalf("AddTables() = %v", err)
	}
	if captured, err = subscriber.VerifyTable(t.Context(), "dbo."+f.other); err != nil || !captured {
		t.Fatalf("VerifyTable() = %v, %v after AddTables", captured, err)
	}
	if !subscriber.IsTracking("dbo." + f.other) {
		t.Error("IsTracking() = false after AddTables")
	}

	if err := subscriber.RemoveTables(t.Context(), "dbo."+f.other); err != nil {
		t.Fatalf("RemoveTables() = %v", err)
	}
	if captured, err = subscriber.VerifyTable(t.Context(), "dbo."+f.other); err != nil || captured {
		t.Fatalf("VerifyTable() = %v, %v after RemoveTables", captured, err)
	}
}

// The point of demoting ReplicationManager off Subscriber: a vendor with no
// slots or publications must not appear to have them.
func TestSQLServerCDCOffersNoReplicationManager(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t)

	if _, ok := subscriber.(cdc.ReplicationManager); ok {
		t.Error("the SQL Server subscriber claims ReplicationManager, but it has no slots or publications")
	}
}

func TestSQLServerCDCRejectsAForeignPosition(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t, "dbo."+f.table)

	for _, foreign := range []cdc.Position{
		"0/1A2B3C4D", // PostgreSQL
		"gtid:3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", // MySQL
		"file:mysql-bin.000042:1234",                    // MySQL
	} {
		if _, err := subscriber.SubscribeFrom(t.Context(), foreign); err == nil {
			t.Errorf("SubscribeFrom(%q) accepted a position from another vendor", foreign)
		}
	}
}

// A database without CDC is otherwise indistinguishable from one where nothing
// has changed, which is the failure someone would spend an afternoon on.
func TestSQLServerCDCReportsADatabaseWithoutCapture(t *testing.T) {
	plain := os.Getenv("MSSQL_DSN")
	if plain == "" {
		t.Skip("MSSQL_DSN not set")
	}

	subscriber, err := gpool.NewSubscriber(mssqlpool.SQLServer, mssqlcdc.Config{DSN: plain})
	if err != nil {
		t.Fatalf("NewSubscriber() = %v", err)
	}
	defer subscriber.Close()

	_, err = subscriber.Subscribe(t.Context())
	if err == nil {
		t.Fatal("Subscribe() succeeded against master, which cannot have CDC")
	}
	if !strings.Contains(err.Error(), "change data capture") {
		t.Errorf("error should name the missing capability, got: %v", err)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// Changes committed together share a __$start_lsn, which is what makes
// Event.Transaction meaningful here.
func TestSQLServerCDCGroupsChangesByTransaction(t *testing.T) {
	f := newFixture(t)
	subscriber := f.subscribe(t, "dbo."+f.table)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer stream.Close()

	collected := make(chan []cdc.Event, 1)
	go func() { collected <- collect(t, stream, 4, captureWait) }()

	f.exec(t, fmt.Sprintf(`BEGIN TRANSACTION;
INSERT INTO dbo.%s (id, email) VALUES (1, 't1'), (2, 't2'), (3, 't3');
COMMIT TRANSACTION;`, f.table))
	f.exec(t, fmt.Sprintf("INSERT INTO dbo.%s (id, email) VALUES (4, 'alone')", f.table))

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
}
