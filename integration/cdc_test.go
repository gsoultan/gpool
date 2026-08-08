// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package integration

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	postgrescdc "github.com/gsoultan/gpool/pkg/vendors/postgres/cdc"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

// cdcFixture is a disposable table, publication, and slot for one test.
type cdcFixture struct {
	pool       gpool.Pool
	conn       string
	table      string
	slot       string
	publicaton string
}

// newCDCFixture provisions the server-side objects a CDC test needs, skipping the
// test when the server is not configured for logical replication.
func newCDCFixture(t *testing.T) *cdcFixture {
	t.Helper()

	pool := newPool(t, postgrespool.Config{MaxConns: 4})
	ctx := t.Context()

	var walLevel string
	if err := pool.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		t.Fatalf("SHOW wal_level = %v", err)
	}
	if walLevel != "logical" {
		t.Skipf("wal_level is %q, CDC needs 'logical'", walLevel)
	}

	// A per-test suffix keeps parallel runs and leftovers from previous runs apart.
	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	f := &cdcFixture{
		pool:       pool,
		conn:       connString(t),
		table:      "gpool_cdc_" + suffix,
		slot:       "gpool_slot_" + suffix,
		publicaton: "gpool_pub_" + suffix,
	}

	create := fmt.Sprintf(
		`CREATE TABLE %s (id bigserial PRIMARY KEY, email text, bio text)`, f.table)
	if _, err := pool.Exec(ctx, create); err != nil {
		t.Fatalf("creating the fixture table = %v", err)
	}
	// REPLICA IDENTITY FULL makes the before image available on update and delete.
	if _, err := pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", f.table)); err != nil {
		t.Fatalf("setting REPLICA IDENTITY = %v", err)
	}

	t.Cleanup(func() {
		// A slot that outlives its consumer retains WAL forever, so cleanup is not optional.
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		_, _ = pool.Exec(cleanup, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", f.publicaton))
		_, _ = pool.Exec(cleanup,
			`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, f.slot)
		_, _ = pool.Exec(cleanup, fmt.Sprintf("DROP TABLE IF EXISTS %s", f.table))
	})

	return f
}

func (f *cdcFixture) config() postgrescdc.Config {
	return postgrescdc.Config{
		ConnString:        f.conn,
		SlotName:          f.slot,
		PublicationName:   f.publicaton,
		Tables:            []string{"public." + f.table},
		CreateSlot:        true,
		CreatePublication: true,
		StandbyInterval:   time.Second,
	}
}

func (f *cdcFixture) subscribe(t *testing.T) cdc.Subscriber {
	t.Helper()

	subscriber, err := gpool.NewSubscriber(gpool.Postgres, f.config())
	if err != nil {
		t.Fatalf("NewSubscriber() = %v", err)
	}
	t.Cleanup(func() { _ = subscriber.Close() })
	return subscriber
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

func TestCDCStreamsChanges(t *testing.T) {
	f := newCDCFixture(t)
	subscriber := f.subscribe(t)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	collected := make(chan []cdc.Event, 1)
	go func() {
		collected <- collect(t, stream, 3, 20*time.Second)
	}()

	// Give the walsender a moment to reach a steady state before generating changes.
	time.Sleep(500 * time.Millisecond)

	ctx := t.Context()
	if _, err := f.pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (email, bio) VALUES ($1, $2)", f.table), "a@example.com", "first"); err != nil {
		t.Fatalf("INSERT = %v", err)
	}
	if _, err := f.pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = $1", f.table), "b@example.com"); err != nil {
		t.Fatalf("UPDATE = %v", err)
	}
	if _, err := f.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s", f.table)); err != nil {
		t.Fatalf("DELETE = %v", err)
	}

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
		// Every event carries the position needed to resume from it.
		if event.Position == cdc.NoPosition {
			t.Errorf("event %d has no position", i)
		}
		// And the commit time pgoutput reports in the Begin that opened the
		// transaction. A zero value means the Begin was decoded but discarded.
		if event.Timestamp.IsZero() {
			t.Errorf("event %d has no commit timestamp", i)
		}
		if age := time.Since(event.Timestamp); age > time.Hour || age < -time.Hour {
			t.Errorf("event %d commit timestamp is %s, %s from now", i, event.Timestamp, age)
		}
	}

	if got := events[0].After["email"]; got != "a@example.com" {
		t.Errorf("insert After[email] = %v", got)
	}
	if got := events[1].After["email"]; got != "b@example.com" {
		t.Errorf("update After[email] = %v", got)
	}
	// REPLICA IDENTITY FULL, so the before image is present.
	if got := events[1].Before["email"]; got != "a@example.com" {
		t.Errorf("update Before[email] = %v", got)
	}
	if got := events[2].Before["email"]; got != "b@example.com" {
		t.Errorf("delete Before[email] = %v", got)
	}
}

// The subscriber must resume from the slot's confirmed position, replaying what
// happened while it was disconnected. Starting from the server's current WAL head
// instead silently discarded that backlog.
func TestCDCResumesFromTheSlotAfterReconnect(t *testing.T) {
	f := newCDCFixture(t)
	ctx := t.Context()

	// First session: create the slot and publication, then close without consuming.
	first := f.subscribe(t)
	stream, err := first.Subscribe(ctx)
	if err != nil {
		t.Fatalf("first Subscribe() = %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := stream.Close(); err != nil {
		t.Fatalf("closing the first stream = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("closing the first subscriber = %v", err)
	}

	// Changes made while nothing is streaming. The slot retains them.
	for i := range 3 {
		if _, err := f.pool.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s (email) VALUES ($1)", f.table),
			fmt.Sprintf("offline-%d@example.com", i)); err != nil {
			t.Fatalf("INSERT while disconnected = %v", err)
		}
	}

	// Second session over the same slot must see them.
	second, err := gpool.NewSubscriber(gpool.Postgres, f.config())
	if err != nil {
		t.Fatalf("second NewSubscriber() = %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	resumed, err := second.Subscribe(ctx)
	if err != nil {
		t.Fatalf("second Subscribe() = %v", err)
	}

	events := collect(t, resumed, 3, 20*time.Second)
	if len(events) != 3 {
		t.Fatalf("got %d events after reconnect, want the 3 retained by the slot", len(events))
	}
	for i, event := range events {
		want := fmt.Sprintf("offline-%d@example.com", i)
		if got := event.After["email"]; got != want {
			t.Errorf("event %d After[email] = %v, want %q", i, got, want)
		}
	}
}

// Table management runs on its own control connection, so it must work while a
// stream is live. Sharing the walsender connection corrupted the protocol.
// insertRows writes one row per statement, so each is its own transaction and a
// position between them is a real boundary.
func (f *cdcFixture) insertRows(t *testing.T, emails ...string) {
	t.Helper()

	for _, email := range emails {
		if _, err := f.pool.Exec(t.Context(),
			fmt.Sprintf("INSERT INTO %s (email) VALUES ($1)", f.table), email); err != nil {
			t.Fatalf("INSERT %s = %v", email, err)
		}
	}
}

func emailsOf(events []cdc.Event) []string {
	seen := make([]string, 0, len(events))
	for _, event := range events {
		seen = append(seen, fmt.Sprint(event.After["email"]))
	}
	return seen
}

// SubscribeFrom is what lets a consumer keep its own bookkeeping rather than
// trusting the slot, and against a source with no server-side position it is the
// only way to resume at all. Here it has to move the stream forward: everything
// before the recorded position stays skipped.
//
// Delivery is at-least-once, and the recorded position sits at the end of a
// row's record while its transaction commits slightly later, so the event the
// position came from may arrive again. That is the guarantee working, not a
// defect — which is why this asserts on what must not reappear rather than on an
// exact list.
func TestCDCResumesForwardFromARecordedPosition(t *testing.T) {
	f := newCDCFixture(t)
	subscriber := f.subscribe(t)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	f.insertRows(t, "r1", "r2", "r3", "r4")

	first := collect(t, stream, 4, 20*time.Second)
	if len(first) != 4 {
		t.Fatalf("collected %d events, want 4", len(first))
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	checkpoint := first[3].Position
	if checkpoint == cdc.NoPosition {
		t.Fatal("event carries no position to resume from")
	}
	f.insertRows(t, "r5")

	resumed, err := subscriber.SubscribeFrom(t.Context(), checkpoint)
	if err != nil {
		t.Fatalf("SubscribeFrom(%q) = %v", checkpoint, err)
	}
	defer resumed.Close()

	got := emailsOf(collect(t, resumed, 2, 20*time.Second))
	t.Logf("resuming after %q delivered %v", checkpoint, got)

	if !slices.Contains(got, "r5") {
		t.Errorf("resumed stream = %v, want it to reach r5", got)
	}
	for _, stale := range []string{"r1", "r2", "r3"} {
		if slices.Contains(got, stale) {
			t.Errorf("resumed stream replayed %s, so it did not resume forward: %v", stale, got)
		}
	}
}

// PostgreSQL clamps a start position up to the slot's confirmed_flush_lsn and
// says nothing, so a consumer resuming from older bookkeeping would get a stream
// with a hole in it that looks complete. Refusing is the only way the caller
// finds out.
func TestCDCRefusesToResumeBehindTheSlot(t *testing.T) {
	f := newCDCFixture(t)
	subscriber := f.subscribe(t)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	f.insertRows(t, "r1", "r2", "r3", "r4")

	first := collect(t, stream, 4, 20*time.Second)
	if len(first) != 4 {
		t.Fatalf("collected %d events, want 4", len(first))
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	// The stream confirmed its way past the first event as the consumer drained
	// it, so that position is now behind the slot.
	_, err = subscriber.SubscribeFrom(t.Context(), first[0].Position)
	if !errors.Is(err, postgrescdc.ErrPositionBehindSlot) {
		t.Fatalf("SubscribeFrom(%q) = %v, want ErrPositionBehindSlot", first[0].Position, err)
	}
}

// A position from another vendor must not start a stream. Coercing it would
// resume from an arbitrary point in the WAL without saying so.
func TestCDCRejectsAForeignPosition(t *testing.T) {
	f := newCDCFixture(t)
	subscriber := f.subscribe(t)

	_, err := subscriber.SubscribeFrom(t.Context(), "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")
	if err == nil {
		t.Fatal("SubscribeFrom() accepted a MySQL GTID set as a PostgreSQL position")
	}
}

func TestCDCTableManagementDuringStreaming(t *testing.T) {
	f := newCDCFixture(t)
	subscriber := f.subscribe(t)
	ctx := t.Context()

	other := f.table + "_other"
	if _, err := f.pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigserial PRIMARY KEY, note text)", other)); err != nil {
		t.Fatalf("creating the second table = %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = f.pool.Exec(cleanup, fmt.Sprintf("DROP TABLE IF EXISTS %s", other))
	})

	stream, err := subscriber.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	collected := make(chan []cdc.Event, 1)
	go func() {
		collected <- collect(t, stream, 1, 20*time.Second)
	}()

	time.Sleep(500 * time.Millisecond)

	// Management traffic while the stream is running.
	if err := subscriber.AddTables(ctx, "public."+other); err != nil {
		t.Fatalf("AddTables() during streaming = %v", err)
	}
	if !subscriber.IsTracking("public." + other) {
		t.Error("IsTracking() should report the newly added table")
	}
	tracked, err := subscriber.VerifyTable(ctx, "public."+other)
	if err != nil {
		t.Fatalf("VerifyTable() during streaming = %v", err)
	}
	if !tracked {
		t.Error("VerifyTable() = false for a table just added to the publication")
	}

	// The stream survived the management traffic and still delivers changes.
	if _, err := f.pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (email) VALUES ($1)", f.table), "still@example.com"); err != nil {
		t.Fatalf("INSERT = %v", err)
	}

	events := <-collected
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1 after concurrent table management", len(events))
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream failed during table management: %v", err)
	}
}

// A second stream on one subscriber would need a second walsender connection; the
// subscriber refuses rather than quietly corrupting the first.
func TestCDCRefusesASecondStream(t *testing.T) {
	f := newCDCFixture(t)
	subscriber := f.subscribe(t)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	defer func() { _ = stream.Close() }()

	if _, err := subscriber.Subscribe(t.Context()); !errors.Is(err, postgrescdc.ErrAlreadySubscribed) {
		t.Fatalf("second Subscribe() = %v, want ErrAlreadySubscribed", err)
	}
}

// Closing a stream twice, and closing the subscriber that owns it, must all be safe.
func TestCDCCloseIsIdempotent(t *testing.T) {
	f := newCDCFixture(t)
	subscriber := f.subscribe(t)

	stream, err := subscriber.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("first stream Close() = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second stream Close() = %v", err)
	}
	if err := subscriber.Close(); err != nil {
		t.Fatalf("subscriber Close() = %v", err)
	}
	if err := subscriber.Close(); err != nil {
		t.Fatalf("second subscriber Close() = %v", err)
	}
}

// An unchanged TOASTed value is not transmitted. It must be absent from the map,
// not recorded as a NULL that would blank the column on replay.
func TestCDCOmitsUnchangedToastedColumns(t *testing.T) {
	f := newCDCFixture(t)
	ctx := t.Context()

	// Default identity means the update sends only the changed columns, and a large
	// external value that did not change is left out entirely.
	if _, err := f.pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY DEFAULT", f.table)); err != nil {
		t.Fatalf("setting REPLICA IDENTITY DEFAULT = %v", err)
	}
	if _, err := f.pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN bio SET STORAGE EXTERNAL", f.table)); err != nil {
		t.Fatalf("forcing external storage = %v", err)
	}

	large := make([]byte, 16*1024)
	for i := range large {
		large[i] = 'x'
	}
	if _, err := f.pool.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (email, bio) VALUES ($1, $2)", f.table),
		"toast@example.com", string(large)); err != nil {
		t.Fatalf("INSERT = %v", err)
	}

	subscriber := f.subscribe(t)
	stream, err := subscriber.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	collected := make(chan []cdc.Event, 1)
	go func() {
		collected <- collect(t, stream, 1, 20*time.Second)
	}()

	time.Sleep(500 * time.Millisecond)
	if _, err := f.pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = $1", f.table), "changed@example.com"); err != nil {
		t.Fatalf("UPDATE = %v", err)
	}

	events := <-collected
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}

	after := events[0].After
	if got := after["email"]; got != "changed@example.com" {
		t.Errorf("After[email] = %v", got)
	}
	if value, present := after["bio"]; present && value == nil {
		t.Fatal("an unchanged TOASTed column was reported as SQL NULL instead of being omitted")
	}
}
