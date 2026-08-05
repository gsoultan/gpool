// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

// scratchTable creates a table for one test and drops it afterwards.
func scratchTable(t *testing.T, pool gpool.Pool, definition string) string {
	t.Helper()

	name := fmt.Sprintf("gpool_cap_%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := pool.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s %s", name, definition)); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	})
	return name
}

func TestCopyFromLoadsRows(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 4})
	table := scratchTable(t, pool, "(id int PRIMARY KEY, label text)")

	copier, ok := pool.(gpool.BulkCopier)
	if !ok {
		t.Fatal("the PostgreSQL pool should implement gpool.BulkCopier")
	}

	const rows = 5000
	copied, err := copier.CopyFrom(t.Context(), gpool.CopyRequest{
		Table:   []string{table},
		Columns: []string{"id", "label"},
		Rows: gpool.CopyFromSlice(rows, func(i int) ([]any, error) {
			return []any{i, fmt.Sprintf("label-%d", i)}, nil
		}),
	})
	if err != nil {
		t.Fatalf("CopyFrom() = %v", err)
	}
	if copied != rows {
		t.Fatalf("CopyFrom() copied %d rows, want %d", copied, rows)
	}

	var count int
	if err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatalf("count = %v", err)
	}
	if count != rows {
		t.Fatalf("table holds %d rows, want %d", count, rows)
	}

	// The connection went back to the pool usable.
	if got := pool.Stat().ActiveConnections(); got != 0 {
		t.Errorf("ActiveConnections() = %d after CopyFrom, want 0", got)
	}
}

// COPY is atomic: a source that fails part-way must leave nothing behind.
func TestCopyFromRollsBackOnSourceError(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2})
	table := scratchTable(t, pool, "(id int PRIMARY KEY)")

	copier := pool.(gpool.BulkCopier)
	sentinel := errors.New("source exhausted early")

	_, err := copier.CopyFrom(t.Context(), gpool.CopyRequest{
		Table:   []string{table},
		Columns: []string{"id"},
		Rows: gpool.CopyFromSlice(100, func(i int) ([]any, error) {
			if i == 50 {
				return nil, sentinel
			}
			return []any{i}, nil
		}),
	})
	if err == nil {
		t.Fatal("CopyFrom() with a failing source should return an error")
	}

	var count int
	if err := pool.QueryRow(t.Context(), fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatalf("count = %v", err)
	}
	if count != 0 {
		t.Fatalf("a failed copy left %d rows behind; COPY should be atomic", count)
	}
}

func TestCopyFromValidatesTheRequest(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2})
	copier := pool.(gpool.BulkCopier)

	tests := map[string]gpool.CopyRequest{
		"no table":   {Columns: []string{"id"}, Rows: gpool.CopyFromSlice(0, nil)},
		"no columns": {Table: []string{"t"}, Rows: gpool.CopyFromSlice(0, nil)},
		"no rows":    {Table: []string{"t"}, Columns: []string{"id"}},
	}

	for name, request := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := copier.CopyFrom(t.Context(), request); !errors.Is(err, postgrespool.ErrInvalidConfig) {
				t.Fatalf("CopyFrom() = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestSendBatchPipelinesStatements(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2})
	table := scratchTable(t, pool, "(id int PRIMARY KEY, label text)")

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer conn.Release()

	batcher, ok := conn.(gpool.Batcher)
	if !ok {
		t.Fatal("the PostgreSQL connection should implement gpool.Batcher")
	}

	batch := &gpool.Batch{}
	for i := range 3 {
		batch.Queue(fmt.Sprintf("INSERT INTO %s (id, label) VALUES ($1, $2)", table), i, fmt.Sprintf("row-%d", i))
	}
	batch.Queue(fmt.Sprintf("SELECT count(*) FROM %s", table))

	if batch.Len() != 4 {
		t.Fatalf("Len() = %d, want 4", batch.Len())
	}

	results := batcher.SendBatch(t.Context(), batch)

	for i := range 3 {
		result, err := results.Exec()
		if err != nil {
			t.Fatalf("statement %d: Exec() = %v", i, err)
		}
		if result.RowsAffected() != 1 {
			t.Errorf("statement %d: RowsAffected() = %d, want 1", i, result.RowsAffected())
		}
	}

	var count int
	if err := results.QueryRow().Scan(&count); err != nil {
		t.Fatalf("QueryRow() = %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}

	if err := results.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	// Close is idempotent, and reading after it is refused rather than corrupting
	// the connection.
	if err := results.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if _, err := results.Exec(); !errors.Is(err, postgrespool.ErrBatchClosed) {
		t.Errorf("Exec() after Close = %v, want ErrBatchClosed", err)
	}

	// The connection survived the batch.
	if err := conn.Ping(t.Context()); err != nil {
		t.Fatalf("Ping() after a batch = %v", err)
	}
}

func TestSendBatchRejectsAnEmptyBatch(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer conn.Release()

	batcher := conn.(gpool.Batcher)
	if err := batcher.SendBatch(t.Context(), &gpool.Batch{}).Close(); !errors.Is(err, postgrespool.ErrEmptyBatch) {
		t.Fatalf("SendBatch() with no statements = %v, want ErrEmptyBatch", err)
	}
}

func TestNotifyDeliversToAListener(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 4})

	listenerConn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer listenerConn.Release()

	listener, ok := listenerConn.(gpool.Notifier)
	if !ok {
		t.Fatal("the PostgreSQL connection should implement gpool.Notifier")
	}

	const channel = "gpool_events"
	if err := listener.Listen(t.Context(), channel); err != nil {
		t.Fatalf("Listen() = %v", err)
	}

	senderConn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	if err := senderConn.(gpool.Notifier).Notify(t.Context(), channel, "hello"); err != nil {
		t.Fatalf("Notify() = %v", err)
	}
	senderConn.Release()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	notification, err := listener.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("WaitForNotification() = %v", err)
	}
	if notification.Channel != channel {
		t.Errorf("Channel = %q, want %q", notification.Channel, channel)
	}
	if notification.Payload != "hello" {
		t.Errorf("Payload = %q, want %q", notification.Payload, "hello")
	}
	if notification.PID == 0 {
		t.Error("PID = 0, want the sending backend's pid")
	}
}

// A LISTEN belongs to the session, so it must not survive the connection going
// back to the pool. Otherwise the next caller silently receives notifications it
// never subscribed to.
func TestListenDoesNotLeakToTheNextCaller(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 1})
	const channel = "gpool_leak_probe"

	first, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	if err := first.(gpool.Notifier).Listen(t.Context(), channel); err != nil {
		t.Fatalf("Listen() = %v", err)
	}
	// Released while still subscribed - the mistake this gate exists for.
	first.Release()

	// MaxConns is 1, so the next caller gets that same connection.
	next, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer next.Release()

	var listening int
	if err := next.QueryRow(t.Context(),
		"SELECT count(*) FROM pg_listening_channels() AS c WHERE c = $1", channel).Scan(&listening); err != nil {
		t.Fatalf("probing subscriptions = %v", err)
	}
	if listening != 0 {
		t.Fatalf("the next caller inherited %d subscription(s) to %q", listening, channel)
	}

	// Unwinding the subscription must not have cost a reconnect.
	if got := pool.Stat().TotalConnections(); got != 1 {
		t.Errorf("TotalConnections() = %d, want 1 - the connection was replaced rather than cleaned", got)
	}
}

func TestNotifierValidatesChannel(t *testing.T) {
	pool := newPool(t, postgrespool.Config{MaxConns: 2})

	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("Acquire() = %v", err)
	}
	defer conn.Release()

	notifier := conn.(gpool.Notifier)
	if err := notifier.Listen(t.Context(), ""); !errors.Is(err, postgrespool.ErrInvalidConfig) {
		t.Errorf("Listen(\"\") = %v, want ErrInvalidConfig", err)
	}
	if err := notifier.Notify(t.Context(), "", "x"); !errors.Is(err, postgrespool.ErrInvalidConfig) {
		t.Errorf("Notify(\"\") = %v, want ErrInvalidConfig", err)
	}

	// A channel name with a quote must be neutralised, not injected.
	if err := notifier.Listen(t.Context(), `odd"name`); err != nil {
		t.Errorf("Listen() on a quoted channel name = %v", err)
	}
}
