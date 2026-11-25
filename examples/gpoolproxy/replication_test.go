// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/pooling"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// requireLogical skips a test on a server that cannot decode its WAL.
func requireLogical(t *testing.T) {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	conn, err := pgx.Connect(t.Context(), url)
	if err != nil {
		t.Fatalf("Connect() = %v", err)
	}
	defer conn.Close(context.Background())

	var level string
	if err := conn.QueryRow(t.Context(), "SHOW wal_level").Scan(&level); err != nil {
		t.Fatalf("SHOW wal_level = %v", err)
	}
	if level != "logical" {
		t.Skipf("wal_level is %q, need logical", level)
	}
}

// replicationConn connects through the proxy asking for logical replication.
func replicationConn(t *testing.T, url string) (*pgconn.PgConn, error) {
	t.Helper()

	config, err := pgconn.ParseConfig(url)
	if err != nil {
		t.Fatalf("ParseConfig() = %v", err)
	}
	config.RuntimeParams["replication"] = "database"

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	return pgconn.ConnectConfig(ctx, config)
}

func sqlState(err error) string {
	var serverErr *pgconn.PgError
	if errors.As(err, &serverErr) {
		return serverErr.Code
	}
	return ""
}

// TestReplicationClientStreamsThroughADedicatedConnection is the feature.
//
// Before it, the replication startup parameter was ignored: the client got an
// ordinary pooled backend, and its first walsender command came back as a
// syntax error from a server that had never been told this was a replication
// connection. The pool here has one connection, and a second client runs
// queries on it while the stream is open, which is only possible if the stream
// is somewhere else.
func TestReplicationClientStreamsThroughADedicatedConnection(t *testing.T) {
	requireLogical(t)

	url := startProxyWith(t, func(c *Config) {
		c.MaxReplicationClients = 1
		c.Pool = pooling.Config{MaxConns: 1}
	})

	stream, err := replicationConn(t, url)
	if err != nil {
		t.Fatalf("replication connect = %v", err)
	}
	defer stream.Close(context.Background())

	if _, err := stream.Exec(t.Context(), "IDENTIFY_SYSTEM").ReadAll(); err != nil {
		t.Fatalf("IDENTIFY_SYSTEM = %v", err)
	}

	slot := fmt.Sprintf("gpoolproxy_test_%d", time.Now().UnixNano())
	create := fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL test_decoding", slot)
	if _, err := stream.Exec(t.Context(), create).ReadAll(); err != nil {
		t.Fatalf("CREATE_REPLICATION_SLOT = %v", err)
	}

	// An ordinary client on the one pooled connection, while the stream's
	// session is alive and holding its own.
	client, err := pgx.Connect(t.Context(), url)
	if err != nil {
		t.Fatalf("Connect() = %v", err)
	}
	defer client.Close(context.Background())

	table := slot + "_t"
	if _, err := client.Exec(t.Context(), "CREATE TABLE "+table+" (id int PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}
	defer client.Exec(context.Background(), "DROP TABLE "+table)

	frontend := stream.Frontend()
	frontend.SendQuery(&pgproto3.Query{String: fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL 0/0", slot)})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("START_REPLICATION = %v", err)
	}

	if _, err := client.Exec(t.Context(), "INSERT INTO "+table+" VALUES (4242)"); err != nil {
		t.Fatalf("INSERT = %v", err)
	}

	stream.Conn().SetDeadline(time.Now().Add(20 * time.Second))
	want := []byte("id[integer]:4242")
	for seen := false; !seen; {
		message, err := frontend.Receive()
		if err != nil {
			t.Fatalf("Receive() = %v", err)
		}
		switch message := message.(type) {
		case *pgproto3.ErrorResponse:
			t.Fatalf("server error %s: %s", message.Code, message.Message)
		case *pgproto3.CopyData:
			seen = bytes.Contains(message.Data, want)
		}
	}

	// The other direction of CopyBoth: a standby status update, then the end of
	// the stream. The server answers CopyDone with its own and a ReadyForQuery,
	// which only arrives if both made it through.
	status := make([]byte, 34)
	status[0] = 'r'
	frontend.Send(&pgproto3.CopyData{Data: status})
	frontend.Send(&pgproto3.CopyDone{})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("CopyDone = %v", err)
	}
	for ready := false; !ready; {
		message, err := frontend.Receive()
		if err != nil {
			t.Fatalf("Receive() after CopyDone = %v", err)
		}
		_, ready = message.(*pgproto3.ReadyForQuery)
	}
}

// TestReplicationIsRefusedUnlessEnabled covers the default. Every client reaches
// the server as the one upstream role, so passing replication through is an
// operator's decision, and a proxy that was not told to must say so rather than
// hand over a pooled connection that fails on the first command.
func TestReplicationIsRefusedUnlessEnabled(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 1})

	conn, err := replicationConn(t, url)
	if err == nil {
		conn.Close(context.Background())
		t.Fatal("replication connect succeeded on a proxy that does not allow it")
	}
	if code := sqlState(err); code != "0A000" {
		t.Fatalf("SQLSTATE = %q (%v), want 0A000", code, err)
	}
}

// TestReplicationClientsAreBoundedAndTheirPlaceComesBack covers the bound and
// its release. Each replication client holds a server connection and a
// walsender for as long as it stays, so the count must be capped, and a place
// that did not come back when its client left would refuse everyone after the
// first few.
func TestReplicationClientsAreBoundedAndTheirPlaceComesBack(t *testing.T) {
	requireLogical(t)

	url := startProxyWith(t, func(c *Config) {
		c.MaxReplicationClients = 1
		c.Pool = pooling.Config{MaxConns: 1}
	})

	first, err := replicationConn(t, url)
	if err != nil {
		t.Fatalf("first replication connect = %v", err)
	}

	second, err := replicationConn(t, url)
	if err == nil {
		second.Close(context.Background())
		t.Fatal("second replication connect succeeded past a limit of one")
	}
	if code := sqlState(err); code != "53300" {
		t.Fatalf("SQLSTATE = %q (%v), want 53300", code, err)
	}

	first.Close(context.Background())

	// The proxy learns of the disconnect from its event loop, not from Close
	// returning here.
	deadline := time.Now().Add(5 * time.Second)
	for {
		third, err := replicationConn(t, url)
		if err == nil {
			third.Close(context.Background())
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("place never came back: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLogicalChangesCanBePolledInTransactionMode covers CDC without a stream:
// pg_logical_slot_get_changes is an ordinary function call, complete within one
// statement, so it needs nothing from the proxy but to be passed along.
func TestLogicalChangesCanBePolledInTransactionMode(t *testing.T) {
	requireLogical(t)

	url := startProxy(t, pooling.Config{MaxConns: 1})
	conn, err := pgx.Connect(t.Context(), url)
	if err != nil {
		t.Fatalf("Connect() = %v", err)
	}
	defer conn.Close(context.Background())

	slot := fmt.Sprintf("gpoolproxy_poll_%d", time.Now().UnixNano())
	table := slot + "_t"
	if _, err := conn.Exec(t.Context(), "SELECT pg_create_logical_replication_slot($1, 'test_decoding')", slot); err != nil {
		t.Fatalf("create slot = %v", err)
	}
	defer conn.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", slot)

	if _, err := conn.Exec(t.Context(), "CREATE TABLE "+table+" (id int PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE = %v", err)
	}
	defer conn.Exec(context.Background(), "DROP TABLE "+table)
	if _, err := conn.Exec(t.Context(), "INSERT INTO "+table+" VALUES (7)"); err != nil {
		t.Fatalf("INSERT = %v", err)
	}

	rows, err := conn.Query(t.Context(), "SELECT data FROM pg_logical_slot_get_changes($1, NULL, NULL)", slot)
	if err != nil {
		t.Fatalf("get_changes = %v", err)
	}
	changes, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("CollectRows() = %v", err)
	}
	for _, change := range changes {
		if bytes.Contains([]byte(change), []byte("id[integer]:7")) {
			return
		}
	}
	t.Fatalf("insert not among the polled changes: %q", changes)
}
