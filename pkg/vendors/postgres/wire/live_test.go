// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"
)

// Tests against a real server. A scripted server proves the state machine; only
// PostgreSQL proves this speaks the protocol PostgreSQL does.
func liveConfig(t *testing.T) *Config {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	config, err := ParseConfig(dsn)
	if err != nil {
		t.Fatalf("ParseConfig() = %v", err)
	}
	return config
}

func liveConn(t *testing.T) *Conn {
	t.Helper()
	conn, err := Connect(t.Context(), liveConfig(t))
	if err != nil {
		t.Fatalf("Connect() = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func TestLiveConnectAndQuery(t *testing.T) {
	conn := liveConn(t)
	if conn.ParameterStatus("server_version") == "" {
		t.Error("no server_version reported")
	}
	if conn.PID() == 0 {
		t.Error("no backend key data")
	}
	result, err := conn.QueryText(t.Context(), "SELECT $1::int + 1, NULL::text, ''::text", "41")
	if err != nil {
		t.Fatalf("QueryText() = %v", err)
	}
	row := result.Rows[0]
	if string(row[0]) != "42" || row[1] != nil || row[2] == nil || len(row[2]) != 0 {
		t.Fatalf("row = %q", row)
	}
	if result.Tag != "SELECT 1" {
		t.Errorf("tag = %q", result.Tag)
	}
	if conn.TxStatus() != TxIdle {
		t.Errorf("tx status = %c", conn.TxStatus())
	}
}

// A server error leaves the connection at a boundary, usable.
func TestLiveServerErrorKeepsTheConnection(t *testing.T) {
	conn := liveConn(t)
	_, err := conn.Exec(t.Context(), "SELECT * FROM no_such_table_here")
	var serverErr *Error
	if !errors.As(err, &serverErr) || serverErr.Code != "42P01" {
		t.Fatalf("err = %v, want SQLSTATE 42P01", err)
	}
	if _, err := conn.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("after an error: %v", err)
	}
}

// A context that ends mid-query cancels it on the server and closes the
// connection, and the error says it was the context.
func TestLiveContextCancelsTheQuery(t *testing.T) {
	conn := liveConn(t)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := conn.Exec(ctx, "SELECT pg_sleep(10)")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatalf("took %v", time.Since(started))
	}
	if !conn.IsClosed() {
		t.Fatal("the connection was left open with a query in flight")
	}
}

// A wrong password is the server's SQLSTATE, not a string to match.
func TestLiveWrongPassword(t *testing.T) {
	config := liveConfig(t)
	if os.Getenv("DATABASE_URL_SCRAM") == "" {
		t.Skip("DATABASE_URL_SCRAM not set")
	}
	scram, err := ParseConfig(os.Getenv("DATABASE_URL_SCRAM"))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(t.Context(), scram)
	if err != nil {
		t.Fatalf("SCRAM login = %v", err)
	}
	conn.Close(t.Context())

	scram.Password = "wrong"
	_, err = Connect(t.Context(), scram)
	var serverErr *Error
	if !errors.As(err, &serverErr) || serverErr.Code != "28P01" {
		t.Fatalf("err = %v, want 28P01", err)
	}
	_ = config
	_ = url.QueryEscape
}
