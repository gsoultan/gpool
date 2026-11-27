// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gsoultan/nextar-engine/pkg/pooling"
)

// The other tests drive the proxy with a client made of the proxy's own code,
// so a mistake shared by both halves — a SCRAM proof computed wrongly the same
// way twice, a message framed the way only this code frames it — would pass
// every one of them. psql is libpq, which shares nothing with it.
//
// It is run as a program rather than linked, so the module still depends on no
// client library, and it is skipped where it is not installed.
func TestProxyServesPsql(t *testing.T) {
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql is not installed")
	}
	url := startProxy(t, pooling.Config{MaxConns: 2})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// The password travels in the environment, not in the argument list, where
	// every other process on the host could read it.
	withoutPassword := strings.Replace(url, ":"+proxyPassword+"@", "@", 1)
	command := exec.CommandContext(ctx, psql, "--no-psqlrc", "--tuples-only", "--no-align",
		"--set", "ON_ERROR_STOP=1", "--command", "SELECT 40 + 2", withoutPassword)
	command.Env = append(os.Environ(), "PGPASSWORD="+proxyPassword)

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("psql = %v: %s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != "42" {
		t.Fatalf("psql printed %q, want 42", got)
	}
}
