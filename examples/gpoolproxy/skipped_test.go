// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"testing"

	"github.com/gsoultan/nextar/pkg/pooling"
)

// failing is a statement that fails as it runs, and the server skips every
// message after it up to the Sync.
func failing() [][]byte {
	return frontendBatch("", "SELECT 1/0")
}

// A Parse or Close the proxy sends ahead of a client's message, and the
// client's own, may be skipped by the server: an error earlier in the same
// Sync skips everything up to it. The proxy used to record each as done when
// it sent it. A Close and Parse it injected behind a failing statement left it
// believing the backend held this client's statement when it held another
// client's of the same name, and the next execution ran that one instead, with
// no error: 111 where the client asked for 222. The completions it waited to
// swallow for them never came, either, and it swallowed the client's own.
func TestProxyRecordsOnlyWhatTheServerRan(t *testing.T) {
	// One backend, so both clients share it.
	url := startProxy(t, pooling.Config{MaxConns: 1})
	first, second := connect(t, url), connect(t, url)
	if err := first.prepare("shared", "SELECT 111"); err != nil {
		t.Fatal(err)
	}
	if err := second.prepare("shared", "SELECT 222"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.execute("shared"); err != nil {
		t.Fatal(err)
	}

	// The proxy puts the second client's statement back in front of its
	// Bind, behind a statement that fails.
	messages := append(failing(), frontendBind("shared"), frontendExecute(), frontendSync())
	if err := second.send(messages...); err != nil {
		t.Fatal(err)
	}
	if _, err := second.untilReady(); sqlState(err) != "22012" {
		t.Fatalf("the failing exchange = %v, want SQLSTATE 22012", err)
	}

	rows, err := second.execute("shared")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := single(rows); got != "222" {
		t.Fatalf("the second client got %q, want 222: it ran the other client's statement", got)
	}

	// Every reply the client is owed reaches it.
	if err := second.send(frontendParse("", "SELECT 5"), frontendSync()); err != nil {
		t.Fatal(err)
	}
	if kind, _, err := second.receive(); err != nil || kind != msgParseComplete {
		t.Fatalf("the first reply to a Parse is %q, %v; want a ParseComplete", kind, err)
	}
	if _, err := second.untilReady(); err != nil {
		t.Fatal(err)
	}
}

// A Close the server skipped leaves the statement where it was. The proxy used
// to forget it all the same: the next client to prepare the same name on that
// backend was refused with 42P05, and the client whose Close it was found its
// statement gone from its own set, so that a later execution on that backend
// ran whatever was there.
func TestProxyKeepsAStatementWhoseCloseWasSkipped(t *testing.T) {
	url := startProxy(t, pooling.Config{MaxConns: 1})
	first, second := connect(t, url), connect(t, url)
	if err := first.prepare("temporary", "SELECT 7"); err != nil {
		t.Fatal(err)
	}
	messages := append(failing(), frontendClose('S', "temporary"), frontendSync())
	if err := first.send(messages...); err != nil {
		t.Fatal(err)
	}
	if _, err := first.untilReady(); sqlState(err) != "22012" {
		t.Fatalf("the failing exchange = %v, want SQLSTATE 22012", err)
	}

	if err := second.prepare("temporary", "SELECT 8"); err != nil {
		t.Fatalf("another client preparing the same name = %v", err)
	}
	for _, check := range []struct {
		client *wireClient
		want   string
	}{{second, "8"}, {first, "7"}, {second, "8"}} {
		rows, err := check.client.execute("temporary")
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := single(rows); got != check.want {
			t.Fatalf("got %q, want %s", got, check.want)
		}
	}
}
