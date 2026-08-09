// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// The map behind these tests used to be unbounded, on both sides.
//
// A session remembered one Parse message per distinct statement name its client
// had ever used, and a backend remembered one per name any client had ever used
// on it — and the backend's, by its own comment, deliberately outlived the
// session that created it, because the statement really does live on until
// something deallocates it. Nothing ever did. A client that prepares under
// generated names and never closes them therefore grew both the proxy's heap and
// the server's, without limit, at the client's discretion. Multiplied by the
// three thousand clients this example exists to demonstrate, that is the memory
// exhaustion vector the pool's own MaxClients bound was added to prevent, reached
// by a different road.

// parseMessage builds a Parse naming a statement, which is what is remembered.
func parseMessage(name, sql string) []byte {
	body := make([]byte, 0, len(name)+len(sql)+4)
	body = append(body, name...)
	body = append(body, 0)
	body = append(body, sql...)
	body = append(body, 0, 0, 0) // no parameter types

	message := make([]byte, headerSize+len(body))
	message[0] = msgParse
	binary.BigEndian.PutUint32(message[1:], uint32(len(body)+4))
	copy(message[headerSize:], body)
	return message
}

func TestStatementsHoldTheirLimit(t *testing.T) {
	held := newStatements(4)

	for i := range 100 {
		name := fmt.Sprintf("s%d", i)
		held.remember(name, parseMessage(name, "SELECT 1"))
	}

	if got := held.len(); got != 4 {
		t.Errorf("len() = %d after 100 statements, want the limit (4)", got)
	}
}

// The point of an LRU rather than a plain cap is that a client with a working set
// smaller than the limit never notices it exists, however many statements it has
// cycled through.
func TestStatementsEvictTheLeastRecentlyUsed(t *testing.T) {
	held := newStatements(3)

	for _, name := range []string{"a", "b", "c"} {
		held.remember(name, parseMessage(name, "SELECT 1"))
	}
	// "a" becomes the most recent, so "b" is now the one to go.
	if _, ok := held.lookup("a"); !ok {
		t.Fatal("lookup(a) missed a statement that was just remembered")
	}

	evicted, ok := held.remember("d", parseMessage("d", "SELECT 1"))
	if !ok {
		t.Fatal("remembering a fourth statement past a limit of 3 evicted nothing")
	}
	if evicted != "b" {
		t.Errorf("evicted %q, want the least recently used (b)", evicted)
	}
	for _, name := range []string{"a", "c", "d"} {
		if _, ok := held.lookup(name); !ok {
			t.Errorf("lookup(%s) = false, want it kept", name)
		}
	}
	if _, ok := held.lookup("b"); ok {
		t.Error("lookup(b) = true, want it evicted")
	}
}

// Re-remembering a name is what a client does when it deallocates a statement and
// prepares a different one under the same name. It must replace rather than
// accumulate, or the limit is reached by a client that only ever holds one.
func TestStatementsReplaceRatherThanAccumulate(t *testing.T) {
	held := newStatements(2)

	for range 50 {
		if _, ok := held.remember("same", parseMessage("same", "SELECT 1")); ok {
			t.Fatal("re-remembering one name evicted something")
		}
	}
	if got := held.len(); got != 1 {
		t.Errorf("len() = %d, want 1", got)
	}
}

func TestStatementsForget(t *testing.T) {
	held := newStatements(4)
	held.remember("a", parseMessage("a", "SELECT 1"))

	held.forget("a")
	if _, ok := held.lookup("a"); ok {
		t.Error("lookup(a) = true after forget")
	}
	if got := held.len(); got != 0 {
		t.Errorf("len() = %d after forget, want 0", got)
	}
}

// The unnamed statement is replaced by every Parse and is never replayed, so
// remembering it would spend the limit on the one entry that can never be used.
func TestStatementsIgnoreTheUnnamedStatement(t *testing.T) {
	held := newStatements(4)

	if _, ok := held.remember("", parseMessage("", "SELECT 1")); ok {
		t.Error("remembering the unnamed statement evicted something")
	}
	if got := held.len(); got != 0 {
		t.Errorf("len() = %d, want the unnamed statement not to be remembered", got)
	}
}

// A negative limit reinstates the unbounded behaviour deliberately, for an
// operator who would rather have the old failure than the new one.
func TestStatementsUnlimitedWhenNegative(t *testing.T) {
	held := newStatements(-1)

	for i := range 500 {
		name := fmt.Sprintf("s%d", i)
		if _, ok := held.remember(name, parseMessage(name, "SELECT 1")); ok {
			t.Fatalf("statement %d was evicted from an unlimited set", i)
		}
	}
	if got := held.len(); got != 500 {
		t.Errorf("len() = %d, want 500", got)
	}
}

// Whatever is remembered has to be a copy. The buffer a Parse arrives in is
// reused by the next message, so keeping the slice would replay whatever happened
// to be in it later.
func TestStatementsCopyWhatTheyRemember(t *testing.T) {
	held := newStatements(4)

	message := parseMessage("a", "SELECT 1")
	held.remember("a", message)
	clear(message)

	remembered, ok := held.lookup("a")
	if !ok {
		t.Fatal("lookup(a) = false")
	}
	if remembered[0] != msgParse {
		t.Error("the remembered message was overwritten when its source buffer was reused")
	}
}
