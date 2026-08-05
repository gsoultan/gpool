// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// A position is the only durable record a MySQL consumer has of its progress,
// so the round trip has to be exact in both notations.
func TestPositionRoundTrips(t *testing.T) {
	tests := []struct {
		name   string
		text   cdc.Position
		flavor string
	}{
		{"gtid, single range", "gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", mysql.MySQLFlavor},
		{"gtid, several servers", "gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,de346a55-71ca-11e1-9e33-c80aa9429562:1-9", mysql.MySQLFlavor},
		{"file and offset", "file:mysql-bin.000042:1234", mysql.MySQLFlavor},
		{"file at zero", "file:mysql-bin.000001:4", mysql.MySQLFlavor},
		{"mariadb gtid", "gtid:0-1-100", mysql.MariaDBFlavor},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := parsePosition(test.text, test.flavor)
			if err != nil {
				t.Fatalf("parsePosition(%q) = %v", test.text, err)
			}
			if got := parsed.encode(); got != test.text {
				t.Errorf("round trip = %q, want %q", got, test.text)
			}
		})
	}
}

// The two notations are not interchangeable — a GTID set survives a failover to
// a replica and a file offset does not — and both contain colons, so the tag is
// what stops one being read as the other.
func TestPositionDistinguishesItsTwoNotations(t *testing.T) {
	gtid, err := parsePosition("gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", mysql.MySQLFlavor)
	if err != nil {
		t.Fatalf("parsePosition() = %v", err)
	}
	if !gtid.isGTID() {
		t.Error("a gtid: position did not parse as a GTID set")
	}

	file, err := parsePosition("file:mysql-bin.000042:1234", mysql.MySQLFlavor)
	if err != nil {
		t.Fatalf("parsePosition() = %v", err)
	}
	if file.isGTID() {
		t.Error("a file: position parsed as a GTID set")
	}
	if file.file.Name != "mysql-bin.000042" || file.file.Pos != 1234 {
		t.Errorf("parsed %+v, want mysql-bin.000042 at 1234", file.file)
	}
}

// A position from another vendor must be refused rather than coerced. Starting a
// binlog dump from a misread place yields a stream that begins somewhere else
// entirely and says nothing about it.
func TestParsePositionRejectsAForeignPosition(t *testing.T) {
	tests := []struct {
		name     string
		position cdc.Position
	}{
		{"PostgreSQL LSN", "0/1A2B3C4D"},
		{"untagged GTID set", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{"untagged file offset", "mysql-bin.000042:1234"},
		{"empty", ""},
		{"tagged but empty file", "file:"},
		{"file with no offset", "file:mysql-bin.000042"},
		{"file with a non-numeric offset", "file:mysql-bin.000042:abc"},
		{"gtid that is not a set", "gtid:not-a-gtid"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parsePosition(test.position, mysql.MySQLFlavor); !errors.Is(err, ErrInvalidPosition) {
				t.Errorf("parsePosition(%q) error = %v, want ErrInvalidPosition", test.position, err)
			}
		})
	}
}

// MariaDB and MySQL write GTIDs differently, so a set from one must not parse as
// the other's — reading it wrong would resume from a place that does not exist.
func TestParsePositionIsFlavourSpecific(t *testing.T) {
	if _, err := parsePosition("gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", mysql.MariaDBFlavor); err == nil {
		t.Error("a MySQL GTID set parsed as MariaDB")
	}
}

func TestFilterMatching(t *testing.T) {
	tests := []struct {
		name    string
		tables  []string
		schema  string
		table   string
		allowed bool
	}{
		{"empty filter allows everything", nil, "app", "users", true},
		{"qualified match", []string{"app.users"}, "app", "users", true},
		{"different schema", []string{"app.users"}, "other", "users", false},
		{"different table", []string{"app.users"}, "app", "orders", false},
		{"one of several", []string{"app.orders", "app.users"}, "app", "users", true},
		// MySQL's own table-name case sensitivity depends on the host filesystem
		// via lower_case_table_names, so matching case sensitively would make one
		// configuration behave differently on Linux and macOS.
		{"case folded", []string{"App.Users"}, "app", "users", true},
		{"surrounding space ignored", []string{"  app.users  "}, "app", "users", true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := newFilter(test.tables).allows(test.schema, test.table); got != test.allowed {
				t.Errorf("allows(%q, %q) = %v, want %v", test.schema, test.table, got, test.allowed)
			}
		})
	}
}

func TestFilterMutation(t *testing.T) {
	f := newFilter([]string{"app.users"})

	f.add([]string{"app.orders"})
	if !f.allows("app", "orders") {
		t.Error("add() did not take effect")
	}
	if !f.allows("app", "users") {
		t.Error("add() dropped an existing table")
	}

	f.remove([]string{"app.users"})
	if f.allows("app", "users") {
		t.Error("remove() did not take effect")
	}

	f.set([]string{"app.invoices"})
	if f.allows("app", "orders") {
		t.Error("set() did not replace the previous contents")
	}
	if got := f.list(); len(got) != 1 || got[0] != "app.invoices" {
		t.Errorf("list() = %v, want [app.invoices]", got)
	}

	// Emptying the filter means everything, not nothing — the same reading a
	// caller who never named a table gets.
	f.set(nil)
	if !f.allows("anything", "at-all") {
		t.Error("an emptied filter must allow everything")
	}
}
