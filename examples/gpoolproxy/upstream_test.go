// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"strings"
	"testing"
)

func TestParseUpstream(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		network string
		address string
		user    string
		db      string
		tls     bool
		must    bool
	}{
		{"everything defaulted", "postgres://app@db.example", "tcp", "db.example:5432", "app", "app", true, false},
		{"port and database", "postgresql://app:pw@db.example:6000/orders?sslmode=disable", "tcp", "db.example:6000", "app", "orders", false, false},
		{"required TLS", "postgres://app@db.example/x?sslmode=require", "tcp", "db.example:5432", "app", "x", true, true},
		{"socket directory", "postgres://app@/orders?host=/var/run/postgresql", "unix", "/var/run/postgresql/.s.PGSQL.5432", "app", "orders", false, false},
		{"socket never encrypts", "postgres://app@/orders?host=/run/pg&port=6000&sslmode=require", "unix", "/run/pg/.s.PGSQL.6000", "app", "orders", false, false},
		{"IPv6", "postgres://app@[::1]:5433/x?sslmode=disable", "tcp", "[::1]:5433", "app", "x", false, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseUpstream(test.url)
			if err != nil {
				t.Fatalf("parseUpstream() = %v", err)
			}
			if got.network != test.network || got.address != test.address {
				t.Errorf("dial target = %s %s, want %s %s", got.network, got.address, test.network, test.address)
			}
			if got.user != test.user || got.database != test.db {
				t.Errorf("user, database = %q, %q, want %q, %q", got.user, got.database, test.user, test.db)
			}
			if (got.tls != nil) != test.tls || got.required != test.must {
				t.Errorf("tls = %v required = %v, want %v, %v", got.tls != nil, got.required, test.tls, test.must)
			}
		})
	}
}

// TestParseUpstreamNeverRepeatsThePassword covers every way a connection string
// can be refused. The string holds the proxy's own database password, and the
// error goes to stderr and from there to wherever logs go.
func TestParseUpstreamNeverRepeatsThePassword(t *testing.T) {
	const secret = "s3cr3t-do-not-print"

	refused := []string{
		"postgres://app:" + secret + "@db.example:notaport/x",
		"postgres://app:" + secret + "@db.example/x?sslmode=sometimes",
		"postgres://app:" + secret + "@db.example/x?connect_timeout=soon",
		"postgres://app:" + secret + "@db.example/x?sslrootcert=/no/such/file&sslmode=verify-full",
		"postgres://:" + secret + "@db.example/x",
		"mysql://app:" + secret + "@db.example/x",
		"host=db.example user=app password=" + secret,
		"postgres://app:" + secret + "@db.example/%zz",
	}
	for _, url := range refused {
		_, err := parseUpstream(url)
		if err == nil {
			t.Errorf("parseUpstream(%q) succeeded", strings.ReplaceAll(url, secret, "…"))
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error repeats the password: %v", err)
		}
	}
}
