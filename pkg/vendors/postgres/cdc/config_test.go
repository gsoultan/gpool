// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"errors"
	"testing"
	"time"
)

const testConnString = "postgres://user:pass@127.0.0.1:5432/testdb"

func validConfig() Config {
	return Config{
		ConnString:      testConnString,
		SlotName:        "gpool_slot",
		PublicationName: "gpool_pub",
		Tables:          []string{"public.users"},
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "valid", mutate: func(*Config) {}},
		{
			name:   "publication creation with tables",
			mutate: func(c *Config) { c.CreatePublication = true },
		},
		{
			name:    "missing conn string",
			mutate:  func(c *Config) { c.ConnString = "" },
			wantErr: true,
		},
		{
			name:    "missing slot name",
			mutate:  func(c *Config) { c.SlotName = "" },
			wantErr: true,
		},
		{
			name:    "missing publication name",
			mutate:  func(c *Config) { c.PublicationName = "" },
			wantErr: true,
		},
		{
			name:    "publication creation without tables",
			mutate:  func(c *Config) { c.CreatePublication = true; c.Tables = nil },
			wantErr: true,
		},
		// Slot names reach the replication command unquoted, so the character set is
		// enforced up front rather than trusted.
		{
			name:    "slot name with uppercase",
			mutate:  func(c *Config) { c.SlotName = "GpoolSlot" },
			wantErr: true,
		},
		{
			name:    "slot name with a quote",
			mutate:  func(c *Config) { c.SlotName = `slot"; DROP` },
			wantErr: true,
		},
		{
			name:    "slot name with a space",
			mutate:  func(c *Config) { c.SlotName = "my slot" },
			wantErr: true,
		},
		{
			name:    "slot name too long",
			mutate:  func(c *Config) { c.SlotName = string(make([]byte, 0, 64)) + longSlotName() },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := validConfig()
			tt.mutate(&config)

			err := config.validate()
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("validate() = %v, want ErrInvalidConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validate() = %v, want nil", err)
			}
		})
	}
}

func longSlotName() string {
	name := make([]byte, 64)
	for i := range name {
		name[i] = 'a'
	}
	return string(name)
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()

	got := validConfig().withDefaults()

	if got.StandbyInterval != DefaultStandbyInterval {
		t.Errorf("StandbyInterval = %v, want %v", got.StandbyInterval, DefaultStandbyInterval)
	}
	if got.Buffer != DefaultBuffer {
		t.Errorf("Buffer = %d, want %d", got.Buffer, DefaultBuffer)
	}

	// The standby interval must stay well under the server's wal_sender_timeout,
	// which defaults to 60s, or the server drops the connection as unresponsive.
	if got.StandbyInterval >= 30*time.Second {
		t.Errorf("StandbyInterval = %v, too close to the default wal_sender_timeout", got.StandbyInterval)
	}
}

func TestNewNormalisesTables(t *testing.T) {
	t.Parallel()

	config := validConfig()
	config.Tables = []string{"users", "users", "", "orders"}

	subscriber, err := New(config)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	got := subscriber.GetTables()
	if len(got) != 2 || got[0] != "users" || got[1] != "orders" {
		t.Fatalf("GetTables() = %v, want [users orders]", got)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	if _, err := New(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() = %v, want ErrInvalidConfig", err)
	}
}

// Every method must refuse to run once the subscriber is closed, rather than
// dialling a fresh connection behind the caller's back.
func TestClosedSubscriberRefusesWork(t *testing.T) {
	t.Parallel()

	subscriber, err := New(validConfig())
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	if err := subscriber.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	// Close is idempotent.
	if err := subscriber.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}

	ctx := t.Context()
	checks := map[string]error{
		"Subscribe":         firstErr(subscriber.Subscribe(ctx)),
		"AddTables":         subscriber.AddTables(ctx, "x"),
		"RemoveTables":      subscriber.RemoveTables(ctx, "public.users"),
		"SyncTables":        subscriber.SyncTables(ctx, "x"),
		"CreateSlot":        subscriber.CreateSlot(ctx, "gpool_slot"),
		"DropSlot":          subscriber.DropSlot(ctx, "gpool_slot"),
		"CreatePublication": subscriber.CreatePublication(ctx, "pub", "x"),
		"DropPublication":   subscriber.DropPublication(ctx, "pub"),
	}

	for name, err := range checks {
		if !errors.Is(err, ErrClosed) {
			t.Errorf("%s() = %v, want ErrClosed", name, err)
		}
	}

	if _, err := subscriber.VerifyTable(ctx, "public.users"); !errors.Is(err, ErrClosed) {
		t.Errorf("VerifyTable() = %v, want ErrClosed", err)
	}
}

func firstErr[T any](_ T, err error) error {
	return err
}

// A no-op call must not reach the database at all, which is what makes these safe
// to call on a closed or unreachable subscriber.
func TestTableOperationsShortCircuit(t *testing.T) {
	t.Parallel()

	subscriber, err := New(validConfig())
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(func() { _ = subscriber.Close() })

	ctx := t.Context()

	// "public.users" is already tracked, so there is nothing to add.
	if err := subscriber.AddTables(ctx, "public.users"); err != nil {
		t.Errorf("AddTables() on an already-tracked table = %v, want nil", err)
	}
	// "ghost" is not tracked, so there is nothing to remove.
	if err := subscriber.RemoveTables(ctx, "ghost"); err != nil {
		t.Errorf("RemoveTables() on an untracked table = %v, want nil", err)
	}
	if err := subscriber.AddTables(ctx); err != nil {
		t.Errorf("AddTables() with no arguments = %v, want nil", err)
	}
	if err := subscriber.SyncTables(ctx); !errors.Is(err, ErrNoTables) {
		t.Errorf("SyncTables() with no arguments = %v, want ErrNoTables", err)
	}
	if err := subscriber.CreatePublication(ctx, "pub"); !errors.Is(err, ErrNoTables) {
		t.Errorf("CreatePublication() with no tables = %v, want ErrNoTables", err)
	}
}

func TestIsTrackingAndGetTablesAreIsolated(t *testing.T) {
	t.Parallel()

	subscriber, err := New(validConfig())
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(func() { _ = subscriber.Close() })

	if !subscriber.IsTracking("public.users") {
		t.Error("IsTracking(public.users) = false, want true")
	}
	if subscriber.IsTracking("ghost") {
		t.Error("IsTracking(ghost) = true, want false")
	}

	// The returned slice is a copy; mutating it must not corrupt the subscriber.
	tables := subscriber.GetTables()
	tables[0] = "tampered"
	if !subscriber.IsTracking("public.users") {
		t.Error("mutating the result of GetTables() changed the tracking list")
	}
}
