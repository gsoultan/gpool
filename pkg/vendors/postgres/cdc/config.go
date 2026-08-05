// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"fmt"
	"regexp"
	"time"
)

// Defaults applied by Config.withDefaults for any field left at its zero value.
const (
	// DefaultStandbyInterval is how often the stream confirms its position to the
	// server. It must stay well under the server's wal_sender_timeout (60s by
	// default) or the server will drop the connection as unresponsive.
	DefaultStandbyInterval = 10 * time.Second

	// DefaultBuffer is how many decoded events the stream may read ahead of the
	// consumer. Read-ahead smooths bursts; it does not let a stalled consumer off
	// the hook, because unconfirmed events still pin WAL on the primary.
	DefaultBuffer = 256
)

// slotNamePattern matches what PostgreSQL accepts for a replication slot name.
// Slot names are interpolated into the replication command unquoted, so this also
// closes the injection path that a quoted identifier would otherwise cover.
var slotNamePattern = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

// Config holds the configuration for the PostgreSQL CDC subscriber.
type Config struct {
	// ConnString is the PostgreSQL connection string. Required. The subscriber adds
	// the replication parameter itself where it is needed, so do not set it here.
	ConnString string

	// SlotName is the logical replication slot to stream from. Required.
	// PostgreSQL restricts slot names to lowercase letters, digits, and underscores.
	SlotName string

	// PublicationName is the publication to subscribe to. Required.
	PublicationName string

	// Tables is the set of tables the publication should cover. Names may be
	// schema-qualified ("public.users"). Required when CreatePublication is set.
	Tables []string

	// CreateSlot creates the replication slot during Subscribe if it is missing.
	//
	// The slot is what retains WAL for this consumer while it is disconnected. It is
	// never dropped automatically: an abandoned slot pins WAL forever, so removing
	// one is left as a deliberate DropSlot call.
	CreateSlot bool

	// CreatePublication creates the publication during Subscribe if it is missing,
	// and reconciles an existing one to match Tables.
	CreatePublication bool

	// StartLSN overrides the position to stream from. Zero, the default, resumes
	// from the slot's confirmed_flush_lsn, which is almost always what you want:
	// it replays everything the slot retained while the consumer was away.
	StartLSN uint64

	// StandbyInterval is how often the stream confirms its position.
	// Defaults to DefaultStandbyInterval.
	StandbyInterval time.Duration

	// Buffer is the stream's read-ahead depth in events. Defaults to DefaultBuffer.
	Buffer int
}

// withDefaults returns a copy of the config with unset fields populated.
func (c Config) withDefaults() Config {
	if c.StandbyInterval <= 0 {
		c.StandbyInterval = DefaultStandbyInterval
	}
	if c.Buffer <= 0 {
		c.Buffer = DefaultBuffer
	}
	return c
}

// validate reports why the config cannot be used, if it cannot.
func (c Config) validate() error {
	if c.ConnString == "" {
		return fmt.Errorf("%w: ConnString is required", ErrInvalidConfig)
	}
	if c.SlotName == "" {
		return fmt.Errorf("%w: SlotName is required", ErrInvalidConfig)
	}
	if !slotNamePattern.MatchString(c.SlotName) {
		return fmt.Errorf("%w: SlotName %q must be 1-63 characters of [a-z0-9_]", ErrInvalidConfig, c.SlotName)
	}
	if c.PublicationName == "" {
		return fmt.Errorf("%w: PublicationName is required", ErrInvalidConfig)
	}
	if c.CreatePublication && len(c.Tables) == 0 {
		return fmt.Errorf("%w: Tables is required when CreatePublication is set", ErrInvalidConfig)
	}
	return nil
}
