// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/gsoultan/gpool/pkg/pooling"
)

const (
	defaultListen         = "127.0.0.1:6432"
	defaultCleanupTimeout = 5 * time.Second

	// defaultMaxClients bounds concurrent client sessions. Every accepted
	// connection costs two goroutines and two 64 KiB relay buffers, so an
	// unbounded listener is a memory exhaustion vector rather than a feature.
	defaultMaxClients = 1000
)

// Config is everything the proxy needs to run. There is no config file: flags
// are parsed in main and land here, so the proxy stays one struct a test can
// construct directly.
type Config struct {
	// Listen is the address clients connect to.
	Listen string

	// Upstream is the PostgreSQL connection string the proxy pools against. It
	// carries the proxy's own credentials, which are not the clients'.
	Upstream string

	// Userlist is the path to the file of client credentials.
	Userlist string

	// MaxClients bounds concurrent client sessions.
	MaxClients int

	// Pool is the shared engine's configuration. MaxConns here is the number of
	// real PostgreSQL backends, which is the number this whole exercise exists
	// to keep small.
	Pool pooling.Config

	// TLSCert and TLSKey enable TLS on the client side. Without them a client's
	// SSLRequest is refused and credentials cross the client link protected only
	// by SCRAM, which authenticates but does not encrypt.
	TLSCert string
	TLSKey  string
}

// WithDefaults returns a copy with unset fields populated.
func (c Config) WithDefaults() Config {
	if c.Listen == "" {
		c.Listen = defaultListen
	}
	if c.MaxClients == 0 {
		c.MaxClients = defaultMaxClients
	}
	c.Pool = c.Pool.WithDefaults()
	return c
}

// Validate reports why the config cannot be used, if it cannot.
func (c Config) Validate() error {
	if c.Upstream == "" {
		return errors.New("gpoolproxy: Upstream connection string is required")
	}
	if c.Userlist == "" {
		return errors.New("gpoolproxy: Userlist is required; the proxy will not run unauthenticated")
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return errors.New("gpoolproxy: TLSCert and TLSKey must be given together")
	}
	if c.MaxClients < 0 {
		return fmt.Errorf("gpoolproxy: MaxClients must not be negative, got %d", c.MaxClients)
	}
	return c.Pool.Validate()
}
