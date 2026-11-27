// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// errUpstreamURL is deliberately the whole of what a malformed connection
// string produces. The parser's own error quotes its input, and the input is
// where the password is.
var errUpstreamURL = errors.New("gpoolproxy: Upstream must be a postgres:// URL")

// upstream is the PostgreSQL server the proxy logs in to, as parsed once from
// its connection string.
//
// Only the URL form is read, and only what is in it: there is no ~/.pgpass, no
// service file, no PG* environment and no list of hosts. Those are conveniences
// of a client library, and each is a place a credential can come from that the
// operator of a proxy did not write down.
type upstream struct {
	network string
	address string
	host    string

	user     string
	password string
	database string

	// settings are sent to the server in the startup packet, in a fixed order so
	// that two connections never differ by map iteration.
	settings [][2]string

	// tls is nil when the connection is not to be encrypted, and required says
	// whether a server that declines encryption is an error or a fallback.
	tls      *tls.Config
	required bool

	timeout time.Duration
}

// connectionKeys are the query parameters that describe the connection rather
// than the session. Everything else in the query is a server setting.
var connectionKeys = map[string]bool{
	"host": true, "port": true, "user": true, "password": true, "dbname": true,
	"sslmode": true, "sslrootcert": true, "sslcert": true, "sslkey": true,
	"connect_timeout": true,
}

func parseUpstream(text string) (*upstream, error) {
	parsed, err := url.Parse(text)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return nil, errUpstreamURL
	}
	query := parsed.Query()

	u := &upstream{
		host:     firstOf(query.Get("host"), parsed.Hostname(), "localhost"),
		user:     firstOf(query.Get("user"), parsed.User.Username()),
		database: firstOf(query.Get("dbname"), strings.TrimPrefix(parsed.Path, "/")),
	}
	if u.user == "" {
		return nil, errors.New("gpoolproxy: Upstream names no user")
	}
	if u.database == "" {
		u.database = u.user
	}
	u.password, _ = parsed.User.Password()
	if value := query.Get("password"); value != "" {
		u.password = value
	}

	port := firstOf(query.Get("port"), parsed.Port(), "5432")
	if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
		return nil, errors.New("gpoolproxy: Upstream port is not a port number")
	}
	u.network, u.address = "tcp", net.JoinHostPort(u.host, port)
	if strings.HasPrefix(u.host, "/") {
		u.network, u.address = "unix", u.host+"/.s.PGSQL."+port
	}

	if value := query.Get("connect_timeout"); value != "" {
		seconds, err := strconv.Atoi(value)
		if err != nil || seconds < 0 {
			return nil, errors.New("gpoolproxy: Upstream connect_timeout is not a number of seconds")
		}
		u.timeout = time.Duration(seconds) * time.Second
	}

	for _, name := range sortedKeys(query) {
		if !connectionKeys[name] {
			u.settings = append(u.settings, [2]string{name, query.Get(name)})
		}
	}

	if err := u.encrypt(query); err != nil {
		return nil, err
	}
	return u, nil
}

// encrypt turns sslmode and its companions into a TLS configuration, with
// libpq's meanings: prefer and require encrypt without authenticating the
// server, verify-ca checks its certificate chain, verify-full also checks the
// name. A root certificate given with require is verified, as libpq does.
func (u *upstream) encrypt(query url.Values) error {
	mode := firstOf(query.Get("sslmode"), "prefer")
	if u.network == "unix" || mode == "disable" {
		return nil
	}

	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.host}
	rootFile := query.Get("sslrootcert")

	switch {
	case mode == "prefer":
		config.InsecureSkipVerify = true
	case mode == "require" && rootFile == "":
		config.InsecureSkipVerify = true
		u.required = true
	case mode == "require", mode == "verify-ca":
		u.required = true
	case mode == "verify-full":
		u.required = true
	default:
		return fmt.Errorf("gpoolproxy: Upstream sslmode %q is not supported (disable, prefer, require, verify-ca, verify-full)", mode)
	}

	if rootFile != "" {
		pem, err := os.ReadFile(rootFile)
		if err != nil {
			return fmt.Errorf("gpoolproxy: reading sslrootcert: %w", err)
		}
		config.RootCAs = x509.NewCertPool()
		if !config.RootCAs.AppendCertsFromPEM(pem) {
			return errors.New("gpoolproxy: sslrootcert holds no certificate")
		}
	}
	if u.required && mode != "verify-full" && !config.InsecureSkipVerify {
		verifyChainOnly(config)
	}

	certFile, keyFile := query.Get("sslcert"), query.Get("sslkey")
	if (certFile == "") != (keyFile == "") {
		return errors.New("gpoolproxy: Upstream sslcert and sslkey must be given together")
	}
	if certFile != "" {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("gpoolproxy: loading sslcert: %w", err)
		}
		config.Certificates = []tls.Certificate{pair}
	}

	u.tls = config
	return nil
}

// verifyChainOnly makes a configuration check that the server's certificate
// descends from a trusted root while not checking the name on it, which is what
// verify-ca means and what crypto/tls has no switch for.
func verifyChainOnly(config *tls.Config) {
	roots := config.RootCAs
	config.InsecureSkipVerify = true
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("gpoolproxy: server presented no certificate")
		}
		options := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
		for _, certificate := range state.PeerCertificates[1:] {
			options.Intermediates.AddCert(certificate)
		}
		_, err := state.PeerCertificates[0].Verify(options)
		return err
	}
}

// local reports a connection that never leaves the host, and so keeps what
// crosses it private without TLS.
func (u *upstream) local() bool {
	return u.network == "unix"
}

func firstOf(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func sortedKeys(values url.Values) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
