// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"
)

// ParseConfig resolves a connection string, in URL form
// (postgres://user:password@host:port/database?sslmode=require) or keyword/value
// form (host=db user=app sslmode=require), the way libpq does for a single host.
//
// What the string leaves out is taken from the PG* environment variables libpq
// reads — PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, PGSSLMODE,
// PGSSLROOTCERT, PGSSLCERT, PGSSLKEY, PGAPPNAME, PGCONNECT_TIMEOUT, PGPASSFILE —
// and a password from the password file. Everything is read here, once; a
// Config never goes back to the environment.
//
// Not supported, and refused rather than ignored: several hosts in one string,
// and service files. Settings that only pgx understood (statement_cache_capacity,
// default_query_exec_mode, pool_*) are dropped, so a string written for pgx
// still connects.
func ParseConfig(connString string) (*Config, error) {
	settings, err := parseSettings(connString)
	if err != nil {
		return nil, err
	}
	addEnvironment(settings)
	return buildConfig(settings)
}

// connectionKeys describe the connection rather than the session, so they are
// not sent to the server.
var connectionKeys = map[string]bool{
	"host": true, "port": true, "user": true, "password": true, "dbname": true,
	"sslmode": true, "sslrootcert": true, "sslcert": true, "sslkey": true,
	"sslsni": true, "sslpassword": true, "connect_timeout": true, "passfile": true,
	"target_session_attrs": true, "hostaddr": true,
}

// clientOnlyKeys are settings pgx read from a connection string. Passing them on
// would make the server refuse the connection as an unrecognised parameter.
var clientOnlyKeys = map[string]bool{
	"statement_cache_capacity": true, "description_cache_capacity": true,
	"default_query_exec_mode": true, "min_read_buffer_size": true,
	"pool_max_conns": true, "pool_min_conns": true, "pool_max_conn_lifetime": true,
	"pool_max_conn_idle_time": true, "pool_health_check_period": true,
	"pool_max_conn_lifetime_jitter": true, "pool_min_idle_conns": true,
}

var environment = [][2]string{
	{"PGHOST", "host"}, {"PGPORT", "port"}, {"PGDATABASE", "dbname"},
	{"PGUSER", "user"}, {"PGPASSWORD", "password"}, {"PGSSLMODE", "sslmode"},
	{"PGSSLROOTCERT", "sslrootcert"}, {"PGSSLCERT", "sslcert"},
	{"PGSSLKEY", "sslkey"}, {"PGAPPNAME", "application_name"},
	{"PGCONNECT_TIMEOUT", "connect_timeout"}, {"PGPASSFILE", "passfile"},
	{"PGTARGETSESSIONATTRS", "target_session_attrs"},
}

func parseSettings(text string) (map[string]string, error) {
	if strings.HasPrefix(text, "postgres://") || strings.HasPrefix(text, "postgresql://") {
		return parseURL(text)
	}
	return parseKeywords(text)
}

func parseURL(text string) (map[string]string, error) {
	parsed, err := url.Parse(text)
	if err != nil {
		return nil, ErrInvalidConnString
	}
	settings := map[string]string{}
	if parsed.User != nil {
		settings["user"] = parsed.User.Username()
		if password, ok := parsed.User.Password(); ok {
			settings["password"] = password
		}
	}
	if host := parsed.Hostname(); host != "" {
		settings["host"] = host
	}
	if port := parsed.Port(); port != "" {
		settings["port"] = port
	}
	if database := strings.TrimPrefix(parsed.Path, "/"); database != "" {
		settings["dbname"] = database
	}
	for key, values := range parsed.Query() {
		settings[key] = values[len(values)-1]
	}
	if strings.Contains(parsed.Host, ",") {
		return nil, fmt.Errorf("%w: more than one host is not supported", ErrInvalidConnString)
	}
	return settings, nil
}

// parseKeywords reads libpq's keyword/value form: key=value pairs separated by
// whitespace, a value optionally single-quoted, with backslash escaping a quote
// or a backslash.
func parseKeywords(text string) (map[string]string, error) {
	settings := map[string]string{}
	for text = strings.TrimSpace(text); text != ""; text = strings.TrimSpace(text) {
		equals := strings.IndexByte(text, '=')
		if equals <= 0 {
			return nil, ErrInvalidConnString
		}
		key := strings.TrimSpace(text[:equals])
		value, rest, err := keywordValue(strings.TrimLeft(text[equals+1:], " \t\n\r"))
		if err != nil {
			return nil, err
		}
		settings[key] = value
		text = rest
	}
	return settings, nil
}

func keywordValue(text string) (string, string, error) {
	var value strings.Builder
	quoted := strings.HasPrefix(text, "'")
	if quoted {
		text = text[1:]
	}
	for i := 0; i < len(text); i++ {
		switch c := text[i]; {
		case c == '\\' && i+1 < len(text):
			i++
			value.WriteByte(text[i])
		case quoted && c == '\'':
			return value.String(), text[i+1:], nil
		case !quoted && (c == ' ' || c == '\t' || c == '\n' || c == '\r'):
			return value.String(), text[i:], nil
		default:
			value.WriteByte(c)
		}
	}
	if quoted {
		return "", "", fmt.Errorf("%w: unterminated quoted value", ErrInvalidConnString)
	}
	return value.String(), "", nil
}

func addEnvironment(settings map[string]string) {
	for _, pair := range environment {
		if _, set := settings[pair[1]]; set {
			continue
		}
		if value, ok := os.LookupEnv(pair[0]); ok {
			settings[pair[1]] = value
		}
	}
}

func buildConfig(settings map[string]string) (*Config, error) {
	config := &Config{RuntimeParams: map[string]string{}}
	if err := config.address(settings); err != nil {
		return nil, err
	}
	if err := config.identity(settings); err != nil {
		return nil, err
	}
	if err := config.timeout(settings["connect_timeout"]); err != nil {
		return nil, err
	}
	if attrs := settings["target_session_attrs"]; attrs != "" && attrs != "any" {
		return nil, fmt.Errorf("%w: target_session_attrs=%s is not supported", ErrInvalidConnString, attrs)
	}
	for key, value := range settings {
		if !connectionKeys[key] && !clientOnlyKeys[key] {
			config.RuntimeParams[key] = value
		}
	}
	if err := config.encryption(settings); err != nil {
		return nil, err
	}
	return config, nil
}

func (c *Config) address(settings map[string]string) error {
	c.Host = settings["host"]
	if strings.Contains(c.Host, ",") {
		return fmt.Errorf("%w: more than one host is not supported", ErrInvalidConnString)
	}
	port := settings["port"]
	if port == "" {
		port = "5432"
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return fmt.Errorf("%w: port is not a port number", ErrInvalidConnString)
	}
	c.Port = uint16(number)
	if c.Host == "" {
		c.Host = defaultHost(c.Port)
	}
	return nil
}

// defaultHost is a local socket when the server is listening on one, as libpq
// prefers, and localhost otherwise.
func defaultHost(port uint16) string {
	for _, dir := range []string{"/var/run/postgresql", "/private/tmp", "/tmp"} {
		if _, err := os.Stat(dir + "/.s.PGSQL." + strconv.Itoa(int(port))); err == nil {
			return dir
		}
	}
	return "localhost"
}

func (c *Config) identity(settings map[string]string) error {
	c.User = settings["user"]
	if c.User == "" {
		current, err := user.Current()
		if err != nil {
			return fmt.Errorf("%w: no user given and the current one is unknown", ErrInvalidConnString)
		}
		c.User = current.Username
	}
	c.Database = settings["dbname"]
	if c.Database == "" {
		c.Database = c.User
	}
	c.Password = settings["password"]
	if _, given := settings["password"]; !given {
		c.Password = passwordFromFile(settings["passfile"], c)
	}
	return nil
}

func (c *Config) timeout(text string) error {
	if text == "" {
		return nil
	}
	seconds, err := strconv.Atoi(text)
	if err != nil || seconds < 0 {
		return fmt.Errorf("%w: connect_timeout is not a number of seconds", ErrInvalidConnString)
	}
	c.ConnectTimeout = time.Duration(seconds) * time.Second
	return nil
}

// encryption turns sslmode and its companions into a TLS configuration, with
// libpq's meanings: prefer and require encrypt without authenticating the
// server, verify-ca checks its certificate chain, verify-full also checks its
// name. require with a root certificate given verifies the chain, as libpq does.
func (c *Config) encryption(settings map[string]string) error {
	mode := settings["sslmode"]
	if mode == "" {
		mode = "prefer"
	}
	if mode == "disable" || mode == "allow" || c.unix() {
		return validMode(mode)
	}

	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.Host}
	if settings["sslsni"] == "0" {
		config.ServerName = ""
	}
	rootFile := settings["sslrootcert"]

	switch mode {
	case "prefer":
		config.InsecureSkipVerify = true
		c.TLSFallback = true
	case "require":
		config.InsecureSkipVerify = rootFile == ""
	case "verify-ca", "verify-full":
	default:
		return validMode(mode)
	}

	if err := loadRoots(config, rootFile); err != nil {
		return err
	}
	if !config.InsecureSkipVerify && mode != "verify-full" {
		verifyChainOnly(config)
	}
	if err := loadClientCertificate(config, settings["sslcert"], settings["sslkey"]); err != nil {
		return err
	}
	c.TLSConfig = config
	return nil
}

func validMode(mode string) error {
	switch mode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return nil
	}
	return fmt.Errorf("%w: sslmode %q is not one of disable, allow, prefer, require, verify-ca, verify-full", ErrInvalidConnString, mode)
}

func loadRoots(config *tls.Config, rootFile string) error {
	if rootFile == "" || rootFile == "system" {
		return nil
	}
	pem, err := os.ReadFile(rootFile)
	if err != nil {
		return fmt.Errorf("%w: reading sslrootcert: %w", ErrInvalidConnString, err)
	}
	config.RootCAs = x509.NewCertPool()
	if !config.RootCAs.AppendCertsFromPEM(pem) {
		return fmt.Errorf("%w: sslrootcert holds no certificate", ErrInvalidConnString)
	}
	return nil
}

func loadClientCertificate(config *tls.Config, certFile, keyFile string) error {
	if (certFile == "") != (keyFile == "") {
		return fmt.Errorf("%w: sslcert and sslkey must be given together", ErrInvalidConnString)
	}
	if certFile == "" {
		return nil
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("%w: loading sslcert: %w", ErrInvalidConnString, err)
	}
	config.Certificates = []tls.Certificate{pair}
	return nil
}

// verifyChainOnly makes a configuration check that the server's certificate
// descends from a trusted root without checking the name on it: libpq's
// verify-ca. crypto/tls has no such mode, so the check is done by hand.
func verifyChainOnly(config *tls.Config) {
	roots := config.RootCAs
	config.InsecureSkipVerify = true
	config.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 {
			return errors.New("gpool/postgres/wire: server sent no certificate")
		}
		certificates := make([]*x509.Certificate, len(raw))
		for i, der := range raw {
			certificate, err := x509.ParseCertificate(der)
			if err != nil {
				return err
			}
			certificates[i] = certificate
		}
		intermediates := x509.NewCertPool()
		for _, certificate := range certificates[1:] {
			intermediates.AddCert(certificate)
		}
		_, err := certificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates})
		return err
	}
}
