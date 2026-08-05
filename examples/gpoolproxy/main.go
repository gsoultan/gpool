// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

// Command gpoolproxy is a PostgreSQL connection pooler built on gpool.
//
// gpool is a library, and an in-process pool cannot bound connections across
// separate applications: forty services holding twenty-five connections each
// still open a thousand. That is the one thing a sidecar pooler does which a
// library cannot, and this program is what it looks like to close the gap —
// the same pooling.Core every gpool vendor runs on, with a PostgreSQL wire
// protocol front end on top.
//
// It is an example, not a product. It implements transaction-mode pooling and
// SCRAM-SHA-256, and deliberately does not implement online reconfiguration, an
// admin console, or session-mode pooling.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gsoultan/gpool/pkg/pooling"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gpoolproxy:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "hash" {
		return hash()
	}

	config, err := parseFlags(args)
	if err != nil {
		return err
	}

	proxy, err := NewProxy(config)
	if err != nil {
		return err
	}
	if err := proxy.Listen(); err != nil {
		return err
	}

	fmt.Printf("gpoolproxy listening on %s, pooling up to %d server connections\n",
		proxy.Addr(), config.Pool.MaxConns)

	stopping := make(chan os.Signal, 1)
	signal.Notify(stopping, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopping
		fmt.Println("gpoolproxy: shutting down")
		proxy.Close()
	}()

	return proxy.Serve()
}

func parseFlags(args []string) (Config, error) {
	flags := flag.NewFlagSet("gpoolproxy", flag.ContinueOnError)

	var config Config
	var maxConns, minConns int
	var lifetime, idle time.Duration

	flags.StringVar(&config.Listen, "listen", defaultListen, "address to accept clients on")
	flags.StringVar(&config.Userlist, "userlist", "", "path to the client credentials file")
	flags.IntVar(&maxConns, "max-conns", 0, "server connections to pool (default: max(4, GOMAXPROCS))")
	flags.IntVar(&minConns, "min-conns", 0, "server connections to keep warm")
	flags.IntVar(&config.MaxClients, "max-clients", defaultMaxClients, "concurrent client sessions to accept")
	flags.DurationVar(&lifetime, "max-conn-lifetime", 0, "retire a server connection after this long")
	flags.DurationVar(&idle, "max-conn-idle", 0, "close a server connection idle this long")
	flags.StringVar(&config.TLSCert, "tls-cert", "", "certificate for TLS on the client side")
	flags.StringVar(&config.TLSKey, "tls-key", "", "private key for TLS on the client side")

	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), `gpoolproxy — a PostgreSQL pooler built on gpool

usage:
  gpoolproxy [flags]          run the proxy
  gpoolproxy hash             read a password on stdin, print a SCRAM verifier

The upstream connection string is read from GPOOLPROXY_UPSTREAM rather than a
flag, because a command line is visible to every process on the host.

flags:
`)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return Config{}, err
	}

	config.Upstream = os.Getenv("GPOOLPROXY_UPSTREAM")
	if config.Upstream == "" {
		return Config{}, errors.New("set GPOOLPROXY_UPSTREAM to the PostgreSQL connection string to pool")
	}
	config.Pool = pooling.Config{
		MaxConns:        int32(maxConns),
		MinConns:        int32(minConns),
		MaxConnLifetime: lifetime,
		MaxConnIdleTime: idle,
	}
	return config, nil
}

// hash turns a password into a verifier for the userlist file.
//
// The password is read from stdin rather than taken as an argument, so it does
// not appear in the process table or the shell history.
func hash() error {
	fmt.Fprint(os.Stderr, "password: ")

	reader := bufio.NewReader(os.Stdin)
	password, err := reader.ReadString('\n')
	if err != nil && password == "" {
		return err
	}
	password = strings.TrimRight(password, "\r\n")
	if password == "" {
		return errors.New("empty password")
	}

	credential, err := deriveVerifier(password, defaultSCRAMIterations)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr)
	fmt.Println(credential)
	return nil
}
