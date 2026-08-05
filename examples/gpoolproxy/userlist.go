// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// userlist maps a client username to the secret that authenticates it.
//
// It is read once at startup and never mutated, so lookups need no lock.
type userlist map[string]verifier

// lookup reports the verifier for a user.
//
// A miss still costs a SCRAM exchange against a throwaway verifier rather than an
// immediate rejection — see session.authenticate. Returning early here is what
// turns a proxy into a username oracle.
func (u userlist) lookup(name string) (verifier, bool) {
	v, ok := u[name]
	return v, ok
}

// loadUserlist reads a file of "user:secret" lines. The secret is either a
// PostgreSQL SCRAM verifier, which is what production should use, or a plaintext
// password, which is converted to a verifier on load and never kept.
//
//	# comments and blank lines are ignored
//	app:SCRAM-SHA-256$4096:zW1B…$Vk9x…:Qm4t…
//	dev:hunter2
func loadUserlist(path string) (userlist, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if err := checkSecretPermissions(file, path); err != nil {
		return nil, err
	}

	users := make(userlist)
	scanner := bufio.NewScanner(file)

	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}

		name, secret, ok := strings.Cut(text, ":")
		if !ok || name == "" || secret == "" {
			return nil, fmt.Errorf("%s:%d: expected user:secret", path, line)
		}
		if users[name], err = parseSecret(secret); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("%s: no users defined", path)
	}
	return users, nil
}

// parseSecret accepts either a stored verifier or a plaintext password.
func parseSecret(secret string) (verifier, error) {
	if strings.HasPrefix(secret, verifierPrefix) {
		return parseVerifier(secret)
	}
	return deriveVerifier(secret, defaultSCRAMIterations)
}

// checkSecretPermissions refuses a credentials file that anyone on the host can
// read. Failing to start is the right outcome: the alternative is a proxy that
// runs happily while its userlist is world-readable.
func checkSecretPermissions(file *os.File, path string) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if mode := info.Mode().Perm(); mode&fs.FileMode(0o077) != 0 {
		return fmt.Errorf("%s is mode %#o and readable beyond its owner; run: chmod 600 %s", path, mode, path)
	}
	return nil
}
