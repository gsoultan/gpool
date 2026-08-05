// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrAuthFailed is returned for every authentication failure, whatever the cause.
//
// One error for all causes is deliberate: distinguishing "no such user" from
// "wrong password" tells an attacker which half of a guess was right.
var ErrAuthFailed = errors.New("gpoolproxy: authentication failed")

// defaultSCRAMIterations matches what PostgreSQL 16+ uses for new passwords.
const defaultSCRAMIterations = 4096

// scramNonceLen is the server nonce length in bytes before base64.
const scramNonceLen = 18

// verifier is a SCRAM-SHA-256 secret in PostgreSQL's own storage format:
//
//	SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>
//
// Note what is absent. A verifier authenticates a client without the proxy ever
// holding the password that produced it, so a compromised proxy host does not
// hand over a credential that works anywhere else. It is also exactly what
// PostgreSQL keeps in pg_authid.rolpassword, so an operator can copy one across
// rather than typing a password into a config file.
type verifier struct {
	iterations int
	salt       []byte
	storedKey  []byte
	serverKey  []byte
}

const verifierPrefix = "SCRAM-SHA-256$"

// parseVerifier reads PostgreSQL's stored verifier format.
func parseVerifier(text string) (verifier, error) {
	rest, ok := strings.CutPrefix(text, verifierPrefix)
	if !ok {
		return verifier{}, fmt.Errorf("gpoolproxy: verifier must start with %q", verifierPrefix)
	}

	params, keys, ok := strings.Cut(rest, "$")
	if !ok {
		return verifier{}, errors.New("gpoolproxy: verifier is missing its key section")
	}
	iterText, saltText, ok := strings.Cut(params, ":")
	if !ok {
		return verifier{}, errors.New("gpoolproxy: verifier is missing its salt")
	}
	storedText, serverText, ok := strings.Cut(keys, ":")
	if !ok {
		return verifier{}, errors.New("gpoolproxy: verifier is missing its server key")
	}

	iterations, err := strconv.Atoi(iterText)
	if err != nil || iterations < 1 {
		return verifier{}, fmt.Errorf("gpoolproxy: verifier iteration count %q is not a positive integer", iterText)
	}

	decoded := make([][]byte, 3)
	for i, text := range []string{saltText, storedText, serverText} {
		if decoded[i], err = base64.StdEncoding.DecodeString(text); err != nil {
			return verifier{}, fmt.Errorf("gpoolproxy: verifier field %d is not valid base64: %w", i+1, err)
		}
	}

	return verifier{
		iterations: iterations,
		salt:       decoded[0],
		storedKey:  decoded[1],
		serverKey:  decoded[2],
	}, nil
}

// String renders the verifier back into PostgreSQL's format.
func (v verifier) String() string {
	encode := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("%s%d:%s$%s:%s",
		verifierPrefix, v.iterations, encode(v.salt), encode(v.storedKey), encode(v.serverKey))
}

// deriveVerifier turns a password into a verifier, so an operator can produce one
// without a running PostgreSQL to ask.
func deriveVerifier(password string, iterations int) (verifier, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return verifier{}, err
	}

	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return verifier{}, err
	}
	stored := sha256.Sum256(sign(salted, "Client Key"))

	return verifier{
		iterations: iterations,
		salt:       salt,
		storedKey:  stored[:],
		serverKey:  sign(salted, "Server Key"),
	}, nil
}

// sign is HMAC-SHA-256, which SCRAM uses for every derivation step.
func sign(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return mac.Sum(nil)
}
