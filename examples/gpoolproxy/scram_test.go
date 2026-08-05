// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// scramClient is the client half of SCRAM-SHA-256, written out so the server
// half is tested against an independent implementation rather than against
// itself. A round trip through the same code proves only that it is
// self-consistent.
type scramClient struct {
	password  string
	gs2Header string
	nonce     string

	firstBare   string
	serverFirst string
}

func (c *scramClient) first() []byte {
	c.firstBare = "n=,r=" + c.nonce
	return []byte(c.gs2Header + c.firstBare)
}

func (c *scramClient) final(serverFirst []byte) ([]byte, error) {
	c.serverFirst = string(serverFirst)

	salt, iterations, err := parseServerFirst(c.serverFirst)
	if err != nil {
		return nil, err
	}
	combined, err := field(c.serverFirst, "r=")
	if err != nil {
		return nil, err
	}

	salted, err := pbkdf2.Key(sha256.New, c.password, salt, iterations, sha256.Size)
	if err != nil {
		return nil, err
	}
	clientKey := sign(salted, "Client Key")
	stored := sha256.Sum256(clientKey)

	withoutProof := "c=" + base64.StdEncoding.EncodeToString([]byte(c.gs2Header)) + ",r=" + combined
	authMessage := c.firstBare + "," + c.serverFirst + "," + withoutProof

	signature := sign(stored[:], authMessage)
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ signature[i]
	}
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)), nil
}

// verifyServer checks the server proved it holds the verifier, which is the half
// of SCRAM that stops anything listening on a port from impersonating the pool.
func (c *scramClient) verifyServer(final []byte, password string) error {
	salt, iterations, err := parseServerFirst(c.serverFirst)
	if err != nil {
		return err
	}
	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return err
	}

	combined, _ := field(c.serverFirst, "r=")
	withoutProof := "c=" + base64.StdEncoding.EncodeToString([]byte(c.gs2Header)) + ",r=" + combined
	authMessage := c.firstBare + "," + c.serverFirst + "," + withoutProof

	want := "v=" + base64.StdEncoding.EncodeToString(sign(sign(salted, "Server Key"), authMessage))
	if string(final) != want {
		return errors.New("server signature does not verify")
	}
	return nil
}

func parseServerFirst(message string) ([]byte, int, error) {
	saltText, err := field(message, "s=")
	if err != nil {
		return nil, 0, err
	}
	iterText, err := field(message, "i=")
	if err != nil {
		return nil, 0, err
	}
	salt, err := base64.StdEncoding.DecodeString(saltText)
	if err != nil {
		return nil, 0, err
	}
	iterations, err := strconv.Atoi(iterText)
	return salt, iterations, err
}

func exchange(t *testing.T, server *scramServer, client *scramClient) error {
	t.Helper()

	challenge, err := server.challenge(client.first())
	if err != nil {
		return err
	}
	final, err := client.final(challenge)
	if err != nil {
		return err
	}
	signature, err := server.verify(final)
	if err != nil {
		return err
	}
	return client.verifyServer(signature, client.password)
}

func TestSCRAMAcceptsTheRightPassword(t *testing.T) {
	const password = "correct horse battery staple"

	credential, err := deriveVerifier(password, defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}

	for _, header := range []string{"n,,", "y,,"} {
		t.Run(fmt.Sprintf("gs2=%q", header), func(t *testing.T) {
			err := exchange(t,
				&scramServer{verifier: credential},
				&scramClient{password: password, gs2Header: header, nonce: "clientnonce123"})
			if err != nil {
				t.Fatalf("authentication should succeed: %v", err)
			}
		})
	}
}

func TestSCRAMRejectsTheWrongPassword(t *testing.T) {
	credential, err := deriveVerifier("right", defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}

	err = exchange(t,
		&scramServer{verifier: credential},
		&scramClient{password: "wrong", gs2Header: "n,,", nonce: "clientnonce123"})
	if !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed", err)
	}
}

// A verifier holds SHA-256(ClientKey) and never the password, so a proxy host
// that is compromised does not yield a credential usable anywhere else. This
// test is what keeps that property from being quietly lost.
func TestVerifierDoesNotContainThePassword(t *testing.T) {
	const password = "hunter2"

	credential, err := deriveVerifier(password, defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}
	if strings.Contains(credential.String(), password) {
		t.Fatal("the stored verifier contains the plaintext password")
	}

	// Derived directly, the storedKey is SHA-256 of the HMAC of the salted
	// password — one-way at every step.
	salted, err := pbkdf2.Key(sha256.New, password, credential.salt, credential.iterations, sha256.Size)
	if err != nil {
		t.Fatalf("pbkdf2 = %v", err)
	}
	want := sha256.Sum256(sign(salted, "Client Key"))
	if !hmac.Equal(want[:], credential.storedKey) {
		t.Error("storedKey does not match the documented derivation")
	}
}

// The client repeats its GS2 header inside the final message. Not comparing the
// two is how a downgrade attack strips channel binding unnoticed.
func TestSCRAMRejectsAlteredChannelBinding(t *testing.T) {
	credential, err := deriveVerifier("pw", defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}

	server := &scramServer{verifier: credential}
	client := &scramClient{password: "pw", gs2Header: "n,,", nonce: "abc"}

	challenge, err := server.challenge(client.first())
	if err != nil {
		t.Fatalf("challenge() = %v", err)
	}
	final, err := client.final(challenge)
	if err != nil {
		t.Fatalf("final() = %v", err)
	}

	// Rewrite c= to claim a header the client never sent.
	altered := strings.Replace(string(final),
		"c="+base64.StdEncoding.EncodeToString([]byte("n,,")),
		"c="+base64.StdEncoding.EncodeToString([]byte("y,,")), 1)

	if _, err := server.verify([]byte(altered)); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed", err)
	}
}

// Channel binding the server never offered must not be accepted, or a client
// that asked for it would believe it got it.
func TestSCRAMRejectsRequiredChannelBinding(t *testing.T) {
	credential, err := deriveVerifier("pw", defaultSCRAMIterations)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}

	server := &scramServer{verifier: credential}
	if _, err := server.challenge([]byte("p=tls-server-end-point,,n=,r=abc")); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed", err)
	}
}

// A verifier copied straight out of pg_authid.rolpassword must parse, because
// that is the migration path from an existing PostgreSQL role.
func TestVerifierRoundTripsPostgresFormat(t *testing.T) {
	credential, err := deriveVerifier("pw", 4096)
	if err != nil {
		t.Fatalf("deriveVerifier() = %v", err)
	}

	parsed, err := parseVerifier(credential.String())
	if err != nil {
		t.Fatalf("parseVerifier() = %v", err)
	}
	if parsed.String() != credential.String() {
		t.Errorf("round trip changed the verifier:\n got %s\nwant %s", parsed.String(), credential.String())
	}
	if parsed.iterations != 4096 {
		t.Errorf("iterations = %d, want 4096", parsed.iterations)
	}
}

func TestVerifierRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{"empty", ""},
		{"wrong scheme", "MD5$4096:aaaa$bbbb:cccc"},
		{"no key section", "SCRAM-SHA-256$4096:aaaa"},
		{"no salt", "SCRAM-SHA-256$4096$bbbb:cccc"},
		{"no server key", "SCRAM-SHA-256$4096:aaaa$bbbb"},
		{"zero iterations", "SCRAM-SHA-256$0:YWJj$YWJj:YWJj"},
		{"bad base64", "SCRAM-SHA-256$4096:!!!!$YWJj:YWJj"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseVerifier(test.text); err == nil {
				t.Errorf("parseVerifier(%q) succeeded, want an error", test.text)
			}
		})
	}
}
