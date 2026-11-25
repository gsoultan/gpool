// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// maxSCRAMIterations bounds the key-stretching work a server may ask for.
// PostgreSQL's default is 4,096. The count comes from the peer before it has
// proved anything, so without a ceiling whoever answers on the server's address
// decides how long this process spends in PBKDF2.
const maxSCRAMIterations = 10_000_000

// scramNonceLen is the size of the client nonce, before base64.
const scramNonceLen = 18

// scramClient is the client half of SCRAM-SHA-256.
//
// The password is used as given. PostgreSQL passes one through SASLprep and
// falls back to the raw bytes where that fails; for a password of printable
// ASCII the two are the same, and that is the case this covers. A password
// SASLprep would rewrite, such as one with full-width characters, will not
// authenticate.
type scramClient struct {
	password string

	firstBare   string
	authMessage string
	serverKey   []byte
}

// first produces the client's opening message.
func (c *scramClient) first() ([]byte, error) {
	nonce := make([]byte, scramNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	// The username is empty on purpose: PostgreSQL takes it from the startup
	// packet and ignores this one.
	c.firstBare = "n=,r=" + base64.StdEncoding.EncodeToString(nonce)
	return []byte("n,," + c.firstBare), nil
}

// final consumes the server's challenge and produces the proof.
func (c *scramClient) final(serverFirst []byte) ([]byte, error) {
	text := string(serverFirst)

	nonce, saltText, countText, err := challengeFields(text)
	if err != nil {
		return nil, err
	}
	// A nonce that does not extend ours is a reply to somebody else's exchange.
	if !strings.HasPrefix(nonce, strings.TrimPrefix(c.firstBare, "n=,r=")) {
		return nil, ErrSCRAM
	}
	salt, err := base64.StdEncoding.DecodeString(saltText)
	if err != nil {
		return nil, ErrSCRAM
	}
	iterations, err := strconv.Atoi(countText)
	if err != nil || iterations < 1 || iterations > maxSCRAMIterations {
		return nil, ErrSCRAM
	}

	salted, err := pbkdf2.Key(sha256.New, c.password, salt, iterations, sha256.Size)
	if err != nil {
		return nil, err
	}
	clientKey := sign(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	c.serverKey = sign(salted, "Server Key")

	// "biws" is the base64 of "n,,": the channel-binding header, repeated so the
	// server can tell if it was altered on the way.
	withoutProof := "c=biws,r=" + nonce
	c.authMessage = c.firstBare + "," + text + "," + withoutProof

	proof := sign(storedKey[:], c.authMessage)
	for i := range proof {
		proof[i] ^= clientKey[i]
	}
	return fmt.Appendf(nil, "%s,p=%s", withoutProof, base64.StdEncoding.EncodeToString(proof)), nil
}

// verify checks the server's signature. SCRAM is mutual: this is the step that
// proves the peer knew the password's verifier, and skipping it would let
// anything that can accept a connection stand in for the database.
func (c *scramClient) verify(serverFinal []byte) error {
	encoded, ok := strings.CutPrefix(string(serverFinal), "v=")
	if !ok || c.serverKey == nil {
		return ErrSCRAM
	}
	signature, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return ErrSCRAM
	}
	if !hmac.Equal(signature, sign(c.serverKey, c.authMessage)) {
		return ErrSCRAM
	}
	return nil
}

// sign is HMAC-SHA-256.
func sign(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return mac.Sum(nil)
}

func challengeFields(text string) (nonce, salt, count string, err error) {
	for part := range strings.SplitSeq(text, ",") {
		switch {
		case strings.HasPrefix(part, "r="):
			nonce = part[2:]
		case strings.HasPrefix(part, "s="):
			salt = part[2:]
		case strings.HasPrefix(part, "i="):
			count = part[2:]
		}
	}
	if nonce == "" || salt == "" || count == "" {
		return "", "", "", ErrSCRAM
	}
	return nonce, salt, count, nil
}
