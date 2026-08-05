// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"
)

// scramServer runs the server half of one SCRAM-SHA-256 exchange.
type scramServer struct {
	verifier verifier

	gs2Header       string
	clientFirstBare string
	serverFirst     string
}

// challenge consumes the client's first message and produces the server's.
//
// The gs2 header is retained rather than discarded: the client repeats it inside
// its final message, and comparing the two is what detects a man in the middle
// having stripped channel binding from the negotiation.
func (s *scramServer) challenge(clientFirst []byte) ([]byte, error) {
	text := string(clientFirst)

	gs2, bare, err := splitGS2(text)
	if err != nil {
		return nil, err
	}
	s.gs2Header, s.clientFirstBare = gs2, bare

	clientNonce, err := field(bare, "r=")
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, scramNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	s.serverFirst = fmt.Sprintf("r=%s%s,s=%s,i=%d",
		clientNonce,
		base64.StdEncoding.EncodeToString(nonce),
		base64.StdEncoding.EncodeToString(s.verifier.salt),
		s.verifier.iterations)
	return []byte(s.serverFirst), nil
}

// verify checks the client's proof and returns the server's own signature, which
// proves to the client that the peer really held the verifier. SCRAM is mutual;
// skipping this half would let anything that can accept a TCP connection
// impersonate the database.
func (s *scramServer) verify(clientFinal []byte) ([]byte, error) {
	text := string(clientFinal)

	withoutProof, proofField, ok := strings.Cut(text, ",p=")
	if !ok {
		return nil, fmt.Errorf("%w: client final message carries no proof", ErrAuthFailed)
	}
	proof, err := base64.StdEncoding.DecodeString(proofField)
	if err != nil {
		return nil, fmt.Errorf("%w: proof is not valid base64", ErrAuthFailed)
	}

	binding, err := field(withoutProof, "c=")
	if err != nil {
		return nil, err
	}
	if binding != base64.StdEncoding.EncodeToString([]byte(s.gs2Header)) {
		return nil, fmt.Errorf("%w: channel binding does not match the negotiated header", ErrAuthFailed)
	}
	if nonce, err := field(withoutProof, "r="); err != nil || !strings.HasPrefix(s.serverFirst, "r="+nonce) {
		return nil, fmt.Errorf("%w: nonce does not match the challenge", ErrAuthFailed)
	}

	authMessage := s.clientFirstBare + "," + s.serverFirst + "," + withoutProof

	// The proof is ClientKey XOR ClientSignature, and the verifier holds only
	// SHA-256(ClientKey). Recovering ClientKey from the proof and hashing it is
	// what lets the check run without the proxy storing anything replayable.
	signature := sign(s.verifier.storedKey, authMessage)
	if len(proof) != len(signature) {
		return nil, fmt.Errorf("%w: proof is the wrong length", ErrAuthFailed)
	}
	clientKey := make([]byte, len(proof))
	subtle.XORBytes(clientKey, proof, signature)

	derived := sha256.Sum256(clientKey)
	if subtle.ConstantTimeCompare(derived[:], s.verifier.storedKey) != 1 {
		return nil, ErrAuthFailed
	}

	server := sign(s.verifier.serverKey, authMessage)
	return []byte("v=" + base64.StdEncoding.EncodeToString(server)), nil
}

// splitGS2 separates the channel-binding header from the rest of the client's
// first message.
func splitGS2(text string) (header, bare string, err error) {
	switch {
	// "n" means the client does not support channel binding; "y" means it does
	// but believes the server does not. Both are fine, because this proxy
	// advertises SCRAM-SHA-256 rather than SCRAM-SHA-256-PLUS.
	case strings.HasPrefix(text, "n,"), strings.HasPrefix(text, "y,"):
	case strings.HasPrefix(text, "p="):
		return "", "", fmt.Errorf("%w: channel binding was required but is not offered", ErrAuthFailed)
	default:
		return "", "", fmt.Errorf("%w: malformed GS2 header", ErrAuthFailed)
	}

	// The header is the first two comma-separated fields, including the trailing
	// comma: "n,," or "y,,".
	rest := text[2:]
	authzEnd := strings.IndexByte(rest, ',')
	if authzEnd < 0 {
		return "", "", fmt.Errorf("%w: malformed GS2 header", ErrAuthFailed)
	}
	return text[:2+authzEnd+1], rest[authzEnd+1:], nil
}

// field extracts a comma-separated SCRAM attribute such as "r=" or "c=".
func field(message, prefix string) (string, error) {
	for part := range strings.SplitSeq(message, ",") {
		if value, ok := strings.CutPrefix(part, prefix); ok {
			return value, nil
		}
	}
	return "", fmt.Errorf("%w: message has no %q attribute", ErrAuthFailed, prefix)
}
