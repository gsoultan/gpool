// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package wire

import "errors"

var (
	// ErrClosed is returned by any operation on a connection that has been
	// closed, by the caller or by a failure that left it unusable.
	ErrClosed = errors.New("gpool/postgres/wire: connection is closed")

	// ErrProtocol is returned when the server sends something the protocol does
	// not allow at that point. The connection is closed: once the two sides
	// disagree about where a message starts, nothing after it can be trusted.
	ErrProtocol = errors.New("gpool/postgres/wire: protocol violation")

	// ErrMessageTooLarge is returned when the server announces a message larger
	// than Config.MaxMessageSize. The limit exists so that whoever answers on
	// the server's address cannot make the client allocate without bound.
	ErrMessageTooLarge = errors.New("gpool/postgres/wire: message exceeds MaxMessageSize")

	// ErrTLSRefused is returned when TLS is required and the server declined it.
	ErrTLSRefused = errors.New("gpool/postgres/wire: server refused TLS")

	// ErrAuthMethod is returned when the server asks for an authentication method
	// this package does not implement, such as GSSAPI or SCRAM with channel
	// binding only.
	ErrAuthMethod = errors.New("gpool/postgres/wire: unsupported authentication method")

	// ErrSCRAM is returned when the server does not hold up its half of
	// SCRAM-SHA-256. It says no more than that: the detail would be about a secret.
	ErrSCRAM = errors.New("gpool/postgres/wire: server failed SCRAM authentication")

	// ErrInvalidConnString is returned by ParseConfig. It never quotes the
	// string, because the string is where the password is.
	ErrInvalidConnString = errors.New("gpool/postgres/wire: invalid connection string")
)
