// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
)

// takeOver turns an established connection into a bare non-blocking descriptor
// for an event loop, and closes the connection it was given.
//
// A plain TCP or Unix socket is duplicated and the original closed, which takes
// the socket out of the Go runtime's own poller. Left there, every packet would
// wake the runtime as well as the loop, for a connection no goroutine is reading.
//
// Anything else — a TLS connection, in practice — cannot be served as a
// descriptor, because what is readable on the socket is ciphertext. It is
// bridged instead.
func takeOver(conn net.Conn) (int, error) {
	raw, ok := conn.(syscall.Conn)
	if !ok || !isPlainSocket(conn) {
		return bridge(conn)
	}

	control, err := raw.SyscallConn()
	if err != nil {
		return -1, err
	}

	fd, dupErr := -1, error(nil)
	if err := control.Control(func(original uintptr) {
		fd, dupErr = syscall.Dup(int(original))
	}); err != nil {
		return -1, err
	}
	if dupErr != nil {
		return -1, dupErr
	}
	syscall.CloseOnExec(fd)

	// Closing the original does not shut the socket down: the duplicate keeps it
	// open, and only an explicit shutdown would be visible to the peer.
	_ = conn.Close()

	if err := syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

func isPlainSocket(conn net.Conn) bool {
	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		return true
	}
	return false
}

// bridge gives a connection that cannot be polled directly a descriptor that
// can: one end of a socket pair, with two goroutines copying between the other
// end and the real connection.
//
// This is the old cost — two goroutines and their buffers — paid only by the
// connections that need it. TLS is terminated in user space, so there is no
// descriptor on which its plaintext is ever readable.
func bridge(conn net.Conn) (int, error) {
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	syscall.CloseOnExec(pair[0])
	syscall.CloseOnExec(pair[1])

	file := os.NewFile(uintptr(pair[1]), "gpoolproxy-bridge")
	inner, err := net.FileConn(file)
	_ = file.Close()
	if err != nil {
		_ = syscall.Close(pair[0])
		return -1, err
	}
	if err := syscall.SetNonblock(pair[0], true); err != nil {
		_ = syscall.Close(pair[0])
		_ = inner.Close()
		return -1, err
	}

	// Each goroutine ends when its source does, and closes both ends on the way
	// out so the other one follows. Closing the loop's descriptor is therefore
	// all it takes to tear the bridge down.
	shuttle := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		_ = dst.Close()
		_ = src.Close()
	}
	go shuttle(inner, conn)
	go shuttle(conn, inner)

	return pair[0], nil
}

// errTooManyDescriptors reports a descriptor beyond the table sized from the
// process's limit at startup, which only happens if the limit was raised while
// the proxy was running.
var errTooManyDescriptors = errors.New("gpoolproxy: descriptor is beyond the process limit the proxy started with")

// descriptorLimit reports how many descriptors the process may hold, which is
// how large the socket table has to be.
func descriptorLimit() int {
	const fallback, ceiling = 1 << 16, 1 << 20

	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return fallback
	}
	return int(min(uint64(limit.Cur), ceiling))
}
