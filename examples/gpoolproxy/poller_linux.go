// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

//go:build linux

package main

import (
	"syscall"
)

// Event bits, spelled out as unsigned because syscall.EPOLLET is declared as a
// negative constant and will not fit the event mask it belongs in.
const (
	epollIn    uint32 = 0x1
	epollOut   uint32 = 0x4
	epollErr   uint32 = 0x8
	epollHup   uint32 = 0x10
	epollRdHup uint32 = 0x2000
	epollET    uint32 = 1 << 31
)

// pollEvent is one readiness report as the kernel writes it.
type pollEvent = syscall.EpollEvent

// poller is the epoll instance every worker waits on.
//
// Registration is edge-triggered and covers both directions from the start, so a
// descriptor is registered once and never modified. The alternative — arming
// write interest only while output is pending — costs a system call each way on
// a path that otherwise makes four per query.
//
// It is one instance shared by all workers rather than one each. The kernel
// wakes a single waiter per event, most recently parked first, so a quiet proxy
// is served by one warm thread and a busy one by all of them, and a backend that
// moves between clients never has to move between pollers.
type poller struct {
	fd int
}

func newPoller() (*poller, error) {
	fd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &poller{fd: fd}, nil
}

// add registers a descriptor for edge-triggered readiness in both directions.
func (p *poller) add(fd int) error {
	event := syscall.EpollEvent{Events: epollIn | epollOut | epollRdHup | epollET, Fd: int32(fd)}
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, fd, &event)
}

// addLevel registers a descriptor that should report as long as it is readable.
// It is for the wake pipe, which is drained rather than parsed.
func (p *poller) addLevel(fd int) error {
	event := syscall.EpollEvent{Events: epollIn, Fd: int32(fd)}
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, fd, &event)
}

// remove withdraws a descriptor. A descriptor that is simply closed removes
// itself.
func (p *poller) remove(fd int) {
	_ = syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_DEL, fd, nil)
}

// wait fills events with ready descriptors and reports how many, parking until
// there is one if block is set.
func (p *poller) wait(events []pollEvent, block bool) (int, error) {
	timeout := 0
	if block {
		timeout = -1
	}
	for {
		n, err := syscall.EpollWait(p.fd, events, timeout)
		if err == syscall.EINTR {
			continue
		}
		return max(n, 0), err
	}
}

// readiness decodes one report.
//
// An error or hang-up is reported as both readable and writable, and as ended:
// the cheapest way to learn what actually happened to a socket is to try to use
// it, and the read or write that fails carries the real error.
func readiness(event *pollEvent) (fd int, readable, writable, ended bool) {
	failed := event.Events&(epollErr|epollHup) != 0
	ended = failed || event.Events&epollRdHup != 0

	return int(event.Fd), event.Events&epollIn != 0 || ended, event.Events&epollOut != 0 || failed, ended
}

func (p *poller) close() {
	_ = syscall.Close(p.fd)
}
