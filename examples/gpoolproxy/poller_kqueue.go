// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package main

import (
	"syscall"
)

// pollEvent is one readiness report as the kernel writes it.
type pollEvent = syscall.Kevent_t

// poller is the kqueue every worker waits on. It mirrors the epoll
// implementation — edge-triggered (EV_CLEAR), both directions registered once —
// so the workers above it do not know which kernel they are on.
type poller struct {
	fd int
}

func newPoller() (*poller, error) {
	fd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}
	syscall.CloseOnExec(fd)
	return &poller{fd: fd}, nil
}

func change(fd int, filter int16, flags uint16) syscall.Kevent_t {
	var event syscall.Kevent_t
	syscall.SetKevent(&event, fd, int(filter), int(flags))
	return event
}

// add registers a descriptor for edge-triggered readiness in both directions.
func (p *poller) add(fd int) error {
	changes := []syscall.Kevent_t{
		change(fd, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_CLEAR),
		change(fd, syscall.EVFILT_WRITE, syscall.EV_ADD|syscall.EV_CLEAR),
	}
	_, err := syscall.Kevent(p.fd, changes, nil, nil)
	return err
}

// addLevel registers a descriptor that should report as long as it is readable.
func (p *poller) addLevel(fd int) error {
	changes := []syscall.Kevent_t{change(fd, syscall.EVFILT_READ, syscall.EV_ADD)}
	_, err := syscall.Kevent(p.fd, changes, nil, nil)
	return err
}

// remove withdraws a descriptor. A descriptor that is simply closed removes
// itself.
func (p *poller) remove(fd int) {
	changes := []syscall.Kevent_t{
		change(fd, syscall.EVFILT_READ, syscall.EV_DELETE),
		change(fd, syscall.EVFILT_WRITE, syscall.EV_DELETE),
	}
	_, _ = syscall.Kevent(p.fd, changes, nil, nil)
}

// wait fills events with ready filters and reports how many, parking until there
// is one if block is set.
func (p *poller) wait(events []pollEvent, block bool) (int, error) {
	var timeout *syscall.Timespec
	if !block {
		timeout = &syscall.Timespec{}
	}
	for {
		n, err := syscall.Kevent(p.fd, nil, events, timeout)
		if err == syscall.EINTR {
			continue
		}
		return max(n, 0), err
	}
}

// readiness decodes one report. kqueue reports each direction as its own event,
// where epoll merges them; a worker tolerates a descriptor appearing twice.
func readiness(event *pollEvent) (fd int, readable, writable, ended bool) {
	ended = event.Flags&(syscall.EV_EOF|syscall.EV_ERROR) != 0

	return int(event.Ident), event.Filter == syscall.EVFILT_READ || ended, event.Filter == syscall.EVFILT_WRITE || ended, ended
}

func (p *poller) close() {
	_ = syscall.Close(p.fd)
}
