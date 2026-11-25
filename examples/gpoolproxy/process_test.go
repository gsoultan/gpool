// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package main

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"time"
)

// process is a proxy under measurement, identified by its pid on this host.
type process struct {
	pid int
}

// usage is one reading of a process's CPU time and resident memory.
type usage struct {
	cpu time.Duration
	rss int64
	ok  bool
}

// processOf reads a pid from the named environment variable. It returns nil when
// the variable is unset, which skips the cost metrics without failing the run.
func processOf(env string) *process {
	if env == "" {
		return nil
	}
	pid, err := strconv.Atoi(os.Getenv(env))
	if err != nil || pid <= 0 {
		return nil
	}
	return &process{pid: pid}
}

// clockTick is the unit /proc/<pid>/stat reports CPU time in. USER_HZ is 100 on
// every Linux architecture Go supports.
const clockTick = 10 * time.Millisecond

// sample reads user plus system CPU time, and resident memory, from /proc.
func (p *process) sample() usage {
	if p == nil {
		return usage{}
	}
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(p.pid) + "/stat")
	if err != nil {
		return usage{}
	}
	// The command name is parenthesised and may contain spaces, so fields are
	// counted from the closing parenthesis.
	fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
	if len(fields) < 13 {
		return usage{}
	}
	user, _ := strconv.ParseInt(fields[11], 10, 64)
	system, _ := strconv.ParseInt(fields[12], 10, 64)

	return usage{
		cpu: time.Duration(user+system) * clockTick,
		rss: residentBytes(p.pid),
		ok:  true,
	}
}

// residentBytes reads VmRSS from /proc/<pid>/status.
func residentBytes(pid int) int64 {
	status, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(status), "\n") {
		if value, found := strings.CutPrefix(line, "VmRSS:"); found {
			kib, _ := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(value), " kB"), 10, 64)
			return kib << 10
		}
	}
	return 0
}
