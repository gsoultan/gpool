// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pooling

import (
	"sync"
	"sync/atomic"
)

// shardCount is the number of lock-striped idle buckets. A power of two keeps the
// modulo cheap, and 16 is deep enough to spread contention across realistic core
// counts without leaving every shard empty on a small pool.
const shardCount = 16

// cacheLinePadding keeps adjacent shards off the same cache line. Without it the
// per-shard mutexes and counters would false-share and defeat the sharding.
const cacheLinePadding = 24

// shard is one lock-striped bucket of idle connections.
//
// count mirrors len(conns) and is maintained under mu, but is readable without
// the lock. That lets Acquire skip empty shards with an atomic load instead of a
// lock round trip, and lets Stat report idle depth without touching any mutex.
type shard[C any] struct {
	mu    sync.Mutex
	conns []*idleConn[C]
	count atomic.Int32
	_     [cacheLinePadding]byte
}

// pop removes and returns the most recently released connection, or nil when empty.
// Reusing the hottest connection keeps the rest idle so the reaper can retire them.
func (s *shard[C]) pop() *idleConn[C] {
	if s.count.Load() == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	last := len(s.conns) - 1
	if last < 0 {
		return nil
	}
	ic := s.conns[last]
	s.conns[last] = nil
	s.conns = s.conns[:last]
	s.count.Store(int32(len(s.conns)))
	return ic
}

// push returns a connection to the shard.
func (s *shard[C]) push(ic *idleConn[C]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conns = append(s.conns, ic)
	s.count.Store(int32(len(s.conns)))
}

// takeIf removes and returns every connection matching the predicate. The shard
// stores connections; deciding which ones have outlived their usefulness is the
// engine's policy, so it is passed in rather than duplicated here.
func (s *shard[C]) takeIf(match func(*idleConn[C]) bool) []*idleConn[C] {
	if s.count.Load() == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var taken []*idleConn[C]
	kept := s.conns[:0]
	for _, ic := range s.conns {
		if match(ic) {
			taken = append(taken, ic)
			continue
		}
		kept = append(kept, ic)
	}
	clear(s.conns[len(kept):])
	s.conns = kept
	s.count.Store(int32(len(s.conns)))
	return taken
}

// drain removes and returns every connection in the shard.
func (s *shard[C]) drain() []*idleConn[C] {
	s.mu.Lock()
	defer s.mu.Unlock()

	conns := s.conns
	s.conns = nil
	s.count.Store(0)
	return conns
}
