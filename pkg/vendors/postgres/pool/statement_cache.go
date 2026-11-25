// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

// statementCache holds a connection's prepared statements by SQL, evicting the
// least recently used past its capacity.
//
// The map is allocated small and grows with use, unlike pgx's caches, which
// preallocated their full capacity on every connection: that was 57% of the
// pool's heap before a statement had been prepared.
type statementCache struct {
	capacity int
	bySQL    map[string]*statement
	// head is the most recently used statement; head.prev is the least.
	head *statement
}

func newStatementCache(capacity int) *statementCache {
	return &statementCache{capacity: capacity, bySQL: make(map[string]*statement, min(capacity, 8))}
}

// get returns the statement for sql and marks it used.
func (c *statementCache) get(sql string) *statement {
	s := c.bySQL[sql]
	if s != nil && s != c.head {
		c.unlink(s)
		c.push(s)
	}
	return s
}

// put adds a statement and returns the one it evicted, if any, so its server
// side can be closed.
func (c *statementCache) put(s *statement) *statement {
	c.bySQL[s.sql] = s
	c.push(s)
	if len(c.bySQL) <= c.capacity {
		return nil
	}
	oldest := c.head.prev
	c.remove(oldest)
	return oldest
}

// remove forgets a statement.
func (c *statementCache) remove(s *statement) {
	if c.bySQL[s.sql] != s {
		return
	}
	delete(c.bySQL, s.sql)
	c.unlink(s)
}

// clear forgets every statement and returns them, for their server sides to be
// closed.
func (c *statementCache) clear() []*statement {
	all := make([]*statement, 0, len(c.bySQL))
	for _, s := range c.bySQL {
		all = append(all, s)
	}
	clear(c.bySQL)
	c.head = nil
	return all
}

func (c *statementCache) push(s *statement) {
	if c.head == nil {
		s.prev, s.next = s, s
		c.head = s
		return
	}
	tail := c.head.prev
	s.prev, s.next = tail, c.head
	tail.next, c.head.prev = s, s
	c.head = s
}

func (c *statementCache) unlink(s *statement) {
	if s.next == s {
		c.head = nil
	} else {
		s.prev.next, s.next.prev = s.next, s.prev
		if c.head == s {
			c.head = s.next
		}
	}
	s.prev, s.next = nil, nil
}
