package minisql

import (
	"sync"
)

const (
	DefaultMaxCachedStatements = 1000
)

type statementCacheEntry struct {
	stmt Statement
	prev *statementCacheEntry
	next *statementCacheEntry
	key  string
}

type statementCache struct {
	entries map[string]*statementCacheEntry
	head    *statementCacheEntry
	tail    *statementCacheEntry
	maxSize int
	mu      sync.RWMutex
}

func newStatementCache(maxSize int) *statementCache {
	return &statementCache{
		entries: make(map[string]*statementCacheEntry),
		maxSize: maxSize,
	}
}

func (c *statementCache) get(key string) (Statement, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return Statement{}, false
	}

	// Move to front (most recently used)
	c.mu.Lock()
	c.moveToFront(entry)
	c.mu.Unlock()

	return entry.stmt, true
}

func (c *statementCache) put(key string, stmt Statement) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if entry, ok := c.entries[key]; ok {
		entry.stmt = stmt
		c.moveToFront(entry)
		return
	}

	// Create new entry
	entry := &statementCacheEntry{
		stmt: stmt,
		key:  key,
	}

	// Add to cache
	c.entries[key] = entry
	c.addToFront(entry)

	// Evict if over capacity
	if len(c.entries) > c.maxSize {
		c.evictLRU()
	}
}

func (c *statementCache) moveToFront(entry *statementCacheEntry) {
	if entry == c.head {
		return
	}

	// Remove from current position
	if entry.prev != nil {
		entry.prev.next = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	}
	if entry == c.tail {
		c.tail = entry.prev
	}

	// Add to front
	c.addToFront(entry)
}

func (c *statementCache) addToFront(entry *statementCacheEntry) {
	entry.next = c.head
	entry.prev = nil

	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry

	if c.tail == nil {
		c.tail = entry
	}
}

func (c *statementCache) evictLRU() {
	if c.tail == nil {
		return
	}

	oldTail := c.tail
	c.tail = oldTail.prev

	if c.tail != nil {
		c.tail.next = nil
	} else {
		c.head = nil
	}

	delete(c.entries, oldTail.key)
}
