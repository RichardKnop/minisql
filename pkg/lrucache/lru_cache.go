package lrucache

import (
	"sync"
)

type cacheEntry struct {
	value any
	prev  *cacheEntry
	next  *cacheEntry
	key   string
}

type cacheImpl struct {
	entries map[string]*cacheEntry
	head    *cacheEntry
	tail    *cacheEntry
	maxSize int
	mu      sync.RWMutex
}

func New(maxSize int) *cacheImpl {
	return &cacheImpl{
		entries: make(map[string]*cacheEntry),
		maxSize: maxSize,
	}
}

func (c *cacheImpl) Get(key string) (any, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// Move to front (most recently used)
	c.mu.Lock()
	c.moveToFront(entry)
	c.mu.Unlock()

	return entry.value, true
}

func (c *cacheImpl) Put(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if entry, ok := c.entries[key]; ok {
		entry.value = value
		c.moveToFront(entry)
		return
	}

	// Create new entry
	entry := &cacheEntry{
		value: value,
		key:   key,
	}

	// Add to cache
	c.entries[key] = entry
	c.addToFront(entry)

	// Evict if over capacity
	if len(c.entries) > c.maxSize {
		c.evictLRU()
	}
}

func (c *cacheImpl) moveToFront(entry *cacheEntry) {
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

func (c *cacheImpl) addToFront(entry *cacheEntry) {
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

func (c *cacheImpl) evictLRU() {
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
