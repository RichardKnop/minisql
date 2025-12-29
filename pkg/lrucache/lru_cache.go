package lrucache

import (
	"sync"
)

type cacheEntry[T comparable] struct {
	value any
	prev  *cacheEntry[T]
	next  *cacheEntry[T]
	key   T
}

type cacheImpl[T comparable] struct {
	entries map[T]*cacheEntry[T]
	head    *cacheEntry[T]
	tail    *cacheEntry[T]
	maxSize int
	mu      sync.RWMutex
}

func New[T comparable](maxSize int) *cacheImpl[T] {
	return &cacheImpl[T]{
		entries: make(map[T]*cacheEntry[T]),
		maxSize: maxSize,
	}
}

func (c *cacheImpl[T]) Get(key T) (any, bool) {
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

func (c *cacheImpl[T]) Put(key T, value any, evict bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if entry, ok := c.entries[key]; ok {
		entry.value = value
		c.moveToFront(entry)
		return
	}

	// Create new entry
	entry := &cacheEntry[T]{
		value: value,
		key:   key,
	}

	// Add to cache
	c.entries[key] = entry
	c.addToFront(entry)

	if evict {
		// Evict if over capacity
		c.EvictIfNeeded()
	}
}

func (c *cacheImpl[T]) EvictIfNeeded() (T, bool) {
	if len(c.entries) <= c.maxSize {
		var zero T
		return zero, false
	}

	if c.tail == nil {
		var zero T
		return zero, false
	}

	oldTail := c.tail
	c.tail = oldTail.prev

	if c.tail != nil {
		c.tail.next = nil
	} else {
		c.head = nil
	}

	delete(c.entries, oldTail.key)

	return oldTail.key, true
}

func (c *cacheImpl[T]) moveToFront(entry *cacheEntry[T]) {
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

func (c *cacheImpl[T]) addToFront(entry *cacheEntry[T]) {
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
