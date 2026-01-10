package lrucache

import (
	"sync"
	"sync/atomic"
)

type cacheEntry[T comparable] struct {
	value       any
	prev        *cacheEntry[T]
	next        *cacheEntry[T]
	key         T
	accessCount uint32 // Atomic counter for lazy LRU updates
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

// Get returns the value for a key without updating LRU order.
// This avoids write lock contention on hot read paths.
// LRU order is updated lazily during Put/EvictIfNeeded operations.
func (c *cacheImpl[T]) Get(key T) (any, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// Increment access count atomically (lock-free)
	// This provides approximate LRU tracking with zero lock contention
	atomic.AddUint32(&entry.accessCount, 1)

	return entry.value, true
}

// GetAndPromote returns the value and updates LRU order.
// Use this for critical pages that must stay cached (e.g., page 0).
// Most callers should use Get() for better performance.
func (c *cacheImpl[T]) GetAndPromote(key T) (any, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// Update LRU order with write lock
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
		// Reset access count when updating value
		atomic.StoreUint32(&entry.accessCount, 0)
		c.moveToFront(entry)
		return
	}

	// Evict if at capacity BEFORE adding new entry
	// This prevents evicting the newly added entry
	if evict && len(c.entries) >= c.maxSize {
		c.EvictIfNeeded()
	}

	// Create new entry
	entry := &cacheEntry[T]{
		value:       value,
		key:         key,
		accessCount: 0,
	}

	// Add to cache
	c.entries[key] = entry
	c.addToFront(entry)
}

func (c *cacheImpl[T]) EvictIfNeeded() (T, bool) {
	if len(c.entries) < c.maxSize {
		var zero T
		return zero, false
	}

	if c.tail == nil {
		var zero T
		return zero, false
	}

	// Find a victim to evict, considering access counts
	// Walk from tail toward head to find an entry with zero access count
	victim := c.tail
	maxAttempts := 3 // Limit search to avoid scanning entire list
	attempts := 0

	for victim != nil && attempts < maxAttempts {
		accessCount := atomic.LoadUint32(&victim.accessCount)
		if accessCount == 0 {
			// Found a good victim with no recent accesses
			break
		}
		// This entry was accessed, give it a second chance
		// Reset its access count and check the next one
		atomic.StoreUint32(&victim.accessCount, 0)
		victim = victim.prev
		attempts++
	}

	// If all checked entries were accessed, just evict the tail anyway
	if victim == nil {
		victim = c.tail
	}

	// Remove victim from list
	if victim.prev != nil {
		victim.prev.next = victim.next
	} else {
		c.head = victim.next
	}

	if victim.next != nil {
		victim.next.prev = victim.prev
	} else {
		c.tail = victim.prev
	}

	delete(c.entries, victim.key)

	return victim.key, true
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
