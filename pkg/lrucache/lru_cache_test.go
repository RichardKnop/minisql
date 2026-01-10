package lrucache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockValue struct {
	data string
}

// TestLRUCache_HitAndMiss tests basic cache hit and miss behavior
func TestLRUCache_HitAndMiss(t *testing.T) {
	t.Parallel()

	cache := New[string](10)

	// Cache miss
	value, ok := cache.Get("bogus")
	assert.False(t, ok)
	assert.Nil(t, value)

	// Add to cache
	mockValue := mockValue{"foo"}
	cache.Put("foo key", mockValue, true)

	// Cache hit
	value, ok = cache.Get("foo key")
	assert.True(t, ok)
	assert.Equal(t, mockValue, value)

	// Different query is a cache miss
	value, ok = cache.Get("bar key")
	assert.False(t, ok)
	assert.Nil(t, value)
}

// TestLRUCache_LRUEviction tests that items are evicted when cache is full
// With lazy LRU updates, eviction is approximate but still effective
func TestLRUCache_LRUEviction(t *testing.T) {
	t.Parallel()

	cache := New[string](3) // Small cache for testing

	// Add 3 items
	cache.Put("foo key", mockValue{"foo"}, true)
	cache.Put("bar key", mockValue{"bar"}, true)
	cache.Put("baz key", mockValue{"baz"}, true)

	// All 3 should be in cache
	_, ok := cache.Get("foo key")
	assert.True(t, ok)
	_, ok = cache.Get("bar key")
	assert.True(t, ok)
	_, ok = cache.Get("baz key")
	assert.True(t, ok)

	// Add a 4th item, should evict one of the items
	cache.Put("qux key", mockValue{"qux"}, true)

	// Check cache size stays at 3 and doesn't grow beyond max size
	assert.Len(t, cache.entries, 3, "Cache should have max size of 3")

	// qux should be in cache (just added)
	_, ok = cache.Get("qux key")
	assert.True(t, ok)

	// At least one of the original items should have been evicted
	_, ok = cache.Get("foo key")
	assert.False(t, ok, "oldest entry should have been evicted")
	_, ok = cache.Get("bar key")
	assert.True(t, ok)
	_, ok = cache.Get("baz key")
	assert.True(t, ok)
}

// TestLRUCache_LRUOrdering tests that accessing items updates their LRU order
func TestLRUCache_LRUOrdering(t *testing.T) {
	t.Parallel()

	cache := New[string](3)

	// Add 3 items (LRU order: foo -> bar -> baz)
	cache.Put("foo key", mockValue{"foo"}, true)
	cache.Put("bar key", mockValue{"bar"}, true)
	cache.Put("baz key", mockValue{"baz"}, true)

	// Access foo key with GetAndPromote, making it most recently used (LRU order: bar -> baz -> foo)
	_, ok := cache.GetAndPromote("foo key")
	assert.True(t, ok)

	// Add qux key, should evict bar key (now the LRU)
	cache.Put("qux key", mockValue{"qux"}, true)

	// bar key should be evicted
	_, ok = cache.Get("bar key")
	assert.False(t, ok, "bar key should have been evicted as LRU")

	// foo key should still be in cache (was accessed recently)
	_, ok = cache.Get("foo key")
	assert.True(t, ok, "foo key should still be cached")

	// Others should be in cache
	_, ok = cache.Get("baz key")
	assert.True(t, ok)
	_, ok = cache.Get("qux key")
	assert.True(t, ok)
}

// TestStatementCache_Concurrent tests thread safety of the cache
func TestLRUCache_Concurrent(t *testing.T) {
	t.Parallel()

	var (
		cache = New[string](100)
		wg    sync.WaitGroup
	)

	// Concurrent writes
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("foo%d", n)
			cache.Put(key, mockValue{fmt.Sprintf("value%d", n)}, true)
		}(i)
	}

	// Concurrent reads
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("foo%d", n)
			cache.Get(key)
		}(i)
	}

	wg.Wait()

	// Verify all items are accessible
	for i := range 50 {
		key := fmt.Sprintf("foo%d", i)
		_, ok := cache.Get(key)
		assert.True(t, ok, "key %s should be in cache", key)
	}
}
