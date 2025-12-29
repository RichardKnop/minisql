package minisql

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStatementCache_HitAndMiss tests basic cache hit and miss behavior
func TestStatementCache_HitAndMiss(t *testing.T) {
	t.Parallel()

	cache := newStatementCache(10)

	// Cache miss
	stmt, ok := cache.get("SELECT * FROM users")
	assert.False(t, ok)
	assert.Equal(t, Statement{}, stmt)

	// Add to cache
	mockStmt := Statement{
		Kind:      Select,
		TableName: "users",
	}
	cache.put("SELECT * FROM users", mockStmt)

	// Cache hit
	stmt, ok = cache.get("SELECT * FROM users")
	assert.True(t, ok)
	assert.Equal(t, mockStmt, stmt)

	// Different query is a cache miss
	stmt, ok = cache.get("SELECT * FROM posts")
	assert.False(t, ok)
	assert.Equal(t, Statement{}, stmt)
}

// TestStatementCache_LRUEviction tests that least recently used items are evicted
func TestStatementCache_LRUEviction(t *testing.T) {
	t.Parallel()

	cache := newStatementCache(3) // Small cache for testing

	// Add 3 items
	cache.put("query1", Statement{Kind: Select, TableName: "table1"})
	cache.put("query2", Statement{Kind: Select, TableName: "table2"})
	cache.put("query3", Statement{Kind: Select, TableName: "table3"})

	// All 3 should be in cache
	_, ok := cache.get("query1")
	assert.True(t, ok)
	_, ok = cache.get("query2")
	assert.True(t, ok)
	_, ok = cache.get("query3")
	assert.True(t, ok)

	// Add a 4th item, should evict the least recently used (query1)
	cache.put("query4", Statement{Kind: Select, TableName: "table4"})

	// query1 should be evicted
	_, ok = cache.get("query1")
	assert.False(t, ok, "query1 should have been evicted as LRU")

	// Others should still be in cache
	_, ok = cache.get("query2")
	assert.True(t, ok)
	_, ok = cache.get("query3")
	assert.True(t, ok)
	_, ok = cache.get("query4")
	assert.True(t, ok)

	// Check cache size stays at 3 and doesn't grow beyond max size
	assert.Len(t, cache.entries, 3, "Cache should have max size of 3")
}

// TestStatementCache_LRUOrdering tests that accessing items updates their LRU order
func TestStatementCache_LRUOrdering(t *testing.T) {
	t.Parallel()

	cache := newStatementCache(3)

	// Add 3 items (LRU order: query1 -> query2 -> query3)
	cache.put("query1", Statement{Kind: Select, TableName: "table1"})
	cache.put("query2", Statement{Kind: Select, TableName: "table2"})
	cache.put("query3", Statement{Kind: Select, TableName: "table3"})

	// Access query1, making it most recently used (LRU order: query2 -> query3 -> query1)
	_, ok := cache.get("query1")
	assert.True(t, ok)

	// Add query4, should evict query2 (now the LRU)
	cache.put("query4", Statement{Kind: Select, TableName: "table4"})

	// query2 should be evicted
	_, ok = cache.get("query2")
	assert.False(t, ok, "query2 should have been evicted as LRU")

	// query1 should still be in cache (was accessed recently)
	_, ok = cache.get("query1")
	assert.True(t, ok, "query1 should still be cached")

	// Others should be in cache
	_, ok = cache.get("query3")
	assert.True(t, ok)
	_, ok = cache.get("query4")
	assert.True(t, ok)
}

// TestStatementCache_Concurrent tests thread safety of the cache
func TestStatementCache_Concurrent(t *testing.T) {
	t.Parallel()

	var (
		cache = newStatementCache(100)
		wg    sync.WaitGroup
	)

	// Concurrent writes
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			query := fmt.Sprintf("SELECT * FROM table%d", n)
			cache.put(query, Statement{Kind: Select, TableName: fmt.Sprintf("table%d", n)})
		}(i)
	}

	// Concurrent reads
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			query := fmt.Sprintf("SELECT * FROM table%d", n)
			cache.get(query)
		}(i)
	}

	wg.Wait()

	// Verify all items are accessible
	for i := range 50 {
		query := fmt.Sprintf("SELECT * FROM table%d", i)
		_, ok := cache.get(query)
		assert.True(t, ok, "query %s should be in cache", query)
	}
}

// TestDatabase_PrepareStatementCaching tests the database-level statement caching
func TestDatabase_PrepareStatementCaching(t *testing.T) {
	t.Parallel()

	var (
		ctx        = context.Background()
		mockParser = new(MockParser)
		db         = &Database{
			parser:    mockParser,
			stmtCache: newStatementCache(100),
		}
	)

	// Mock the parser to return different statement instances
	query1 := `SELECT * FROM users WHERE id = ?"`
	stmt1Mock := Statement{
		Kind:      Select,
		TableName: "users",
		Conditions: OneOrMore{
			{
				FieldIsEqual("id", OperandPlaceholder, nil),
			},
		},
	}

	query2 := `SELECT * FROM posts WHERE user_id = ?`
	stmt2Mock := Statement{
		Kind:      Select,
		TableName: "posts",
		Conditions: OneOrMore{
			{
				FieldIsEqual("user_id", OperandPlaceholder, nil),
			},
		},
	}

	mockParser.On("Parse", ctx, query1).Return([]Statement{stmt1Mock}, nil).Once()

	// First call should parse
	stmt1, err := db.PrepareStatement(ctx, query1)
	assert.NoError(t, err)
	assert.Equal(t, stmt1Mock, stmt1)

	// Second call with same query should return cached statement
	stmt2, err := db.PrepareStatement(ctx, query1)
	assert.NoError(t, err)
	assert.Equal(t, stmt1Mock, stmt2)

	mockParser.On("Parse", ctx, query2).Return([]Statement{stmt2Mock}, nil).Once()

	// Different query should parse separately
	stmt3, err := db.PrepareStatement(ctx, query2)
	assert.NoError(t, err)
	assert.NotEqual(t, stmt1, stmt3, "Different queries should have different statements")
	assert.Equal(t, stmt2Mock, stmt3)
}
