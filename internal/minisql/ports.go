package minisql

import (
	"context"
)

// Parser is the interface the SQL parsing layer must satisfy. It converts a raw
// SQL string into a slice of Statements ready for preparation and execution.
type Parser interface {
	// Parse tokenises and parses sql, returning one Statement per SQL statement.
	Parse(context.Context, string) ([]Statement, error)
}

// TableProvider provides thread-safe access to tables
type TableProvider interface {
	// GetTable retrieves the named table, returning false when it does not exist.
	GetTable(ctx context.Context, name string) (*Table, bool)
}

// LRUCache is the generic least-recently-used cache interface used by the pager
// to track which pages to evict when the cache reaches capacity.
type LRUCache[T any] interface {
	// Get returns the value associated with key without changing its LRU position.
	Get(T) (any, bool)
	// GetAndPromote returns the value for key and moves it to the MRU position.
	GetAndPromote(T) (any, bool)
	// Put inserts or replaces the value for key; pinned entries are never evicted.
	Put(T, any, bool)
	// EvictIfNeeded removes the least-recently-used entry when the cache is full.
	EvictIfNeeded() (T, bool)
	// Purge removes all entries, used to invalidate the plan cache on schema changes.
	Purge()
}

// PagerFactory creates typed Pager instances for table data and index pages,
// wiring the correct unmarshal logic for each B+ tree's key type.
type PagerFactory interface {
	// ForTable returns a Pager that unmarshals rows using the given column schema.
	ForTable([]Column) Pager
	// ForIndex returns a Pager that unmarshals index nodes for the given columns.
	ForIndex(columns []Column, unique bool) Pager
	// ForInvertedIndex returns a Pager for inverted (full-text / JSON) index pages.
	ForInvertedIndex() Pager
	// ForHNSWIndex returns a Pager that unmarshals HNSW meta and data pages.
	ForHNSWIndex() Pager
}

// TxPagerFactory is a function that opens a transactional pager for the named
// table or index, used during database initialisation to wire up B+ trees.
type TxPagerFactory func(ctx context.Context, tableName, indexName string) (Pager, error)

// Pager provides read-only access to database pages. It is the read-side interface
// passed to B+ tree constructors; write operations are done via TxPager.
type Pager interface {
	// GetPage retrieves the page at the given index, reading from disk if not cached.
	GetPage(context.Context, PageIndex) (*Page, error)
	// GetHeader returns the current in-memory database header.
	GetHeader(context.Context) DatabaseHeader
	// TotalPages returns the total number of pages allocated in the database file.
	TotalPages() uint32
}

// Flusher writes cached pages from memory to the underlying storage. Used during
// WAL checkpoint and database close to persist dirty pages.
type Flusher interface {
	// TotalPages returns the number of pages currently tracked by this flusher.
	TotalPages() uint32
	// Flush writes the page at pageIdx from the cache to the underlying file.
	Flush(context.Context, PageIndex) error
	// FlushBatch writes multiple pages in a single pass, reducing fsync calls.
	FlushBatch(context.Context, []PageIndex) error
	// Close flushes all dirty pages and releases the underlying file handle.
	Close() error
}

// PageSaver extends Flusher with write operations for the in-memory page cache
// and the database header. Implemented by pagerImpl and used by the transaction
// manager to stage page mutations before committing them to the WAL.
type PageSaver interface {
	// SavePage stores page in the LRU cache at pageIdx without writing to disk.
	SavePage(context.Context, PageIndex, *Page)
	// SaveHeader updates the in-memory database header without writing to disk.
	SaveHeader(context.Context, DatabaseHeader)
	// SetWALIndex wires the WAL index into the pager's cache-miss read path.
	SetWALIndex(*WALIndex)
	// InvalidatePage removes a page from the LRU cache so the next read
	// reloads it from the WAL.  Used by RollbackTransaction to discard pages
	// that were modified in-place during a rolled-back write transaction.
	InvalidatePage(PageIndex)
	Flusher
}

// DDLSaver persists schema changes (CREATE/DROP TABLE, CREATE/DROP INDEX) to the
// database header so they survive restart.
type DDLSaver interface {
	// SaveDDLChanges atomically applies schema changes to the on-disk header.
	SaveDDLChanges(ctx context.Context, changes DDLChanges)
}

// TxPager is the read-write page accessor used inside a transaction. It extends
// ReadPage (snapshot-aware read) with ModifyPage (copy-on-write for OCC), free-page
// management, and overflow page access.
type TxPager interface {
	// ReadPage returns the page at pageIdx as seen by this transaction's snapshot.
	ReadPage(context.Context, PageIndex) (*Page, error)
	// ModifyPage returns a writable copy of the page and records it in the write set.
	ModifyPage(context.Context, PageIndex) (*Page, error)
	// GetFreePage returns a free page for reuse, allocating a new one if the free list is empty.
	GetFreePage(context.Context) (*Page, error)
	// AddFreePage returns pageIdx to the free-page list so it can be reused.
	AddFreePage(context.Context, PageIndex) error
	// GetOverflowPage reads an overflow page for a value that spans multiple pages.
	GetOverflowPage(context.Context, PageIndex) (*Page, error)
}

// BTreeIndex is the interface implemented by all B+ tree index types (primary key,
// unique, and secondary non-unique). It provides key insertion, deletion, and
// various traversal operations used by the query planner and DML paths.
type BTreeIndex interface {
	// GetRootPageIdx returns the page index of the B+ tree's root node.
	GetRootPageIdx() PageIndex
	// FindRowIDs returns all row IDs for the given key as a slice.
	// For large non-unique indexes prefer VisitRowIDs to avoid materialising the full list.
	FindRowIDs(ctx context.Context, key any) ([]RowID, error)
	// VisitRowIDs calls fn for each row ID associated with key, reading overflow pages
	// lazily one at a time.  fn may return an error to stop early (e.g. a LIMIT sentinel);
	// that error is propagated unchanged to the caller.
	VisitRowIDs(ctx context.Context, key any, fn func(RowID) error) error
	// PointUniqueRowID returns the single row ID for key on a unique index.
	// Returns ErrNotFound when the key is absent.
	PointUniqueRowID(ctx context.Context, key any) (RowID, error)
	// SeekLastKey returns the largest key currently stored under pageIdx.
	SeekLastKey(ctx context.Context, pageIdx PageIndex) (any, error)
	// Insert adds key→rowID to the index, splitting nodes as needed.
	Insert(ctx context.Context, key any, rowID RowID) error
	// Delete removes key→rowID from the index.
	Delete(ctx context.Context, key any, rowID RowID) error
	// ScanAll visits every key–rowID pair in ascending (or descending) order.
	ScanAll(ctx context.Context, reverse bool, callback indexScanner) error
	// ScanRange visits key–rowID pairs within rangeCondition in order.
	ScanRange(ctx context.Context, rangeCondition RangeCondition, reverse bool, callback indexScanner) error
	// BFS performs a breadth-first traversal of the B+ tree, calling f for each node.
	BFS(ctx context.Context, f indexCallback) error
}
