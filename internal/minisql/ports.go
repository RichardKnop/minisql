package minisql

import (
	"context"
)

// Parser is the interface the SQL parsing layer must satisfy. It converts a raw
// SQL string into a slice of Statements ready for preparation and execution.
type Parser interface {
	Parse(context.Context, string) ([]Statement, error)
}

// TableProvider provides thread-safe access to tables
type TableProvider interface {
	GetTable(ctx context.Context, name string) (*Table, bool)
}

// LRUCache is the generic least-recently-used cache interface used by the pager
// to track which pages to evict when the cache reaches capacity.
type LRUCache[T any] interface {
	Get(T) (any, bool)
	GetAndPromote(T) (any, bool)
	Put(T, any, bool)
	EvictIfNeeded() (T, bool)
}

// PagerFactory creates typed Pager instances for table data and index pages,
// wiring the correct unmarshal logic for each B+ tree's key type.
type PagerFactory interface {
	ForTable([]Column) Pager
	ForIndex(columns []Column, unique bool) Pager
}

// TxPagerFactory is a function that opens a transactional pager for the named
// table or index, used during database initialisation to wire up B+ trees.
type TxPagerFactory func(ctx context.Context, tableName, indexName string) (Pager, error)

// Pager provides read-only access to database pages. It is the read-side interface
// passed to B+ tree constructors; write operations are done via TxPager.
type Pager interface {
	GetPage(context.Context, PageIndex) (*Page, error)
	GetHeader(context.Context) DatabaseHeader
	TotalPages() uint32
}

// Flusher writes cached pages from memory to the underlying storage. Used during
// WAL checkpoint and database close to persist dirty pages.
type Flusher interface {
	TotalPages() uint32
	Flush(context.Context, PageIndex) error
	FlushBatch(context.Context, []PageIndex) error
	Close() error
}

// PageSaver extends Flusher with write operations for the in-memory page cache
// and the database header. Implemented by pagerImpl and used by the transaction
// manager to stage page mutations before committing them to the WAL.
type PageSaver interface {
	SavePage(context.Context, PageIndex, *Page)
	SaveHeader(context.Context, DatabaseHeader)
	SetWALIndex(*WALIndex)
	Flusher
}

// DDLSaver persists schema changes (CREATE/DROP TABLE, CREATE/DROP INDEX) to the
// database header so they survive restart.
type DDLSaver interface {
	SaveDDLChanges(ctx context.Context, changes DDLChanges)
}

// TxPager is the read-write page accessor used inside a transaction. It extends
// ReadPage (snapshot-aware read) with ModifyPage (copy-on-write for OCC), free-page
// management, and overflow page access.
type TxPager interface {
	ReadPage(context.Context, PageIndex) (*Page, error)
	ModifyPage(context.Context, PageIndex) (*Page, error)
	GetFreePage(context.Context) (*Page, error)
	AddFreePage(context.Context, PageIndex) error
	GetOverflowPage(context.Context, PageIndex) (*Page, error)
}

// BTreeIndex is the interface implemented by all B+ tree index types (primary key,
// unique, and secondary non-unique). It provides key insertion, deletion, and
// various traversal operations used by the query planner and DML paths.
type BTreeIndex interface {
	GetRootPageIdx() PageIndex
	// FindRowIDs returns all row IDs for the given key as a slice.
	// For large non-unique indexes prefer VisitRowIDs to avoid materialising the full list.
	FindRowIDs(ctx context.Context, key any) ([]RowID, error)
	// VisitRowIDs calls fn for each row ID associated with key, reading overflow pages
	// lazily one at a time.  fn may return an error to stop early (e.g. a LIMIT sentinel);
	// that error is propagated unchanged to the caller.
	VisitRowIDs(ctx context.Context, key any, fn func(RowID) error) error
	SeekLastKey(ctx context.Context, pageIdx PageIndex) (any, error)
	Insert(ctx context.Context, key any, rowID RowID) error
	Delete(ctx context.Context, key any, rowID RowID) error
	ScanAll(ctx context.Context, reverse bool, callback indexScanner) error
	ScanRange(ctx context.Context, rangeCondition RangeCondition, reverse bool, callback indexScanner) error
	BFS(ctx context.Context, f indexCallback) error
}
