package minisql

import (
	"context"
)

// Parser ...
type Parser interface {
	Parse(context.Context, string) ([]Statement, error)
}

// TableProvider provides thread-safe access to tables
type TableProvider interface {
	GetTable(ctx context.Context, name string) (*Table, bool)
}

// LRUCache ...
type LRUCache[T any] interface {
	Get(T) (any, bool)
	GetAndPromote(T) (any, bool)
	Put(T, any, bool)
	EvictIfNeeded() (T, bool)
}

// PagerFactory ...
type PagerFactory interface {
	ForTable([]Column) Pager
	ForIndex(columns []Column, unique bool) Pager
}

// TxPagerFactory ...
type TxPagerFactory func(ctx context.Context, tableName, indexName string) (Pager, error)

// Pager ...
type Pager interface {
	GetPage(context.Context, PageIndex) (*Page, error)
	GetHeader(context.Context) DatabaseHeader
	TotalPages() uint32
}

// Flusher ...
type Flusher interface {
	TotalPages() uint32
	Flush(context.Context, PageIndex) error
	FlushBatch(context.Context, []PageIndex) error
	Close() error
}

// PageSaver ...
type PageSaver interface {
	SavePage(context.Context, PageIndex, *Page)
	SaveHeader(context.Context, DatabaseHeader)
	SetWALIndex(*WALIndex)
	Flusher
}

// DDLSaver ...
type DDLSaver interface {
	SaveDDLChanges(ctx context.Context, changes DDLChanges)
}

// TxPager ...
type TxPager interface {
	ReadPage(context.Context, PageIndex) (*Page, error)
	ModifyPage(context.Context, PageIndex) (*Page, error)
	GetFreePage(context.Context) (*Page, error)
	AddFreePage(context.Context, PageIndex) error
	GetOverflowPage(context.Context, PageIndex) (*Page, error)
}

// BTreeIndex ...
type BTreeIndex interface {
	GetRootPageIdx() PageIndex
	FindRowIDs(ctx context.Context, key any) ([]RowID, error)
	SeekLastKey(ctx context.Context, pageIdx PageIndex) (any, error)
	Insert(ctx context.Context, key any, rowID RowID) error
	Delete(ctx context.Context, key any, rowID RowID) error
	ScanAll(ctx context.Context, reverse bool, callback indexScanner) error
	ScanRange(ctx context.Context, rangeCondition RangeCondition, reverse bool, callback indexScanner) error
	BFS(ctx context.Context, f indexCallback) error
}
