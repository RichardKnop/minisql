package minisql

import (
	"context"
)

type Parser interface {
	Parse(context.Context, string) ([]Statement, error)
}

// TableProvider provides thread-safe access to tables
type TableProvider interface {
	GetTable(ctx context.Context, name string) (*Table, bool)
}

type LRUCache[T any] interface {
	Get(T) (any, bool)
	GetAndPromote(T) (any, bool)
	Put(T, any, bool)
	EvictIfNeeded() (T, bool)
}

type PagerFactory interface {
	ForTable([]Column) Pager
	ForIndex(columns []Column, unique bool) Pager
}

type TxPagerFactory func(ctx context.Context, tableName, indexName string) (Pager, error)

type Pager interface {
	GetPage(context.Context, PageIndex) (*Page, error)
	GetHeader(context.Context) DatabaseHeader
	TotalPages() uint32
}

type Flusher interface {
	TotalPages() uint32
	Flush(context.Context, PageIndex) error
	FlushBatch(context.Context, []PageIndex) error
	Close() error
}

type PageSaver interface {
	SavePage(context.Context, PageIndex, *Page)
	SaveHeader(context.Context, DatabaseHeader)
	Flusher
}

type DDLSaver interface {
	SaveDDLChanges(ctx context.Context, changes DDLChanges)
}

type TxPager interface {
	ReadPage(context.Context, PageIndex) (*Page, error)
	ModifyPage(context.Context, PageIndex) (*Page, error)
	GetFreePage(context.Context) (*Page, error)
	AddFreePage(context.Context, PageIndex) error
	GetOverflowPage(context.Context, PageIndex) (*Page, error)
}

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
