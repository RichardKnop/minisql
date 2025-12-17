package minisql

import (
	"context"
)

type PagerFactory interface {
	ForTable([]Column) Pager
	ForIndex(kind ColumnKind, keySize uint64, unique bool) Pager
}

type PageFlusher interface {
	TotalPages() uint32
	Flush(context.Context, PageIndex) error
}

type Pager interface {
	GetPage(context.Context, PageIndex) (*Page, error)
	GetHeader(context.Context) DatabaseHeader
	TotalPages() uint32
}

type PageSaver interface {
	SavePage(context.Context, PageIndex, *Page)
	SaveHeader(context.Context, DatabaseHeader)
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
	ScanRange(ctx context.Context, rangeCondition RangeCondition, callback indexScanner) error
	BFS(ctx context.Context, f indexCallback) error
}
