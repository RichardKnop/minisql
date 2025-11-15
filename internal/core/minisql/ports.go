package minisql

import (
	"context"
)

type PagerFactory interface {
	ForTable(uint64) Pager
	ForIndex(kind ColumnKind, keySize uint64) Pager
}

type PageFlusher interface {
	TotalPages() uint32
	Flush(context.Context, uint32) error
}

type Pager interface {
	GetPage(context.Context, uint32) (*Page, error)
	GetHeader(context.Context) DatabaseHeader
	TotalPages() uint32
}

type PageSaver interface {
	SavePage(ctx context.Context, pageIdx uint32, page *Page)
	SaveHeader(ctx context.Context, header DatabaseHeader)
}

type TxPager interface {
	ReadPage(context.Context, uint32) (*Page, error)
	ModifyPage(context.Context, uint32) (*Page, error)
	GetFreePage(context.Context) (*Page, error)
	AddFreePage(context.Context, uint32) error
}

type BTreeIndex interface {
	GetRootPageIdx() uint32
	Seek(ctx context.Context, aPage *Page, keyAny any) (IndexCursor, bool, error)
	SeekLastKey(ctx context.Context, pageIdx uint32) (any, error)
	Insert(ctx context.Context, key any, rowID uint64) error
	Delete(ctx context.Context, key any) error
	BFS(ctx context.Context, f indexCallback) error
}
