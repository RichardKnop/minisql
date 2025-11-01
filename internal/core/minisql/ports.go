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
