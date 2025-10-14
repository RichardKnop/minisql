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
	GetFreePage(context.Context) (*Page, error)
	AddFreePage(context.Context, uint32) error
	Flush(context.Context, uint32) error
}
