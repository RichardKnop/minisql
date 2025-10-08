package minisql

import (
	"context"
)

type Pager interface {
	GetPage(context.Context, uint32, PageOptions) (*Page, error)
	TotalPages() uint32
	GetFreePage(context.Context, PageOptions) (*Page, error)
	AddFreePage(context.Context, uint32, PageOptions) error
	Flush(context.Context, uint32) error
}

type TablePager interface {
	GetPage(context.Context, uint32) (*Page, error)
	TotalPages() uint32
	GetFreePage(context.Context) (*Page, error)
	AddFreePage(context.Context, uint32) error
	Flush(context.Context, uint32) error
}

type IndexPager interface {
	GetPage(context.Context, uint32) (*Page, error)
	TotalPages() uint32
	GetFreePage(context.Context) (*Page, error)
	AddFreePage(context.Context, uint32) error
	Flush(context.Context, uint32) error
}
