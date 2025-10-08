package minisql

import (
	"context"
)

func NewTablePager(aPager Pager, rowSize uint64) *tablePager {
	return &tablePager{
		Pager:   aPager,
		rowSize: rowSize,
	}
}

type tablePager struct {
	Pager
	rowSize uint64
}

func (p *tablePager) GetPage(ctx context.Context, pageIdx uint32) (*Page, error) {
	return p.Pager.GetPage(ctx, pageIdx, PageOptions{
		Type:    PageTypeTable,
		RowSize: p.rowSize,
	})
}

func (p *tablePager) TotalPages() uint32 {
	return p.Pager.TotalPages()
}

func (p *tablePager) GetFreePage(ctx context.Context) (*Page, error) {
	return p.Pager.GetFreePage(ctx, PageOptions{
		Type:    PageTypeTable,
		RowSize: p.rowSize,
	})
}

func (p *tablePager) AddFreePage(ctx context.Context, pageIdx uint32) error {
	return p.Pager.AddFreePage(ctx, pageIdx, PageOptions{
		Type:    PageTypeTable,
		RowSize: p.rowSize,
	})
}

func (p *tablePager) Flush(ctx context.Context, pageIdx uint32) error {
	return p.Pager.Flush(ctx, pageIdx)
}
