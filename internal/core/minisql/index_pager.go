package minisql

import (
	"context"
)

func NewIndexPager(aPager Pager, keySize uint64) *indexPager {
	return &indexPager{
		Pager:   aPager,
		keySize: keySize,
	}
}

type indexPager struct {
	Pager
	keySize uint64
}

func (p *indexPager) GetPage(ctx context.Context, pageIdx uint32) (*Page, error) {
	return p.Pager.GetPage(ctx, pageIdx, PageOptions{
		Type:    PageTypeIndex,
		RowSize: p.keySize,
	})
}

func (p *indexPager) TotalPages() uint32 {
	return p.Pager.TotalPages()
}

func (p *indexPager) GetFreePage(ctx context.Context) (*Page, error) {
	return p.Pager.GetFreePage(ctx, PageOptions{
		Type:    PageTypeIndex,
		RowSize: p.keySize,
	})
}

func (p *indexPager) AddFreePage(ctx context.Context, pageIdx uint32) error {
	return p.Pager.AddFreePage(ctx, pageIdx, PageOptions{
		Type:    PageTypeIndex,
		RowSize: p.keySize,
	})
}

func (p *indexPager) Flush(ctx context.Context, pageIdx uint32) error {
	return p.Pager.Flush(ctx, pageIdx)
}
