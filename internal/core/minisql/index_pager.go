package minisql

import (
	"context"
)

func (p *pagerImpl) ForIndex(kind ColumnKind, keySize uint64) Pager {
	switch kind {
	case Boolean:
		return &indexPager[int8]{p, keySize}
	case Int4:
		return &indexPager[int32]{p, keySize}
	case Int8:
		return &indexPager[int64]{p, keySize}
	case Real:
		return &indexPager[float32]{p, keySize}
	case Double:
		return &indexPager[float64]{p, keySize}
	case Varchar:
		return &indexPager[string]{p, keySize}
	default:
		return nil
	}
}

type indexPager[T int8 | int32 | int64 | float32 | float64 | string] struct {
	*pagerImpl
	keySize uint64
}

func (p *indexPager[T]) GetPage(ctx context.Context, pageIdx uint32) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *indexPager[T]) GetFreePage(ctx context.Context) (*Page, error) {
	return p.pagerImpl.GetFreePage(ctx, p.unmarshal)
}

func (p *indexPager[T]) AddFreePage(ctx context.Context, pageIdx uint32) error {
	return p.pagerImpl.AddFreePage(ctx, pageIdx, p.unmarshal)
}

func (p *indexPager[T]) unmarshal(pageIdx uint32, buf []byte) (*Page, error) {
	idx := 0

	if p.dbHeader.FirstFreePage != 0 && pageIdx == p.dbHeader.FirstFreePage {
		aFreePage := new(FreePage)
		if err := UnmarshalFreePage(buf[idx:], aFreePage); err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{Index: pageIdx, FreePage: aFreePage}
		return p.pages[pageIdx], nil
	}

	node := NewIndexNode[T](p.keySize)
	_, err := node.Unmarshal(buf)
	if err != nil {
		return nil, err
	}

	// Requesting a new page
	if int(pageIdx) == int(p.totalPages) {
		p.pages = append(p.pages, &Page{Index: pageIdx, IndexNode: node})
		p.totalPages = pageIdx + 1
		return p.pages[len(p.pages)-1], nil
	}

	// Existing page
	p.pages[pageIdx] = &Page{Index: pageIdx, IndexNode: node}
	return p.pages[pageIdx], nil
}
