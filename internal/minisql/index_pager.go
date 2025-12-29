package minisql

import (
	"context"
	"fmt"
)

type indexPager[T IndexKey] struct {
	*pagerImpl
	unique bool
}

func (p *indexPager[T]) GetPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *indexPager[T]) unmarshal(totalPages uint32, pageIdx PageIndex, buf []byte) (*Page, error) {
	idx := 0

	// Requesting a new page
	if uint32(pageIdx) == totalPages {
		node := NewIndexNode[T](p.unique)
		buf[idx] = PageTypeIndex
		_, err := node.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		node.Header.RightChild = RIGHT_CHILD_NOT_SET
		return &Page{Index: pageIdx, IndexNode: node}, nil
	}

	// Existing page
	switch buf[idx] {
	case PageTypeIndex:
		node := NewIndexNode[T](p.unique)
		_, err := node.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, IndexNode: node}, nil
	case PageTypeFree:
		// Free page
		aFreePage := new(FreePage)
		if err := aFreePage.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{
			Index:    pageIdx,
			FreePage: aFreePage,
		}, nil
	case PageTypeIndexOverflow:
		// Index overflow page
		overflow := new(IndexOverflowPage)
		if err := overflow.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{
			Index:             pageIdx,
			IndexOverflowNode: overflow,
		}, nil
	}

	return nil, fmt.Errorf("unrecognised index page type byte %d", buf[idx])
}
