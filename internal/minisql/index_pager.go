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

func (p *indexPager[T]) unmarshal(pageIdx PageIndex, buf []byte) (*Page, error) {
	idx := 0

	// Note: p.mu is already locked by GetPage caller in pagerImpl
	// Requesting a new page
	if int(pageIdx) == int(p.totalPages) {
		node := NewIndexNode[T](p.unique)
		buf[idx] = PageTypeIndex
		_, err := node.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		node.Header.RightChild = RIGHT_CHILD_NOT_SET
		p.pages = append(p.pages, &Page{Index: pageIdx, IndexNode: node})
		p.totalPages = uint32(pageIdx + 1)
		return p.pages[len(p.pages)-1], nil
	}

	// Existing page
	switch buf[idx] {
	case PageTypeIndex:
		node := NewIndexNode[T](p.unique)
		_, err := node.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{Index: pageIdx, IndexNode: node}
		return p.pages[pageIdx], nil
	case PageTypeFree:
		// Free page
		aFreePage := new(FreePage)
		if err := aFreePage.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{
			Index:    pageIdx,
			FreePage: aFreePage,
		}
		return p.pages[pageIdx], nil
	case PageTypeIndexOverflow:
		// Index overflow page
		overflow := new(IndexOverflowPage)
		if err := overflow.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{
			Index:             pageIdx,
			IndexOverflowNode: overflow,
		}
		return p.pages[pageIdx], nil
	}

	return nil, fmt.Errorf("unrecognised index page type byte %d", buf[idx])
}
