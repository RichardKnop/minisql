package minisql

import (
	"context"
	"fmt"
)

type indexPager[T IndexKey] struct {
	*pagerImpl
}

func (p *indexPager[T]) GetPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *indexPager[T]) unmarshal(pageIdx PageIndex, buf []byte) (*Page, error) {
	idx := 0

	// Requesting a new page
	p.mu.RLock()
	totalPages := p.totalPages
	p.mu.RUnlock()
	if int(pageIdx) == int(totalPages) {
		node := NewUniqueIndexNode[T]()
		buf[idx] = PageTypeIndex
		_, err := node.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		node.Header.RightChild = RIGHT_CHILD_NOT_SET
		p.mu.Lock()
		p.pages = append(p.pages, &Page{Index: pageIdx, IndexNode: node})
		p.totalPages = uint32(pageIdx + 1)
		p.mu.Unlock()
		return p.pages[len(p.pages)-1], nil
	}

	// Existing page
	switch buf[idx] {
	case PageTypeIndex:
		node := NewUniqueIndexNode[T]()
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
	}

	return nil, fmt.Errorf("unrecognised page type byte %d", buf[idx])
}
