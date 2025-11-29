package minisql

import (
	"context"
)

type indexPager[T IndexKey] struct {
	*pagerImpl
	keySize uint64
}

func (p *indexPager[T]) GetPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *indexPager[T]) unmarshal(pageIdx PageIndex, buf []byte) (*Page, error) {
	idx := 0

	if p.dbHeader.FirstFreePage != 0 && pageIdx == p.dbHeader.FirstFreePage {
		aFreePage := new(FreePage)
		if err := aFreePage.Unmarshal(buf[idx:]); err != nil {
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
		node.Header.RightChild = RIGHT_CHILD_NOT_SET
		p.mu.Lock()
		p.pages = append(p.pages, &Page{Index: pageIdx, IndexNode: node})
		p.totalPages = uint32(pageIdx + 1)
		p.mu.Unlock()
		return p.pages[len(p.pages)-1], nil
	}

	// Existing page
	p.pages[pageIdx] = &Page{Index: pageIdx, IndexNode: node}
	return p.pages[pageIdx], nil
}
