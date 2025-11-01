package minisql

import (
	"context"
)

type indexPager[T int8 | int32 | int64 | float32 | float64 | string] struct {
	*pagerImpl
	keySize uint64
}

func (p *indexPager[T]) GetPage(ctx context.Context, pageIdx uint32) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
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
		node.Header.RightChild = RIGHT_CHILD_NOT_SET
		p.mu.Lock()
		p.pages = append(p.pages, &Page{Index: pageIdx, IndexNode: node})
		p.totalPages = pageIdx + 1
		p.mu.Unlock()
		return p.pages[len(p.pages)-1], nil
	}

	// Existing page
	p.pages[pageIdx] = &Page{Index: pageIdx, IndexNode: node}
	return p.pages[pageIdx], nil
}
