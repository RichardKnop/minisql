package minisql

import (
	"context"
	"fmt"
)

const (
	PageTypeLeaf byte = iota
	PageTypeInternal
	PageTypeOverflow
	PageTypeFree
)

type tablePager struct {
	*pagerImpl
	columns []Column
}

func (p *tablePager) GetPage(ctx context.Context, pageIdx uint32) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *tablePager) unmarshal(pageIdx uint32, buf []byte) (*Page, error) {
	idx := 0

	// Requesting a new page
	if int(pageIdx) == int(p.totalPages) {
		// Leaf node
		leaf := NewLeafNode()
		_, err := leaf.Unmarshal(p.columns, buf)
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		p.pages = append(p.pages, &Page{
			Index:    pageIdx,
			LeafNode: leaf,
		})
		p.totalPages = pageIdx + 1
		p.mu.Unlock()
		return p.pages[len(p.pages)-1], nil
	}

	if pageIdx == 0 {
		idx = RootPageConfigSize
	}

	switch buf[idx] {
	case PageTypeLeaf:
		// Leaf node
		leaf := NewLeafNode()
		_, err := leaf.Unmarshal(p.columns, buf[idx:])
		if err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{
			Index:    pageIdx,
			LeafNode: leaf,
		}
		return p.pages[pageIdx], nil
	case PageTypeInternal:
		// Internal node
		internal := new(InternalNode)
		_, err := internal.Unmarshal(buf[idx:])
		if err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{
			Index:        pageIdx,
			InternalNode: internal,
		}
		return p.pages[pageIdx], nil
	case PageTypeOverflow:
		// Overflow page
		overflow := new(OverflowPage)
		if err := overflow.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{
			Index:        pageIdx,
			OverflowPage: overflow,
		}
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
