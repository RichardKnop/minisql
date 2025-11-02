package minisql

import (
	"context"
)

type tablePager struct {
	*pagerImpl
	rowSize uint64
}

func (p *tablePager) GetPage(ctx context.Context, pageIdx uint32) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *tablePager) unmarshal(pageIdx uint32, buf []byte) (*Page, error) {
	idx := 0
	if p.dbHeader.FirstFreePage != 0 && pageIdx == p.dbHeader.FirstFreePage {
		aFreePage := new(FreePage)
		if err := UnmarshalFreePage(buf[idx:], aFreePage); err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{Index: pageIdx, FreePage: aFreePage}
		return p.pages[pageIdx], nil
	}

	// Requesting a new page
	if int(pageIdx) == int(p.totalPages) {
		// Leaf node
		leaf := NewLeafNode(p.rowSize)
		_, err := leaf.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		p.pages = append(p.pages, &Page{Index: pageIdx, LeafNode: leaf})
		p.totalPages = pageIdx + 1
		p.mu.Unlock()
		return p.pages[len(p.pages)-1], nil
	}

	if pageIdx == 0 {
		idx = RootPageConfigSize
	}

	if buf[idx] == 0 {
		// First byte is Internal flag, this condition is also true if page does not exist
		// Leaf node
		leaf := NewLeafNode(p.rowSize)
		_, err := leaf.Unmarshal(buf[idx:])
		if err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{Index: pageIdx, LeafNode: leaf}
		return p.pages[pageIdx], nil
	}

	// Internal node
	internal := new(InternalNode)
	_, err := internal.Unmarshal(buf[idx:])
	if err != nil {
		return nil, err
	}
	p.pages[pageIdx] = &Page{Index: pageIdx, InternalNode: internal}
	return p.pages[pageIdx], nil
}
