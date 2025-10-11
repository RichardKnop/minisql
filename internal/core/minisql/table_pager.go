package minisql

import (
	"context"
)

func (p *pagerImpl) ForTable(rowSize uint64) Pager {
	return &tablePager{
		pagerImpl: p,
		rowSize:   rowSize,
	}
}

type tablePager struct {
	*pagerImpl
	rowSize uint64
}

func (p *tablePager) GetPage(ctx context.Context, pageIdx uint32) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *tablePager) GetFreePage(ctx context.Context) (*Page, error) {
	return p.pagerImpl.GetFreePage(ctx, p.unmarshal)
}

func (p *tablePager) AddFreePage(ctx context.Context, pageIdx uint32) error {
	return p.pagerImpl.AddFreePage(ctx, pageIdx, p.unmarshal)
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
		if err := unmarshalLeaf(pageIdx, leaf, buf); err != nil {
			return nil, err
		}
		p.pages = append(p.pages, &Page{Index: pageIdx, LeafNode: leaf})
		p.totalPages = pageIdx + 1
		return p.pages[len(p.pages)-1], nil
	}

	if pageIdx == 0 {
		idx = RootPageConfigSize
	}

	if buf[idx] == 0 {
		// First byte is Internal flag, this condition is also true if page does not exist
		// Leaf node
		leaf := NewLeafNode(p.rowSize)
		if err := unmarshalLeaf(pageIdx, leaf, buf[idx:]); err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &Page{Index: pageIdx, LeafNode: leaf}
		return p.pages[pageIdx], nil
	}

	// Internal node
	internal := new(InternalNode)
	if err := unmarshalInternal(pageIdx, internal, buf[idx:]); err != nil {
		return nil, err
	}
	p.pages[pageIdx] = &Page{Index: pageIdx, InternalNode: internal}
	return p.pages[pageIdx], nil
}

func unmarshalLeaf(pageIdx uint32, leaf *LeafNode, buf []byte) error {
	unmarshaler := leaf.Unmarshal
	if pageIdx == 0 {
		unmarshaler = leaf.UnmarshalRoot
	}
	_, err := unmarshaler(buf)
	if err != nil {
		return err
	}
	return nil
}

func unmarshalInternal(pageIdx uint32, internal *InternalNode, buf []byte) error {
	unmarshaler := internal.Unmarshal
	if pageIdx == 0 {
		unmarshaler = internal.UnmarshalRoot
	}
	_, err := unmarshaler(buf)
	if err != nil {
		return err
	}
	return nil
}
