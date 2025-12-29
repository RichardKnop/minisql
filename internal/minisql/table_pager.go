package minisql

import (
	"context"
	"fmt"
)

const (
	PageTypeLeaf byte = iota
	PageTypeInternal
	PageTypeOverflow
	PageTypeIndex
	PageTypeFree
	PageTypeIndexOverflow
)

type tablePager struct {
	*pagerImpl
	columns []Column
}

func (p *tablePager) GetPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *tablePager) unmarshal(totalPages uint32, pageIdx PageIndex, buf []byte) (*Page, error) {
	idx := 0

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
		return &Page{
			Index:    pageIdx,
			LeafNode: leaf,
		}, nil
	case PageTypeInternal:
		// Internal node
		internal := new(InternalNode)
		_, err := internal.Unmarshal(buf[idx:])
		if err != nil {
			return nil, err
		}
		return &Page{
			Index:        pageIdx,
			InternalNode: internal,
		}, nil
	case PageTypeOverflow:
		// Overflow page
		overflow := new(OverflowPage)
		if err := overflow.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{
			Index:        pageIdx,
			OverflowPage: overflow,
		}, nil
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
	}

	return nil, fmt.Errorf("unrecognised table page type byte %d", buf[idx])
}
