package minisql

import (
	"context"
	"fmt"
)

// PageType constants identify the type of data stored in a database page.
const (
	// PageTypeLeaf identifies a leaf B+ tree node page.
	PageTypeLeaf byte = iota
	// PageTypeInternal identifies an internal (non-leaf) B+ tree node page.
	PageTypeInternal
	// PageTypeOverflow identifies a text overflow page.
	PageTypeOverflow
	// PageTypeIndex identifies an index B+ tree node page.
	PageTypeIndex
	// PageTypeFree identifies a free (unused) page available for reuse.
	PageTypeFree
	// PageTypeIndexOverflow identifies an index overflow page for large row ID lists.
	PageTypeIndexOverflow
	// PageTypeInvertedEntry identifies a future dedicated inverted-index entry tree page.
	PageTypeInvertedEntry
	// PageTypeInvertedPosting identifies a future dedicated inverted-index posting tree page.
	PageTypeInvertedPosting
	// PageTypeInvertedMeta identifies log-structured inverted-index metadata pages.
	PageTypeInvertedMeta
	// PageTypeInvertedSegment identifies log-structured inverted-index segment pages.
	PageTypeInvertedSegment
)

type tablePager struct {
	*pagerImpl
	columns []Column
}

// GetPage returns the table page at pageIdx, deserialising it via the table
// unmarshaler which selects the correct node type (leaf, internal, overflow,
// or free) based on the page-type byte in the raw buffer.
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
		freePage := new(FreePage)
		if err := freePage.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{
			Index:    pageIdx,
			FreePage: freePage,
		}, nil
	}

	return nil, fmt.Errorf("unrecognised table page type byte %d", buf[idx])
}
