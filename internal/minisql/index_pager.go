package minisql

import (
	"context"
	"fmt"
)

type indexPager[T IndexKey] struct {
	*pagerImpl
	columns []Column
	unique  bool
}

// GetPage ...
func (p *indexPager[T]) GetPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *indexPager[T]) unmarshal(totalPages uint32, pageIdx PageIndex, buf []byte) (*Page, error) {
	idx := 0

	// Requesting a new page: pageIdx == totalPages means the page does not exist
	// on disk or in the WAL yet.  The buf is all-zeros (no file read was
	// performed for truly new pages).
	if uint32(pageIdx) == totalPages {
		node := NewIndexNode[T](p.unique)
		buf[idx] = PageTypeIndex
		_, err := node.Unmarshal(p.columns, buf)
		if err != nil {
			return nil, err
		}
		node.Header.RightChild = RightChildNotSet
		return &Page{Index: pageIdx, IndexNode: node}, nil
	}

	// Existing page
	switch buf[idx] {
	case PageTypeIndex:
		node := NewIndexNode[T](p.unique)
		_, err := node.Unmarshal(p.columns, buf)
		if err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, IndexNode: node}, nil
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
