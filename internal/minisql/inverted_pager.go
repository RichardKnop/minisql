package minisql

import (
	"context"
	"fmt"
)

type invertedPager struct {
	*pagerImpl
}

// GetPage returns an inverted-index page, deserialising entry pages, posting
// pages, and free pages from their page-type byte.
func (p *invertedPager) GetPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *invertedPager) unmarshal(totalPages uint32, pageIdx PageIndex, buf []byte) (*Page, error) {
	idx := 0
	if pageIdx == 0 && uint32(pageIdx) != totalPages {
		idx = RootPageConfigSize
	}
	if uint32(pageIdx) == totalPages {
		page := NewInvertedEntryPage(true)
		return &Page{Index: pageIdx, InvertedEntryPage: page}, nil
	}

	switch buf[idx] {
	case PageTypeInvertedEntry:
		page := new(invertedEntryPage)
		if err := page.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, InvertedEntryPage: page}, nil
	case PageTypeInvertedPosting:
		page := new(invertedPostingPage)
		if err := page.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, InvertedPostPage: page}, nil
	case PageTypeInvertedMeta:
		page := new(invertedMetaPage)
		if err := page.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, InvertedMetaPage: page}, nil
	case PageTypeFree:
		freePage := new(FreePage)
		if err := freePage.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, FreePage: freePage}, nil
	default:
		return nil, fmt.Errorf("unrecognised inverted index page type byte %d", buf[idx])
	}
}
