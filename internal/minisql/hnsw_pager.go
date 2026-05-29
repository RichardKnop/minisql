package minisql

import (
	"context"
	"fmt"
)

// hnswPager wraps pagerImpl with an unmarshaler that recognises HNSW meta and
// data pages in addition to the standard free page.
type hnswPager struct {
	*pagerImpl
}

// GetPage returns an HNSW page decoded with the HNSW-specific unmarshaler.
func (p *hnswPager) GetPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	return p.pagerImpl.GetPage(ctx, pageIdx, p.unmarshal)
}

func (p *hnswPager) unmarshal(totalPages uint32, pageIdx PageIndex, buf []byte) (*Page, error) {
	// New page beyond the current database boundary: return a blank HNSW meta page.
	// GetFreePage immediately calls Clear() on it, so the content here is temporary.
	if uint32(pageIdx) == totalPages {
		return &Page{Index: pageIdx, HNSWMetaPage: &hnswMetaPage{EntryPoint: hnswNoEntryPoint}}, nil
	}
	idx := 0
	if pageIdx == 0 {
		idx = RootPageConfigSize
	}
	switch buf[idx] {
	case PageTypeHNSWMeta:
		page := new(hnswMetaPage)
		if err := page.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, HNSWMetaPage: page}, nil
	case PageTypeHNSWData:
		page := new(hnswDataPage)
		if err := page.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, HNSWDataPage: page}, nil
	case PageTypeFree:
		freePage := new(FreePage)
		if err := freePage.Unmarshal(buf[idx:]); err != nil {
			return nil, err
		}
		return &Page{Index: pageIdx, FreePage: freePage}, nil
	default:
		return nil, fmt.Errorf("unrecognised HNSW page type byte %d", buf[idx])
	}
}
