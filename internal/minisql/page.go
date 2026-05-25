package minisql

const (
	// PageSize is the fixed size in bytes of every database page (4 KiB).
	PageSize = 4096 // 4 kilobytes

	// UsablePageSize returns the usable size of a page after accounting for headers
	// Page size minus base + internal/leaf header, minus key and null bitmask
	UsablePageSize = PageSize - 7 - 8 - 8 - 8
)

// PageIndex is a 0-based index that identifies a page within a database file.
// Page 0 is always the root / header page for a given B+ tree.
type PageIndex uint32

// Page is the in-memory representation of a 4 KiB database page. Exactly one
// of the node/page fields is non-nil at any time, reflecting the page's type
// (leaf, internal, index, overflow, etc.).
type Page struct {
	IndexNode           any
	OverflowPage        *OverflowPage
	FreePage            *FreePage
	InternalNode        *InternalNode
	LeafNode            *LeafNode
	IndexOverflowNode   *IndexOverflowPage
	InvertedEntryPage   *invertedEntryPage
	InvertedPostPage    *invertedPostingPage
	InvertedMetaPage    *invertedMetaPage
	InvertedSegmentPage *invertedSegmentPage
	Index               PageIndex
}

// Clone creates a deep copy of the page.
func (p *Page) Clone() *Page {
	pageCopy := &Page{
		Index: p.Index,
	}

	switch {
	case p.LeafNode != nil:
		pageCopy.LeafNode = p.LeafNode.Clone()
	case p.InternalNode != nil:
		pageCopy.InternalNode = p.InternalNode.Clone()
	case p.FreePage != nil:
		pageCopy.FreePage = &FreePage{
			NextFreePage: p.FreePage.NextFreePage,
		}
	case p.OverflowPage != nil:
		pageCopy.OverflowPage = &OverflowPage{
			Header: p.OverflowPage.Header,
			Data:   p.OverflowPage.Data,
		}
	case p.IndexNode != nil:
		pageCopy.IndexNode = copyIndexNode(p.IndexNode)
	case p.IndexOverflowNode != nil:
		pageCopy.IndexOverflowNode = &IndexOverflowPage{
			Header: p.IndexOverflowNode.Header,
			RowIDs: make([]RowID, len(p.IndexOverflowNode.RowIDs)),
		}
		copy(pageCopy.IndexOverflowNode.RowIDs, p.IndexOverflowNode.RowIDs)
	case p.InvertedEntryPage != nil:
		pageCopy.InvertedEntryPage = p.InvertedEntryPage.Clone()
	case p.InvertedPostPage != nil:
		pageCopy.InvertedPostPage = p.InvertedPostPage.Clone()
	case p.InvertedMetaPage != nil:
		pageCopy.InvertedMetaPage = p.InvertedMetaPage.Clone()
	case p.InvertedSegmentPage != nil:
		pageCopy.InvertedSegmentPage = p.InvertedSegmentPage.Clone()
	}

	return pageCopy
}

func headerSize() uint64 {
	return 6 + 8 // base header + leaf/internal header
}

func rootHeaderSize() uint64 {
	return 6 + 8 + RootPageConfigSize // base header + leaf/internal header + root page config
}

func (p *Page) setParent(parentIdx PageIndex) {
	if p.LeafNode != nil {
		p.LeafNode.Header.Parent = parentIdx
	} else if p.InternalNode != nil {
		p.InternalNode.Header.Parent = parentIdx
	}
}
