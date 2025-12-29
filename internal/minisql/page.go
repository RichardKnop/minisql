package minisql

const (
	PageSize = 4096 // 4 kilobytes

	// UsablePageSize returns the usable size of a page after accounting for headers
	// Page size minus base + internal/leaf header, minus key and null bitmask
	UsablePageSize = PageSize - 7 - 8 - 8 - 8
)

type PageIndex uint32

type Page struct {
	Index             PageIndex
	OverflowPage      *OverflowPage
	FreePage          *FreePage
	InternalNode      *InternalNode
	LeafNode          *LeafNode
	IndexNode         any
	IndexOverflowNode *IndexOverflowPage
}

// Create a deep copy of the page
func (p *Page) Clone() *Page {
	pageCopy := &Page{
		Index: p.Index,
	}

	if p.LeafNode != nil {
		pageCopy.LeafNode = p.LeafNode.Clone()
	} else if p.InternalNode != nil {
		pageCopy.InternalNode = p.InternalNode.Clone()
	} else if p.FreePage != nil {
		pageCopy.FreePage = &FreePage{
			NextFreePage: p.FreePage.NextFreePage,
		}
	} else if p.OverflowPage != nil {
		pageCopy.OverflowPage = &OverflowPage{
			Header: p.OverflowPage.Header,
			Data:   p.OverflowPage.Data,
		}
	} else if p.IndexNode != nil {
		pageCopy.IndexNode = copyIndexNode(p.IndexNode)
	} else if p.IndexOverflowNode != nil {
		pageCopy.IndexOverflowNode = &IndexOverflowPage{
			Header: p.IndexOverflowNode.Header,
			RowIDs: make([]RowID, len(p.IndexOverflowNode.RowIDs)),
		}
		copy(pageCopy.IndexOverflowNode.RowIDs, p.IndexOverflowNode.RowIDs)
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
