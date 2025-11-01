package minisql

const (
	PageSize = 4096 // 4 kilobytes

	// UsablePageSize returns the usable size of a page after accounting for headers
	// Page size minus base + internal/leaf header, minus key and null bitmask
	UsablePageSize = PageSize - 6 - 8 - 8 - 8
)

type Page struct {
	Index        uint32
	FreePage     *FreePage
	InternalNode *InternalNode
	LeafNode     *LeafNode
	IndexNode    any
}

func (p *Page) setParent(parentIdx uint32) {
	if p.LeafNode != nil {
		p.LeafNode.Header.Parent = parentIdx
	} else if p.InternalNode != nil {
		p.InternalNode.Header.Parent = parentIdx
	}
}

func remainingPageSpace(columns []Column) int {
	remaining := UsablePageSize
	for _, aColumn := range columns {
		remaining -= int(aColumn.Size)
	}
	return remaining
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
	} else if p.IndexNode != nil {
		pageCopy.IndexNode = copyIndexNode(p.IndexNode)
	}

	return pageCopy
}
