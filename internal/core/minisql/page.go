package minisql

const (
	PageSize = 4096 // 4 kilobytes

	// UsablePageSize returns the usable size of a page after accounting for headers
	// Page size minus base + internal/leaf header, minus key and null bitmask
	UsablePageSize = PageSize - 6 - 8 - 8 - 8
)

type PageIndex uint32

type Page struct {
	Index        PageIndex
	OverflowPage *OverflowPage
	FreePage     *FreePage
	InternalNode *InternalNode
	LeafNode     *LeafNode
	IndexNode    any
}

func (p *Page) MaxSpace() uint64 {
	maxSpace := PageSize - headerSize()
	if p.Index == 0 {
		maxSpace = PageSize - rootHeaderSize()
	}
	return maxSpace
}

func (p *Page) TakenSpace() uint64 {
	takenPageSize := uint64(0)
	for i := uint32(0); i < p.LeafNode.Header.Cells; i++ {
		takenPageSize += p.LeafNode.Cells[i].Size()
	}
	return takenPageSize
}

func (p *Page) AvailableSpace() uint64 {
	return p.MaxSpace() - p.TakenSpace()
}

func (p *Page) HasSpaceForRow(aRow *Row) bool {
	return aRow.Size()+8+8 <= p.AvailableSpace()
}

func (p *Page) AtLeastHalfFull() bool {
	return p.AvailableSpace() < p.MaxSpace()/2
}

func (p *Page) CanMergeWith(p2 *Page) bool {
	return p2.TakenSpace() <= p.AvailableSpace()
}

func (p *Page) CanBorrowFirst() bool {
	firstCellSize := p.LeafNode.Cells[0].Size()
	return p.AvailableSpace()+firstCellSize < p.MaxSpace()/2
}

func (p *Page) CanBorrowLast() bool {
	lastCellSize := p.LeafNode.Cells[p.LeafNode.Header.Cells-1].Size()
	return p.AvailableSpace()+lastCellSize < p.MaxSpace()/2
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
