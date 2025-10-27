package minisql

const (
	PageSize = 4096 // 4 kilobytes
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

// UsablePageSize returns the usable size of a page after accounting for headers
// Page size minus base + internal/leaf header, minus key and null bitmask
const UsablePageSize = PageSize - 6 - 8 - 8 - 8

func remainingPageSpace(columns []Column) int {
	remaining := UsablePageSize
	for _, aColumn := range columns {
		remaining -= int(aColumn.Size)
	}
	return remaining
}
