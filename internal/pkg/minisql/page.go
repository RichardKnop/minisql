package minisql

const (
	PageSize = 4096 // 4 kilobytes
	MaxPages = 100  // temporary limit, TODO - remove later
)

type Page struct {
	Index        uint32
	InternalNode *InternalNode
	LeafNode     *LeafNode
}

func (p *Page) GetMaxKey() (uint64, bool) {
	if p.InternalNode != nil {
		return p.InternalNode.ICells[p.InternalNode.Header.KeysNum-1].Key, true
	}

	// Root leaf node, no keys yet
	if p.LeafNode.Header.Cells == 0 {
		return 0, false
	}

	return p.LeafNode.Cells[p.LeafNode.Header.Cells-1].Key, true
}
