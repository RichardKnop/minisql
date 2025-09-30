package minisql

const (
	PageSize = 4096 // 4 kilobytes
	MaxPages = 1000 // temporary limit, TODO - remove later
)

type Page struct {
	Index        uint32
	InternalNode *InternalNode
	LeafNode     *LeafNode
}

func (p *Page) setParent(parentIdx uint32) {
	if p.LeafNode != nil {
		p.LeafNode.Header.Parent = parentIdx
	} else if p.InternalNode != nil {
		p.InternalNode.Header.Parent = parentIdx
	}
}
