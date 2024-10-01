package minisql

import (
	"github.com/RichardKnop/minisql/internal/pkg/node"
)

const (
	PageSize = 4096 // 4 kilobytes
	MaxPages = 100  // temporary limit, TODO - remove later
)

type Page struct {
	InternalNode *node.InternalNode
	LeafNode     *node.LeafNode
}

func (p *Page) GetMaxKey() (uint32, bool) {
	if p.InternalNode != nil {
		return p.InternalNode.ICells[p.InternalNode.Header.KeysNum-1].Key, true
	}
	// Root leaf node, no keys yet
	if p.LeafNode.Header.Cells == 0 {
		return 0, false
	}
	return p.LeafNode.Cells[p.LeafNode.Header.Cells-1].Key, true
}
