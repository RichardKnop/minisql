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
