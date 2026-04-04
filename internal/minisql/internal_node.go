package minisql

import (
	"math"
)

// InternalNode capacity constants derived from page and header sizes.
// Page size: 4096, Header size: 6 (base) + 8 (internal), ICell size: 12.
const (
	// InternalNodeMaxCells is (4096 - 6 - 8) / 12.
	InternalNodeMaxCells = 340
	// RootInternalNodeMaxCells is (4096 - 6 - 8 - 100) / 12.
	RootInternalNodeMaxCells = 331
)

// InternalNodeHeader ...
type InternalNodeHeader struct {
	Header
	KeysNum    uint32
	RightChild PageIndex
}

// Size ...
func (h *InternalNodeHeader) Size() (s uint64) {
	return h.Header.Size() + 8
}

// Marshal ...
func (h *InternalNodeHeader) Marshal(buf []byte) {
	i := uint64(0)

	h.Header.Marshal(buf[i:])
	i += h.Header.Size()

	buf[i+0] = byte(h.KeysNum >> 0)
	buf[i+1] = byte(h.KeysNum >> 8)
	buf[i+2] = byte(h.KeysNum >> 16)
	buf[i+3] = byte(h.KeysNum >> 24)

	buf[i+4] = byte(h.RightChild >> 0)
	buf[i+5] = byte(h.RightChild >> 8)
	buf[i+6] = byte(h.RightChild >> 16)
	buf[i+7] = byte(h.RightChild >> 24)

	i += 8
}

// Unmarshal ...
func (h *InternalNodeHeader) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := h.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	h.KeysNum = 0 |
		(uint32(buf[i+0]) << 0) |
		(uint32(buf[i+1]) << 8) |
		(uint32(buf[i+2]) << 16) |
		(uint32(buf[i+3]) << 24)

	h.RightChild = PageIndex(0 |
		(uint32(buf[i+4]) << 0) |
		(uint32(buf[i+5]) << 8) |
		(uint32(buf[i+6]) << 16) |
		(uint32(buf[i+7]) << 24))

	return h.Size(), nil
}

// ICell ...
type ICell struct {
	Key   RowID
	Child PageIndex
}

// ICellSize is the serialised byte size of an ICell (8-byte key + 4-byte child pointer).
const ICellSize = 12 // (8+4)

// Size ...
func (c *ICell) Size() uint64 {
	return ICellSize
}

// Marshal ...
func (c *ICell) Marshal(buf []byte) {
	i := uint64(0)

	buf = marshalUint64(buf, uint64(c.Key), i)
	i += 8

	marshalUint32(buf, uint32(c.Child), i)
}

// Unmarshal ...
func (c *ICell) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	c.Key = RowID(unmarshalUint64(buf, i))
	i += 8

	c.Child = PageIndex(unmarshalUint32(buf, i))
	i += 4

	return c.Size(), nil
}

// InternalNode ...
type InternalNode struct {
	Header InternalNodeHeader
	ICells [InternalNodeMaxCells]ICell
}

// Clone ...
func (n *InternalNode) Clone() *InternalNode {
	nodeCopy := NewInternalNode()
	copy(nodeCopy.ICells[:], n.ICells[:])
	nodeCopy.Header = n.Header
	return nodeCopy
}

// RightChildNotSet is the sentinel value indicating an internal node has no right child yet.
const RightChildNotSet = math.MaxUint32

// NewInternalNode ...
func NewInternalNode() *InternalNode {
	node := InternalNode{
		Header: InternalNodeHeader{
			Header: Header{
				IsInternal: true,
			},
			RightChild: RightChildNotSet,
		},
		ICells: [InternalNodeMaxCells]ICell{},
	}
	return &node
}

// Size ...
func (n *InternalNode) Size() uint64 {
	size := n.Header.Size()
	for idx := range n.ICells {
		size += n.ICells[idx].Size()
	}
	return size
}

// Marshal ...
func (n *InternalNode) Marshal(buf []byte) error {
	i := uint64(0)

	n.Header.Marshal(buf[i+0:])
	i += n.Header.Size()

	for idx := range n.ICells[0:n.Header.KeysNum] {
		n.ICells[idx].Marshal(buf[i:])
		i += n.ICells[idx].Size()
	}

	return nil
}

// Unmarshal ...
func (n *InternalNode) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	for idx := range n.ICells[0:n.Header.KeysNum] {
		ci, err := n.ICells[idx].Unmarshal(buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}
