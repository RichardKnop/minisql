package minisql

import (
	"fmt"
	"math"
)

// InternalNode capacity constants derived from page and header sizes.
// Page size: 4096, Header size: 6 (base) + 8 (internal), ICell size: 12, checksum: 4.
const (
	// InternalNodeMaxCells is (4096 - 6 - 8 - 4) / 12.
	InternalNodeMaxCells = 339
	// RootInternalNodeMaxCells is (4096 - 6 - 8 - 100 - 4) / 12.
	RootInternalNodeMaxCells = 331
)

// InternalNodeHeader is the on-disk header for an internal B+ tree node. It
// extends the base Header with the number of separator keys and the page index
// of the rightmost child.
type InternalNodeHeader struct {
	Header
	KeysNum    uint32
	RightChild PageIndex
}

// Size returns the serialised byte size of InternalNodeHeader (base Header + 8 bytes).
func (h *InternalNodeHeader) Size() (s uint64) {
	return h.Header.Size() + 8
}

// Marshal serialises the header into buf in little-endian byte order.
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

// Unmarshal deserialises the header from buf and returns the number of bytes consumed.
func (h *InternalNodeHeader) Unmarshal(buf []byte) (uint64, error) {
	if uint64(len(buf)) < h.Size() {
		return 0, fmt.Errorf("internal node header unmarshal: buffer too short (%d < %d)", len(buf), h.Size())
	}
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

// ICell is a single key-pointer pair stored in an internal B+ tree node.
// Key is the separator row ID and Child is the left child page index.
type ICell struct {
	Key   RowID
	Child PageIndex
}

// ICellSize is the serialised byte size of an ICell (8-byte key + 4-byte child pointer).
const ICellSize = 12 // (8+4)

// Size returns the fixed serialised byte size of an ICell (ICellSize = 12).
func (c *ICell) Size() uint64 {
	return ICellSize
}

// Marshal serialises the ICell into buf: 8-byte key then 4-byte child index.
func (c *ICell) Marshal(buf []byte) {
	i := uint64(0)

	buf = marshalUint64(buf, uint64(c.Key), i)
	i += 8

	marshalUint32(buf, uint32(c.Child), i)
}

// Unmarshal deserialises an ICell from buf and returns the number of bytes consumed.
func (c *ICell) Unmarshal(buf []byte) (uint64, error) {
	if uint64(len(buf)) < c.Size() {
		return 0, fmt.Errorf("ICell unmarshal: buffer too short (%d < %d)", len(buf), c.Size())
	}
	i := uint64(0)

	c.Key = RowID(unmarshalUint64(buf, i))
	i += 8

	c.Child = PageIndex(unmarshalUint32(buf, i))
	i += 4

	return c.Size(), nil
}

// InternalNode is a non-leaf page in the B+ tree. It holds separator keys and
// child page pointers: each ICell carries a left child, and the header's
// RightChild field points to the rightmost subtree.
type InternalNode struct {
	Header InternalNodeHeader
	ICells [InternalNodeMaxCells]ICell
}

// Clone returns a deep copy of the internal node with an independent ICell array.
func (n *InternalNode) Clone() *InternalNode {
	nodeCopy := NewInternalNode()
	copy(nodeCopy.ICells[:], n.ICells[:])
	nodeCopy.Header = n.Header
	return nodeCopy
}

// RightChildNotSet is the sentinel value indicating an internal node has no right child yet.
const RightChildNotSet = math.MaxUint32

// NewInternalNode allocates a new internal node with IsInternal set and
// RightChild initialised to RightChildNotSet.
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

// Size returns the total serialised byte size of the internal node
// (header + all ICells in the fixed-size array).
func (n *InternalNode) Size() uint64 {
	size := n.Header.Size()
	for idx := range n.ICells {
		size += n.ICells[idx].Size()
	}
	return size
}

// Marshal serialises the node into buf: header first, then each ICell up to KeysNum.
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

// Unmarshal deserialises the node from buf, reading the header then each ICell,
// and returns the total number of bytes consumed.
func (n *InternalNode) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	if n.Header.KeysNum > InternalNodeMaxCells {
		return 0, fmt.Errorf("internal node unmarshal: KeysNum %d exceeds max %d", n.Header.KeysNum, InternalNodeMaxCells)
	}

	for idx := range n.ICells[0:n.Header.KeysNum] {
		ci, err := n.ICells[idx].Unmarshal(buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}
