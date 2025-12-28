package minisql

import (
	"github.com/RichardKnop/minisql/pkg/bitwise"
)

type LeafNodeHeader struct {
	Header
	Cells    uint32
	NextLeaf PageIndex
}

func (h *LeafNodeHeader) Size() uint64 {
	return h.Header.Size() + 8
}

func (h *LeafNodeHeader) Marshal(buf []byte) ([]byte, error) {
	size := h.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	hbuf, err := h.Header.Marshal(buf[i:])
	if err != nil {
		return nil, err
	}
	i += uint64(len(hbuf))

	marshalUint32(buf, h.Cells, i)
	i += 4
	marshalUint32(buf, uint32(h.NextLeaf), i)

	return buf[:size], nil
}

func (h *LeafNodeHeader) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := h.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	h.Cells = unmarshalUint32(buf, i)
	i += 4
	h.NextLeaf = PageIndex(unmarshalUint32(buf, i))

	return h.Size(), nil
}

type Cell struct {
	NullBitmask uint64
	Key         RowID
	Value       []byte
	// Tracks if this cell owns its Value slice, cells are shared until
	// they are modified ( copy-on-write )
	isOwned bool
}

func (c *Cell) Size() uint64 {
	// 8 bytes for null bitmask, 8 bytes for key
	return 8 + 8 + uint64(len(c.Value))
}

func (c *Cell) Marshal(buf []byte) ([]byte, error) {
	size := c.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	marshalUint64(buf, c.NullBitmask, i)
	i += 8

	marshalUint64(buf, uint64(c.Key), i)
	i += 8

	copy(buf[i:], c.Value)
	i += uint64(len(c.Value))

	return buf[:i], nil
}

func (c *Cell) Unmarshal(columns []Column, buf []byte) (uint64, error) {
	offset := uint64(0)

	c.NullBitmask = unmarshalUint64(buf, offset)
	offset += 8

	c.Key = RowID(unmarshalUint64(buf, offset))
	offset += 8

	for i, aColumn := range columns {
		if bitwise.IsSet(c.NullBitmask, i) {
			continue
		}
		if aColumn.Kind.IsText() {
			size := unmarshalInt32(buf, offset)
			val := make([]byte, size+4)
			n := copy(val, buf[offset:offset+4+uint64(size)])
			offset += uint64(n)
			c.Value = append(c.Value, val...)
		} else {
			val := make([]byte, aColumn.Size)
			n := copy(val, buf[offset:offset+uint64(aColumn.Size)])
			offset += uint64(n)
			c.Value = append(c.Value, val...)
		}
	}

	return offset, nil
}

type LeafNode struct {
	Header LeafNodeHeader
	Cells  []Cell
}

// Clone cretes a shallow copy of the leaf node, sharing value slices
// until they are about to be modified at which point PrepareModifyCell
// should be called to clone the value slice for that cell.
func (n *LeafNode) Clone() *LeafNode {
	aCopy := &LeafNode{
		Header: n.Header,
		Cells:  make([]Cell, len(n.Cells)),
	}

	// Shallow copy - share Value slices
	for i := range n.Cells {
		aCopy.Cells[i] = Cell{
			NullBitmask: n.Cells[i].NullBitmask,
			Key:         n.Cells[i].Key,
			Value:       n.Cells[i].Value, // Share the slice!
			isOwned:     false,            // Mark as shared
		}
	}
	return aCopy
}

func (n *LeafNode) DeepClone() *LeafNode {
	aCopy := &LeafNode{
		Header: n.Header,
		Cells:  make([]Cell, 0, len(n.Cells)),
	}
	aCopy.Cells = append(aCopy.Cells, n.Cells...)
	for i := range n.Cells {
		aCopy.Cells[i] = Cell{
			NullBitmask: n.Cells[i].NullBitmask,
			Key:         n.Cells[i].Key,
			Value:       make([]byte, len(n.Cells[i].Value)),
			isOwned:     true, // Mark as owned
		}
		copy(aCopy.Cells[i].Value, n.Cells[i].Value)
	}
	return aCopy
}

// Before modifying a cell:
func (n *LeafNode) PrepareModifyCell(idx uint32) {
	if n.Cells[idx].isOwned {
		return
	}
	// Clone the Value slice on first write
	oldValue := n.Cells[idx].Value
	n.Cells[idx].Value = make([]byte, len(oldValue))
	copy(n.Cells[idx].Value, oldValue)
	n.Cells[idx].isOwned = true
}

func NewLeafNode(cells ...Cell) *LeafNode {
	aNode := LeafNode{
		Cells: make([]Cell, 0, len(cells)),
	}
	if len(cells) > 0 {
		aNode.Header.Cells = uint32(len(cells))
		aNode.Cells = append(aNode.Cells, cells...)
	}
	return &aNode
}

func (n *LeafNode) Size() uint64 {
	size := uint64(0)
	size += n.Header.Size()

	for idx := range n.Cells {
		size += n.Cells[idx].Size()
	}

	return size
}

func (n *LeafNode) Marshal(buf []byte) ([]byte, error) {
	size := n.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	hbuf, err := n.Header.Marshal(buf[i:])
	if err != nil {
		return nil, err
	}
	i += uint64(len(hbuf))

	for idx := range n.Cells {
		cbuf, err := n.Cells[idx].Marshal(buf[i:])
		if err != nil {
			return nil, err
		}
		i += uint64(len(cbuf))
	}

	return buf[:i], nil
}

func (n *LeafNode) Unmarshal(columns []Column, buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	for idx := 0; idx < int(n.Header.Cells); idx++ {
		if len(n.Cells) == idx {
			n.Cells = append(n.Cells, Cell{})
		}
		ci, err := n.Cells[idx].Unmarshal(columns, buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}

func (n *LeafNode) Delete(key RowID) (Cell, bool) {
	if n.Header.Cells == 0 {
		return Cell{}, false
	}

	cellIdx := -1
	for i := 0; i < int(n.Header.Cells); i++ {
		if n.Cells[i].Key == key {
			cellIdx = i
			break
		}
	}

	if cellIdx < 0 {
		return Cell{}, false
	}

	n.PrepareModifyCell(uint32(cellIdx))
	aCellToDelete := n.Cells[cellIdx]

	for i := uint32(cellIdx); i < n.Header.Cells-1; i++ {
		n.PrepareModifyCell(i + 1)
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[int(n.Header.Cells)-1] = Cell{}

	n.Header.Cells -= 1

	return aCellToDelete, true
}

func (n *LeafNode) FirstCell() Cell {
	return n.Cells[0]
}

func (n *LeafNode) LastCell() Cell {
	return n.Cells[n.Header.Cells-1]
}

func (n *LeafNode) RemoveLastCell() {
	n.Cells[n.Header.Cells-1] = Cell{}
	n.Header.Cells -= 1
}

func (n *LeafNode) RemoveFirstCell() {
	for i := uint32(0); i < n.Header.Cells-1; i++ {
		n.PrepareModifyCell(i + 1)
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[n.Header.Cells-1] = Cell{}
	n.Header.Cells -= 1
}

func (n *LeafNode) PrependCell(aCell Cell) {
	for i := n.Header.Cells; i > 0; i-- {
		n.PrepareModifyCell(i - 1)
		n.Cells[i] = n.Cells[i-1]
	}
	n.Cells[0] = aCell
	n.Header.Cells += 1
}

func (n *LeafNode) AppendCells(cells ...Cell) {
	for _, aCell := range cells {
		n.Cells[n.Header.Cells] = aCell
		n.Header.Cells += 1
	}
}

func (n *LeafNode) Keys() []RowID {
	keys := make([]RowID, 0, n.Header.Cells)
	for idx := range n.Header.Cells {
		keys = append(keys, n.Cells[idx].Key)
	}
	return keys
}

func (n *LeafNode) MaxSpace() uint64 {
	maxSpace := PageSize - headerSize()
	if n.Header.IsRoot {
		maxSpace = PageSize - rootHeaderSize()
	}
	return maxSpace
}

func (n *LeafNode) TakenSpace() uint64 {
	takenPageSize := uint64(0)
	for i := uint32(0); i < n.Header.Cells; i++ {
		takenPageSize += n.Cells[i].Size()
	}
	return takenPageSize
}

func (n *LeafNode) AvailableSpace() uint64 {
	return n.MaxSpace() - n.TakenSpace()
}

func (n *LeafNode) HasSpaceForRow(aRow Row) bool {
	return aRow.Size()+8+8 <= n.AvailableSpace()
}

func (n *LeafNode) AtLeastHalfFull() bool {
	return n.AvailableSpace() < n.MaxSpace()/2
}

func (n *LeafNode) CanMergeWith(n2 *LeafNode) bool {
	return n2.TakenSpace() <= n.AvailableSpace()
}

func (n *LeafNode) CanBorrowFirst() bool {
	firstCellSize := n.Cells[0].Size()
	return n.AvailableSpace()+firstCellSize < n.MaxSpace()/2
}

func (n *LeafNode) CanBorrowLast() bool {
	lastCellSize := n.Cells[n.Header.Cells-1].Size()
	return n.AvailableSpace()+lastCellSize < n.MaxSpace()/2
}
