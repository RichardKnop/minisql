package minisql

import "math"

type LeafNodeHeader struct {
	Header
	Cells    uint32
	NextLeaf uint32
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
	marshalUint32(buf, h.NextLeaf, i)

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
	h.NextLeaf = unmarshalUint32(buf, i)

	return h.Size(), nil
}

type Cell struct {
	NullBitmask uint64
	Key         uint64
	Value       []byte
}

func (c *Cell) Size(rowSize uint64) uint64 {
	// 8 bytes for null bitmask, 8 bytes for key
	return 8 + 8 + rowSize
}

func (c *Cell) Marshal(rowSize uint64, buf []byte) ([]byte, error) {
	size := c.Size(rowSize)
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	marshalUint64(buf, c.NullBitmask, i)
	i += 8

	marshalUint64(buf, c.Key, i)
	i += 8

	copy(buf[i:], c.Value[0:rowSize])
	i += rowSize

	return buf[:i], nil
}

func (c *Cell) Unmarshal(rowSize uint64, buf []byte) (uint64, error) {
	i := uint64(0)

	c.NullBitmask = unmarshalUint64(buf, i)
	i += 8

	c.Key = unmarshalUint64(buf, i)
	i += 8

	copy(c.Value, buf[i:i+rowSize])
	i += rowSize

	return i, nil
}

type LeafNode struct {
	Header  LeafNodeHeader
	Cells   []Cell // (PageSize - (6+8)) / (rowSize+8+8)
	RowSize uint64
}

func NewCell(rowSize uint64) Cell {
	return Cell{
		Value: make([]byte, rowSize),
	}
}

func NewLeafNode(rowSize uint64, cells ...Cell) *LeafNode {
	aNode := LeafNode{
		Cells:   make([]Cell, 0, maxCells(rowSize)),
		RowSize: rowSize,
	}
	for i := 0; i < int(maxCells(rowSize)); i++ {
		aNode.Cells = append(aNode.Cells, NewCell(aNode.RowSize))
	}
	if len(cells) > 0 {
		aNode.Header.Cells = uint32(len(cells))
		copy(aNode.Cells, cells)
	}
	return &aNode
}

func (n *LeafNode) Size() uint64 {
	size := uint64(0)
	size += n.Header.Size()

	for idx := range n.Cells {
		size += n.Cells[idx].Size(n.RowSize)
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
		cbuf, err := n.Cells[idx].Marshal(n.RowSize, buf[i:])
		if err != nil {
			return nil, err
		}
		i += uint64(len(cbuf))
	}

	return buf[:i], nil
}

func (n *LeafNode) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	for idx := 0; idx < int(n.Header.Cells); idx++ {
		ci, err := n.Cells[idx].Unmarshal(n.RowSize, buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}

func (n *LeafNode) MarshalRoot(buf []byte) ([]byte, error) {
	size := n.Size() - uint64(RootPageConfigSize)
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

	for idx := range n.Cells[len(n.Cells)-int(RootPageConfigSize/n.RowSize+1):] {
		cbuf, err := n.Cells[idx].Marshal(n.RowSize, buf[i:])
		if err != nil {
			return nil, err
		}
		i += uint64(len(cbuf))
	}

	return buf[:i], nil
}

func (n *LeafNode) UnmarshalRoot(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	for idx := range n.Cells[len(n.Cells)-int(RootPageConfigSize/n.RowSize+1):] {
		ci, err := n.Cells[idx].Unmarshal(n.RowSize, buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}

func (n *LeafNode) AtLeastHalfFull() bool {
	return int(n.Header.Cells) >= (len(n.Cells)+1)/2
}

func (n *LeafNode) MoreThanHalfFull() bool {
	return int(n.Header.Cells) > (len(n.Cells)+1)/2
}

func (n *LeafNode) Delete(key uint64) (Cell, bool) {
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
	aCellToDelete := n.Cells[cellIdx]

	for i := int(cellIdx); i < int(n.Header.Cells)-1; i++ {
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[int(n.Header.Cells)-1] = NewCell(n.RowSize)

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
	n.Cells[n.Header.Cells-1] = NewCell(n.RowSize)
	n.Header.Cells -= 1
}

func (n *LeafNode) RemoveFirstCell() {
	for i := 0; i < int(n.Header.Cells)-1; i++ {
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[n.Header.Cells-1] = NewCell(n.RowSize)
	n.Header.Cells -= 1
}

func (n *LeafNode) PrependCell(aCell Cell) {
	for i := int(n.Header.Cells); i > 0; i-- {
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

func (n *LeafNode) Keys() []uint64 {
	keys := make([]uint64, 0, n.Header.Cells)
	for idx := range n.Header.Cells {
		keys = append(keys, n.Cells[idx].Key)
	}
	return keys
}

func marshalUint32(buf []byte, n uint32, i uint64) {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	buf[i+2] = byte(n >> 16)
	buf[i+3] = byte(n >> 24)
}

func unmarshalUint32(buf []byte, i uint64) uint32 {
	return 0 |
		(uint32(buf[i+0]) << 0) |
		(uint32(buf[i+1]) << 8) |
		(uint32(buf[i+2]) << 16) |
		(uint32(buf[i+3]) << 24)
}

func marshalUint64(buf []byte, n, i uint64) {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	buf[i+2] = byte(n >> 16)
	buf[i+3] = byte(n >> 24)
	buf[i+4] = byte(n >> 32)
	buf[i+5] = byte(n >> 40)
	buf[i+6] = byte(n >> 48)
	buf[i+7] = byte(n >> 56)
}

func unmarshalUint64(buf []byte, i uint64) uint64 {
	return 0 | (uint64(buf[i+0]) << 0) |
		(uint64(buf[i+1]) << 8) |
		(uint64(buf[i+2]) << 16) |
		(uint64(buf[i+3]) << 24) |
		(uint64(buf[i+4]) << 32) |
		(uint64(buf[i+5]) << 40) |
		(uint64(buf[i+6]) << 48) |
		(uint64(buf[i+7]) << 56)
}

func marshalInt32(buf []byte, n int32, i uint64) {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	buf[i+2] = byte(n >> 16)
	buf[i+3] = byte(n >> 24)
}

func unmarshalInt32(buf []byte, i uint64) int32 {
	return 0 |
		(int32(buf[i+0]) << 0) |
		(int32(buf[i+1]) << 8) |
		(int32(buf[i+2]) << 16) |
		(int32(buf[i+3]) << 24)
}

func marshalInt64(buf []byte, n int64, i uint64) {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	buf[i+2] = byte(n >> 16)
	buf[i+3] = byte(n >> 24)
	buf[i+4] = byte(n >> 32)
	buf[i+5] = byte(n >> 40)
	buf[i+6] = byte(n >> 48)
	buf[i+7] = byte(n >> 56)
}

func unmarshalInt64(buf []byte, i uint64) int64 {
	return 0 |
		(int64(buf[i+0]) << 0) |
		(int64(buf[i+1]) << 8) |
		(int64(buf[i+2]) << 16) |
		(int64(buf[i+3]) << 24) |
		(int64(buf[i+4]) << 32) |
		(int64(buf[i+5]) << 40) |
		(int64(buf[i+6]) << 48) |
		(int64(buf[i+7]) << 56)
}

func marshalFloat32(buf []byte, n float32, i uint64) {
	bits := math.Float32bits(n)
	buf[i+0] = byte(bits >> 24)
	buf[i+1] = byte(bits >> 16)
	buf[i+2] = byte(bits >> 8)
	buf[i+3] = byte(bits >> 0)
}

func unmarshalFloat32(buf []byte, i uint64) float32 {
	return math.Float32frombits(0 |
		(uint32(buf[i+0]) << 24) |
		(uint32(buf[i+1]) << 16) |
		(uint32(buf[i+2]) << 8) |
		(uint32(buf[i+3]) << 0))
}

func marshalFloat64(buf []byte, n float64, i uint64) {
	bits := math.Float64bits(n)
	buf[i+0] = byte(bits >> 56)
	buf[i+1] = byte(bits >> 48)
	buf[i+2] = byte(bits >> 40)
	buf[i+3] = byte(bits >> 32)
	buf[i+4] = byte(bits >> 24)
	buf[i+5] = byte(bits >> 16)
	buf[i+6] = byte(bits >> 8)
	buf[i+7] = byte(bits >> 0)
}

func unmarshalFloat64(buf []byte, i uint64) float64 {
	return math.Float64frombits(0 |
		(uint64(buf[i+0]) << 56) |
		(uint64(buf[i+1+0]) << 48) |
		(uint64(buf[i+2+0]) << 40) |
		(uint64(buf[i+3+0]) << 32) |
		(uint64(buf[i+4+0]) << 24) |
		(uint64(buf[i+5+0]) << 16) |
		(uint64(buf[i+6+0]) << 8) |
		(uint64(buf[i+7+0]) << 0))
}
