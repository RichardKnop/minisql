package minisql

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

	buf[i+0] = byte(h.Cells >> 0)
	buf[i+1] = byte(h.Cells >> 8)
	buf[i+2] = byte(h.Cells >> 16)
	buf[i+3] = byte(h.Cells >> 24)

	buf[i+4] = byte(h.NextLeaf >> 0)
	buf[i+5] = byte(h.NextLeaf >> 8)
	buf[i+6] = byte(h.NextLeaf >> 16)
	buf[i+7] = byte(h.NextLeaf >> 24)

	return buf[:size], nil
}

func (h *LeafNodeHeader) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := h.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	h.Cells = 0 |
		(uint32(buf[i+0]) << 0) |
		(uint32(buf[i+1]) << 8) |
		(uint32(buf[i+2]) << 16) |
		(uint32(buf[i+3]) << 24)

	h.NextLeaf = 0 |
		(uint32(buf[i+4]) << 0) |
		(uint32(buf[i+5]) << 8) |
		(uint32(buf[i+6]) << 16) |
		(uint32(buf[i+7]) << 24)

	return h.Size(), nil
}

type Cell struct {
	Key     uint64
	Value   []byte // size of rowSize
	RowSize uint64
}

func (c *Cell) Size() uint64 {
	return 8 + c.RowSize
}

func (c *Cell) Marshal(buf []byte) ([]byte, error) {
	size := c.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	buf[0] = byte(c.Key >> 0)
	buf[1] = byte(c.Key >> 8)
	buf[2] = byte(c.Key >> 16)
	buf[3] = byte(c.Key >> 24)
	buf[4] = byte(c.Key >> 32)
	buf[5] = byte(c.Key >> 40)
	buf[6] = byte(c.Key >> 48)
	buf[7] = byte(c.Key >> 56)
	i += 8

	copy(buf[i:], c.Value[0:c.RowSize])
	i += c.RowSize

	return buf[:i], nil
}

func (c *Cell) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	c.Key = 0 |
		(uint64(buf[i+0]) << 0) |
		(uint64(buf[i+1]) << 8) |
		(uint64(buf[i+2]) << 16) |
		(uint64(buf[i+3]) << 24) |
		(uint64(buf[i+4]) << 32) |
		(uint64(buf[i+5]) << 40) |
		(uint64(buf[i+6]) << 48) |
		(uint64(buf[i+7]) << 56)
	i += 8

	copy(c.Value, buf[i:i+c.RowSize])
	i += c.RowSize

	return i, nil
}

type LeafNode struct {
	Header LeafNodeHeader
	Cells  []Cell // (PageSize - (6+8)) / (rowSize+8)
}

func NewLeafNode(rowSize uint64) *LeafNode {
	aNode := LeafNode{
		Cells: make([]Cell, maxCells(rowSize)),
	}
	for idx := range aNode.Cells {
		aNode.Cells[idx].RowSize = rowSize
		aNode.Cells[idx].Value = make([]byte, rowSize)
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

func (n *LeafNode) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	for idx := 0; idx < int(n.Header.Cells); idx++ {
		ci, err := n.Cells[idx].Unmarshal(buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}
