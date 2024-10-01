package node

type LeafNodeHeader struct {
	Cells    uint32
	NextLeaf uint32
}

func (d *LeafNodeHeader) Size() uint64 {
	return 8
}

func (h *LeafNodeHeader) Marshal(buf []byte) ([]byte, error) {
	size := h.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	buf[0] = byte(h.Cells >> 0)
	buf[1] = byte(h.Cells >> 8)
	buf[2] = byte(h.Cells >> 16)
	buf[3] = byte(h.Cells >> 24)

	buf[4] = byte(h.NextLeaf >> 0)
	buf[5] = byte(h.NextLeaf >> 8)
	buf[6] = byte(h.NextLeaf >> 16)
	buf[7] = byte(h.NextLeaf >> 24)

	return buf[:size], nil
}

func (h *LeafNodeHeader) Unmarshal(buf []byte) (uint64, error) {
	h.Cells = 0 |
		(uint32(buf[0]) << 0) |
		(uint32(buf[1]) << 8) |
		(uint32(buf[2]) << 16) |
		(uint32(buf[3]) << 24)

	h.NextLeaf = 0 |
		(uint32(buf[4]) << 0) |
		(uint32(buf[5]) << 8) |
		(uint32(buf[6]) << 16) |
		(uint32(buf[7]) << 24)

	return h.Size(), nil
}

type Cell struct {
	Key     uint32
	Value   []byte // size of rowSize
	rowSize uint64
}

func (c *Cell) Size() uint64 {
	size := c.rowSize
	size += 4
	return size
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
	i += 4

	copy(buf[i:], c.Value[0:c.rowSize])
	i += c.rowSize

	return buf[:i], nil
}

func (c *Cell) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	c.Key = 0 |
		(uint32(buf[i+0]) << 0) |
		(uint32(buf[i+1]) << 8) |
		(uint32(buf[i+2]) << 16) |
		(uint32(buf[i+3]) << 24)
	i += 4

	copy(c.Value, buf[i:i+c.rowSize])
	i += c.rowSize

	return i, nil
}

type LeafNode struct {
	CommonHeader Header
	Header       LeafNodeHeader
	Cells        []Cell // length of PageSize / rowSize
}

func NewLeafNode(numCells uint32, rowSize uint64) *LeafNode {
	aNode := LeafNode{
		Cells: make([]Cell, numCells),
	}
	for idx := range aNode.Cells {
		aNode.Cells[idx].rowSize = rowSize
		aNode.Cells[idx].Value = make([]byte, rowSize)
	}
	return &aNode
}

func (n *LeafNode) Size() uint64 {
	size := uint64(0)
	size += n.CommonHeader.Size()
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

	chbuf, err := n.CommonHeader.Marshal(buf[i:])
	if err != nil {
		return nil, err
	}
	i += uint64(len(chbuf))

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

	chi, err := n.CommonHeader.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += chi

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
