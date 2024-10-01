package node

const (
	InternalNodeMaxCells = 510
)

type InternalNodeHeader struct {
	Header
	KeysNum    uint32
	RightChild uint32
}

func (h *InternalNodeHeader) Size() (s uint64) {
	return h.Header.Size() + 8
}

func (h *InternalNodeHeader) Marshal(buf []byte) ([]byte, error) {
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

	buf[i+0] = byte(h.KeysNum >> 0)
	buf[i+1] = byte(h.KeysNum >> 8)
	buf[i+2] = byte(h.KeysNum >> 16)
	buf[i+3] = byte(h.KeysNum >> 24)

	buf[i+4] = byte(h.RightChild >> 0)
	buf[i+5] = byte(h.RightChild >> 8)
	buf[i+6] = byte(h.RightChild >> 16)
	buf[i+7] = byte(h.RightChild >> 24)

	return buf[:size], nil
}

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

	h.RightChild = 0 |
		(uint32(buf[i+4]) << 0) |
		(uint32(buf[i+5]) << 8) |
		(uint32(buf[i+6]) << 16) |
		(uint32(buf[i+7]) << 24)

	return h.Size(), nil
}

type ICell struct {
	Key   uint32
	Child uint32
}

func (c *ICell) Size() uint64 {
	return 8
}

func (c *ICell) Marshal(buf []byte) ([]byte, error) {
	size := c.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	buf[0] = byte(c.Key >> 0)
	buf[1] = byte(c.Key >> 8)
	buf[2] = byte(c.Key >> 16)
	buf[3] = byte(c.Key >> 24)

	buf[4] = byte(c.Child >> 0)
	buf[5] = byte(c.Child >> 8)
	buf[6] = byte(c.Child >> 16)
	buf[7] = byte(c.Child >> 24)

	return buf[:size], nil
}

func (c *ICell) Unmarshal(buf []byte) (uint64, error) {
	c.Key = 0 |
		(uint32(buf[0]) << 0) |
		(uint32(buf[1]) << 8) |
		(uint32(buf[2]) << 16) |
		(uint32(buf[3]) << 24)

	c.Child = 0 |
		(uint32(buf[4]) << 0) |
		(uint32(buf[5]) << 8) |
		(uint32(buf[6]) << 16) |
		(uint32(buf[7]) << 24)

	return c.Size(), nil
}

type InternalNode struct {
	Header InternalNodeHeader
	ICells [InternalNodeMaxCells]ICell
}

func NewInternalNode() *InternalNode {
	aNode := InternalNode{
		Header: InternalNodeHeader{
			Header: Header{
				IsInternal: true,
			},
		},
	}
	return &aNode
}

func (n *InternalNode) Size() uint64 {
	size := n.Header.Size()
	for idx := range n.ICells {
		size += n.ICells[idx].Size()
	}
	return size
}

func (n *InternalNode) Marshal(buf []byte) ([]byte, error) {
	size := n.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	hbuf, err := n.Header.Marshal(buf[i+0:])
	if err != nil {
		return nil, err
	}
	i += uint64(len(hbuf))

	for idx := range n.ICells {
		icbuf, err := n.ICells[idx].Marshal(buf[i:])
		if err != nil {
			return nil, err
		}
		i += uint64(len(icbuf))
	}

	return buf[:i], nil
}

func (n *InternalNode) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	for idx := range n.ICells {
		ci, err := n.ICells[idx].Unmarshal(buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}
