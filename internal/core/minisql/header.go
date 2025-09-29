package minisql

type Header struct {
	IsInternal bool
	IsRoot     bool
	Parent     uint32
}

func (h *Header) Size() uint64 {
	return 6
}

func (h *Header) Marshal(buf []byte) ([]byte, error) {
	size := h.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	if h.IsInternal {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	if h.IsRoot {
		buf[1] = 1
	} else {
		buf[1] = 0
	}

	buf[0+2] = byte(h.Parent >> 0)
	buf[1+2] = byte(h.Parent >> 8)
	buf[2+2] = byte(h.Parent >> 16)
	buf[3+2] = byte(h.Parent >> 24)

	return buf[:size], nil
}

func (h *Header) Unmarshal(buf []byte) (uint64, error) {
	h.IsInternal = buf[0] == 1
	h.IsRoot = buf[1] == 1
	h.Parent = 0 |
		(uint32(buf[0+2]) << 0) |
		(uint32(buf[1+2]) << 8) |
		(uint32(buf[2+2]) << 16) |
		(uint32(buf[3+2]) << 24)

	return h.Size(), nil
}
