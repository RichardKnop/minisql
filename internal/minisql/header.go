package minisql

import (
	"fmt"
)

type Header struct {
	IsInternal bool
	IsRoot     bool
	Parent     PageIndex
}

func (h *Header) Size() uint64 {
	return 1 + 1 + 4
}

func (h *Header) Marshal(buf []byte) {
	i := uint64(0)
	if h.IsInternal {
		buf[i] = PageTypeInternal
	} else {
		buf[i] = PageTypeLeaf
	}
	i += 1

	if h.IsRoot {
		buf[i] = 1
	} else {
		buf[i] = 0
	}
	i += 1

	buf[i] = byte(h.Parent >> 0)
	buf[i+1] = byte(h.Parent >> 8)
	buf[i+2] = byte(h.Parent >> 16)
	buf[i+3] = byte(h.Parent >> 24)
	i += 4
}

func (h *Header) Unmarshal(buf []byte) (uint64, error) {
	if buf[0] != PageTypeLeaf && buf[0] != PageTypeInternal {
		return 0, fmt.Errorf("unrecognised page type byte %d", buf[0])
	}
	h.IsInternal = buf[0] == PageTypeInternal
	h.IsRoot = buf[1] == 1
	h.Parent = PageIndex(0 |
		(uint32(buf[0+2]) << 0) |
		(uint32(buf[1+2]) << 8) |
		(uint32(buf[2+2]) << 16) |
		(uint32(buf[3+2]) << 24))

	return h.Size(), nil
}
