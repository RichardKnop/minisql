package minisql

import (
	"fmt"
)

// Header is the common 6-byte prefix shared by every leaf and internal B+ tree
// page. It records the page type (leaf vs internal), whether this page is the
// B+ tree root, and the parent page index.
type Header struct {
	IsInternal bool
	IsRoot     bool
	Parent     PageIndex
}

// Size returns the fixed serialised byte length of a Header (6 bytes: type + root flag + parent).
func (h *Header) Size() uint64 {
	return 1 + 1 + 4
}

// Marshal writes the header fields into buf starting at offset 0.
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

// Unmarshal reads the header fields from buf and returns the number of bytes consumed.
func (h *Header) Unmarshal(buf []byte) (uint64, error) {
	if uint64(len(buf)) < h.Size() {
		return 0, fmt.Errorf("header unmarshal: buffer too short (%d < %d)", len(buf), h.Size())
	}
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
