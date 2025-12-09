package minisql

import (
	"fmt"
)

const (
	MaxInlineVarchar    = 255          // Store up to 255 bytes inline
	MaxOverflowPageData = 4096 - 1 - 8 // Page size - page type byte - OverflowPageHeader size
	// Limit maximum size of a text to 16 overflow pages
	MaxOverflowTextSize     = MaxOverflowPageData * 16
	varcharLengthPrefixSize = 4
)

type OverflowPageHeader struct {
	NextPage PageIndex // 0 if last page
	DataSize uint32    // Actual data size in this page
}

func (h *OverflowPageHeader) Size() uint64 {
	return 1 + 4 + 4
}

type OverflowPage struct {
	Header OverflowPageHeader
	Data   []byte
}

func (h *OverflowPage) Size() uint64 {
	return h.Header.Size() + uint64(len(h.Data))
}

func (n *OverflowPage) Marshal(buf []byte) ([]byte, error) {
	size := n.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	buf[i] = PageTypeOverflow
	i += 1

	marshalUint32(buf, uint32(n.Header.NextPage), i)
	i += 4

	marshalUint32(buf, n.Header.DataSize, i)
	i += 4

	copy(buf[i:], n.Data)
	i += uint64(len(n.Data))

	return buf, nil
}

func (n *OverflowPage) Unmarshal(buf []byte) error {
	i := uint64(0)

	if buf[i] != PageTypeOverflow {
		return fmt.Errorf("invalid overflow page type byte %d", buf[i])
	}
	i += 1

	n.Header.NextPage = PageIndex(unmarshalUint32(buf, i))
	i += 4

	n.Header.DataSize = unmarshalUint32(buf, i)
	i += 4

	n.Data = make([]byte, n.Header.DataSize)
	copy(n.Data, buf[i:i+uint64(n.Header.DataSize)])
	i += uint64(n.Header.DataSize)

	return nil
}
