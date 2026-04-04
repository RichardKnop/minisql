package minisql

import (
	"fmt"
)

const (
	// MaxInlineVarchar ...
	MaxInlineVarchar = 255 // Store up to 255 bytes inline
	// MaxOverflowPageData ...
	MaxOverflowPageData = 4096 - 1 - 8 // Page size - page type byte - OverflowPageHeader size
	// MaxOverflowTextSize limits the maximum size of a text value to 16 overflow pages.
	MaxOverflowTextSize     = MaxOverflowPageData * 16
	varcharLengthPrefixSize = 4
)

// OverflowPageHeader ...
type OverflowPageHeader struct {
	NextPage PageIndex // 0 if last page
	DataSize uint32    // Actual data size in this page
}

// Size ...
func (h *OverflowPageHeader) Size() uint64 {
	return 1 + 4 + 4
}

// OverflowPage ...
type OverflowPage struct {
	Header OverflowPageHeader
	Data   []byte
}

// Size ...
func (h *OverflowPage) Size() uint64 {
	return h.Header.Size() + uint64(len(h.Data))
}

// Marshal ...
func (h *OverflowPage) Marshal(buf []byte) error {
	i := uint64(0)

	buf[i] = PageTypeOverflow
	i += 1

	marshalUint32(buf, uint32(h.Header.NextPage), i)
	i += 4

	marshalUint32(buf, h.Header.DataSize, i)
	i += 4

	copy(buf[i:], h.Data)

	return nil
}

// Unmarshal ...
func (h *OverflowPage) Unmarshal(buf []byte) error {
	i := uint64(0)

	if buf[i] != PageTypeOverflow {
		return fmt.Errorf("invalid overflow page type byte %d", buf[i])
	}
	i += 1

	h.Header.NextPage = PageIndex(unmarshalUint32(buf, i))
	i += 4

	h.Header.DataSize = unmarshalUint32(buf, i)
	i += 4

	h.Data = make([]byte, h.Header.DataSize)
	copy(h.Data, buf[i:i+uint64(h.Header.DataSize)])

	return nil
}
