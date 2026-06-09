package minisql

import (
	"fmt"
)

const (
	// MaxInlineVarchar is the maximum number of bytes stored directly inside a
	// leaf cell before the value is spilled to overflow pages.
	MaxInlineVarchar = 512 // Store up to 512 bytes inline
	// MaxOverflowPageData is the maximum number of data bytes that fit in a
	// single overflow page (page size − type byte − header − 4-byte checksum).
	MaxOverflowPageData = PageSize - 1 - 8 - pageChecksumSize
	// MaxOverflowTextSize limits the maximum size of a text value to 16 overflow pages.
	MaxOverflowTextSize     = MaxOverflowPageData * 16
	varcharLengthPrefixSize = 4
)

// OverflowPageHeader is the on-disk header for a text overflow page. It holds
// the next-page pointer (0 = last page) and the number of data bytes on this page.
type OverflowPageHeader struct {
	NextPage PageIndex // 0 if last page
	DataSize uint32    // Actual data size in this page
}

// Size returns the fixed serialised byte size of the header (type byte + 4 + 4).
func (h *OverflowPageHeader) Size() uint64 {
	return 1 + 4 + 4
}

// OverflowPage holds a chunk of text data that does not fit inline in a leaf
// cell. Pages are chained via Header.NextPage until the full value is read.
type OverflowPage struct {
	Data   []byte
	Header OverflowPageHeader
}

// Size returns the serialised byte size of the overflow page (header + data length).
func (h *OverflowPage) Size() uint64 {
	return h.Header.Size() + uint64(len(h.Data))
}

// Marshal serialises the overflow page into buf: type byte, header, then data.
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

// Unmarshal deserialises the overflow page from buf, validating the type byte.
// Data is sub-sliced directly into the page buffer (zero-copy).
func (h *OverflowPage) Unmarshal(buf []byte) error {
	const minHeaderSize = 1 + 4 + 4 // type byte + NextPage + DataSize
	if len(buf) < minHeaderSize {
		return fmt.Errorf("overflow page unmarshal: buffer too short (%d < %d)", len(buf), minHeaderSize)
	}
	i := uint64(0)

	if buf[i] != PageTypeOverflow {
		return fmt.Errorf("invalid overflow page type byte %d", buf[i])
	}
	i += 1

	h.Header.NextPage = PageIndex(unmarshalUint32(buf, i))
	i += 4

	h.Header.DataSize = unmarshalUint32(buf, i)
	i += 4

	// Sub-slice page buffer directly — zero allocation, zero copy.
	// Callers only read h.Data (readOverflowTexts appends it); nothing mutates it after unmarshal.
	if i+uint64(h.Header.DataSize) > uint64(len(buf)) {
		return fmt.Errorf("overflow page unmarshal: data truncated (need %d bytes at offset %d, have %d)", h.Header.DataSize, i, len(buf))
	}
	h.Data = buf[i : i+uint64(h.Header.DataSize)]

	return nil
}
