package minisql

import "fmt"

const (
	invertedPageFormatVersion byte = 1

	invertedPostingKindInline byte = 1
	invertedPostingKindTree   byte = 2
)

type invertedEntryPageHeader struct {
	Parent        PageIndex
	RightChild    PageIndex
	NextLeaf      PageIndex
	KeyCount      uint16
	FreeStart     uint16
	FreeEnd       uint16
	FormatVersion byte
	IsLeaf        bool
}

func (h invertedEntryPageHeader) size() uint64 {
	return 1 + 1 + 1 + 2 + 2 + 2 + 4 + 4 + 4
}

// Marshal writes the versioned entry-page header used by the future inverted
// entry tree. The page type byte is part of the header contract.
func (h invertedEntryPageHeader) Marshal(buf []byte) error {
	if len(buf) < int(h.size()) {
		return fmt.Errorf("inverted entry page header buffer too small")
	}
	i := uint64(0)
	buf[i] = PageTypeInvertedEntry
	i += 1
	buf[i] = h.FormatVersion
	i += 1
	buf = marshalBool(buf, h.IsLeaf, i)
	i += 1
	marshalUint16(buf, h.KeyCount, i)
	i += 2
	marshalUint16(buf, h.FreeStart, i)
	i += 2
	marshalUint16(buf, h.FreeEnd, i)
	i += 2
	marshalUint32(buf, uint32(h.Parent), i)
	i += 4
	marshalUint32(buf, uint32(h.RightChild), i)
	i += 4
	marshalUint32(buf, uint32(h.NextLeaf), i)
	return nil
}

// Unmarshal decodes and validates an inverted entry-page header.
func (h *invertedEntryPageHeader) Unmarshal(buf []byte) error {
	if len(buf) < int(h.size()) {
		return fmt.Errorf("inverted entry page header buffer too small")
	}
	i := uint64(0)
	if buf[i] != PageTypeInvertedEntry {
		return fmt.Errorf("unmarshal inverted entry page: invalid page type %d", buf[i])
	}
	i += 1
	h.FormatVersion = buf[i]
	if h.FormatVersion != invertedPageFormatVersion {
		return fmt.Errorf("unmarshal inverted entry page: unsupported format version %d", h.FormatVersion)
	}
	i += 1
	h.IsLeaf = unmarshalBool(buf, i)
	i += 1
	h.KeyCount = unmarshalUint16(buf, i)
	i += 2
	h.FreeStart = unmarshalUint16(buf, i)
	i += 2
	h.FreeEnd = unmarshalUint16(buf, i)
	i += 2
	h.Parent = PageIndex(unmarshalUint32(buf, i))
	i += 4
	h.RightChild = PageIndex(unmarshalUint32(buf, i))
	i += 4
	h.NextLeaf = PageIndex(unmarshalUint32(buf, i))
	return nil
}

type invertedEntryCell struct {
	Term         string
	Payload      []byte
	DocFreq      uint32
	PostingCount uint32
	Child        PageIndex
	PostingKind  byte
	CodecVersion byte
}

type invertedEntryPage struct {
	Header invertedEntryPageHeader
	Cells  []invertedEntryCell
}

// NewInvertedEntryPage creates an empty v1 slotted entry page. Leaf pages store
// term entries; internal pages will use the same cell format for routing.
func NewInvertedEntryPage(isLeaf bool) *invertedEntryPage {
	header := invertedEntryPageHeader{
		FormatVersion: invertedPageFormatVersion,
		IsLeaf:        isLeaf,
		FreeStart:     uint16((invertedEntryPageHeader{}).size()),
		FreeEnd:       PageSize,
	}
	return &invertedEntryPage{Header: header}
}

// Clone returns a deep copy of the entry page and its variable-length payloads.
func (p *invertedEntryPage) Clone() *invertedEntryPage {
	if p == nil {
		return nil
	}
	clone := &invertedEntryPage{
		Header: p.Header,
		Cells:  make([]invertedEntryCell, len(p.Cells)),
	}
	for i, cell := range p.Cells {
		clone.Cells[i] = cell
		clone.Cells[i].Payload = append([]byte(nil), cell.Payload...)
	}
	return clone
}

// Marshal writes the entry page as a slotted page: fixed header, slot directory,
// and variable-length cells packed backward from the end of the 4 KiB page.
func (p *invertedEntryPage) Marshal(buf []byte) error {
	if len(buf) < int(p.Header.size()) {
		return fmt.Errorf("inverted entry page buffer too small")
	}
	header := p.Header
	header.FormatVersion = invertedPageFormatVersion
	header.KeyCount = uint16(len(p.Cells))
	header.FreeStart = uint16(header.size() + uint64(len(p.Cells))*2)

	freeEnd := len(buf)
	slotBase := header.size()
	for i, cell := range p.Cells {
		cellBuf := make([]byte, cell.size())
		if err := cell.Marshal(cellBuf); err != nil {
			return err
		}
		if freeEnd-len(cellBuf) < int(header.FreeStart) {
			return errInvertedIndexEntryPageFull
		}
		freeEnd -= len(cellBuf)
		copy(buf[freeEnd:freeEnd+len(cellBuf)], cellBuf)
		marshalUint16(buf, uint16(freeEnd), slotBase+uint64(i)*2)
	}
	header.FreeEnd = uint16(freeEnd)
	if err := header.Marshal(buf); err != nil {
		return err
	}
	return nil
}

// Unmarshal decodes a slotted entry page and reconstructs cells using the slot
// directory written by Marshal.
func (p *invertedEntryPage) Unmarshal(buf []byte) error {
	var header invertedEntryPageHeader
	if err := header.Unmarshal(buf); err != nil {
		return err
	}
	slotBase := header.size()
	cells := make([]invertedEntryCell, 0, header.KeyCount)
	for i := uint16(0); i < header.KeyCount; i++ {
		offset := uint64(unmarshalUint16(buf, slotBase+uint64(i)*2))
		if offset >= uint64(len(buf)) {
			return fmt.Errorf("inverted entry page slot %d out of range", i)
		}
		var cell invertedEntryCell
		if _, err := cell.Unmarshal(buf[offset:]); err != nil {
			return err
		}
		cells = append(cells, cell)
	}
	p.Header = header
	p.Cells = cells
	return nil
}

func (c invertedEntryCell) size() uint64 {
	return 2 + uint64(len([]byte(c.Term))) + 1 + 1 + 4 + 4 + 4 + 4 + uint64(len(c.Payload))
}

// Marshal serializes one entry-tree cell containing a term and either inline
// posting payload bytes or a posting-tree root reference in Payload.
func (c invertedEntryCell) Marshal(buf []byte) error {
	if len([]byte(c.Term)) > MaxIndexKeySize {
		return fmt.Errorf("inverted entry term exceeds max index key size %d", MaxIndexKeySize)
	}
	if len(buf) < int(c.size()) {
		return fmt.Errorf("inverted entry cell buffer too small")
	}
	i := uint64(0)
	marshalUint16(buf, uint16(len([]byte(c.Term))), i)
	i += 2
	copy(buf[i:i+uint64(len([]byte(c.Term)))], []byte(c.Term))
	i += uint64(len([]byte(c.Term)))
	buf[i] = c.PostingKind
	i += 1
	buf[i] = c.CodecVersion
	i += 1
	marshalUint32(buf, c.DocFreq, i)
	i += 4
	marshalUint32(buf, c.PostingCount, i)
	i += 4
	marshalUint32(buf, uint32(c.Child), i)
	i += 4
	marshalUint32(buf, uint32(len(c.Payload)), i)
	i += 4
	copy(buf[i:i+uint64(len(c.Payload))], c.Payload)
	return nil
}

// Unmarshal decodes one inverted entry-tree cell from buf and returns the number
// of bytes consumed.
func (c *invertedEntryCell) Unmarshal(buf []byte) (uint64, error) {
	if len(buf) < 20 {
		return 0, fmt.Errorf("inverted entry cell buffer too small")
	}
	i := uint64(0)
	termLen := uint64(unmarshalUint16(buf, i))
	i += 2
	if len(buf) < int(i+termLen+18) {
		return 0, fmt.Errorf("inverted entry cell truncated")
	}
	c.Term = string(buf[i : i+termLen])
	i += termLen
	c.PostingKind = buf[i]
	i += 1
	c.CodecVersion = buf[i]
	i += 1
	c.DocFreq = unmarshalUint32(buf, i)
	i += 4
	c.PostingCount = unmarshalUint32(buf, i)
	i += 4
	c.Child = PageIndex(unmarshalUint32(buf, i))
	i += 4
	payloadLen := uint64(unmarshalUint32(buf, i))
	i += 4
	if len(buf) < int(i+payloadLen) {
		return 0, fmt.Errorf("inverted entry payload truncated")
	}
	c.Payload = append(c.Payload[:0], buf[i:i+payloadLen]...)
	i += payloadLen
	return i, nil
}

type invertedPostingPageHeader struct {
	Parent        PageIndex
	RightChild    PageIndex
	NextLeaf      PageIndex
	ItemCount     uint16
	FreeStart     uint16
	FreeEnd       uint16
	FormatVersion byte
	Level         byte
}

func (h invertedPostingPageHeader) size() uint64 {
	return 1 + 1 + 1 + 2 + 2 + 2 + 4 + 4 + 4
}

// Marshal writes the versioned posting-tree page header. Level 0 represents a
// leaf page; higher levels are reserved for internal posting-tree pages.
func (h invertedPostingPageHeader) Marshal(buf []byte) error {
	if len(buf) < int(h.size()) {
		return fmt.Errorf("inverted posting page header buffer too small")
	}
	i := uint64(0)
	buf[i] = PageTypeInvertedPosting
	i += 1
	buf[i] = h.FormatVersion
	i += 1
	buf[i] = h.Level
	i += 1
	marshalUint16(buf, h.ItemCount, i)
	i += 2
	marshalUint16(buf, h.FreeStart, i)
	i += 2
	marshalUint16(buf, h.FreeEnd, i)
	i += 2
	marshalUint32(buf, uint32(h.Parent), i)
	i += 4
	marshalUint32(buf, uint32(h.RightChild), i)
	i += 4
	marshalUint32(buf, uint32(h.NextLeaf), i)
	return nil
}

// Unmarshal decodes and validates an inverted posting-tree page header.
func (h *invertedPostingPageHeader) Unmarshal(buf []byte) error {
	if len(buf) < int(h.size()) {
		return fmt.Errorf("inverted posting page header buffer too small")
	}
	i := uint64(0)
	if buf[i] != PageTypeInvertedPosting {
		return fmt.Errorf("unmarshal inverted posting page: invalid page type %d", buf[i])
	}
	i += 1
	h.FormatVersion = buf[i]
	if h.FormatVersion != invertedPageFormatVersion {
		return fmt.Errorf("unmarshal inverted posting page: unsupported format version %d", h.FormatVersion)
	}
	i += 1
	h.Level = buf[i]
	i += 1
	h.ItemCount = unmarshalUint16(buf, i)
	i += 2
	h.FreeStart = unmarshalUint16(buf, i)
	i += 2
	h.FreeEnd = unmarshalUint16(buf, i)
	i += 2
	h.Parent = PageIndex(unmarshalUint32(buf, i))
	i += 4
	h.RightChild = PageIndex(unmarshalUint32(buf, i))
	i += 4
	h.NextLeaf = PageIndex(unmarshalUint32(buf, i))
	return nil
}

type invertedPostingBlock struct {
	Payload      []byte
	FirstRowID   RowID
	LastRowID    RowID
	PostingCount uint32
	Child        PageIndex
	CodecVersion byte
}

type invertedPostingPage struct {
	Header invertedPostingPageHeader
	Blocks []invertedPostingBlock
}

// NewInvertedPostingPage creates an empty v1 slotted posting page. Level 0 is a
// leaf block page; higher levels are reserved for posting-tree routing pages.
func NewInvertedPostingPage(level byte) *invertedPostingPage {
	header := invertedPostingPageHeader{
		FormatVersion: invertedPageFormatVersion,
		Level:         level,
		FreeStart:     uint16((invertedPostingPageHeader{}).size()),
		FreeEnd:       PageSize,
	}
	return &invertedPostingPage{Header: header}
}

// Clone returns a deep copy of the posting page and its compressed block payloads.
func (p *invertedPostingPage) Clone() *invertedPostingPage {
	if p == nil {
		return nil
	}
	clone := &invertedPostingPage{
		Header: p.Header,
		Blocks: make([]invertedPostingBlock, len(p.Blocks)),
	}
	for i, block := range p.Blocks {
		clone.Blocks[i] = block
		clone.Blocks[i].Payload = append([]byte(nil), block.Payload...)
	}
	return clone
}

// Marshal writes posting blocks into a slotted 4 KiB page, preserving room for
// future variable-size compressed blocks and internal routing records.
func (p *invertedPostingPage) Marshal(buf []byte) error {
	if len(buf) < int(p.Header.size()) {
		return fmt.Errorf("inverted posting page buffer too small")
	}
	header := p.Header
	header.FormatVersion = invertedPageFormatVersion
	header.ItemCount = uint16(len(p.Blocks))
	header.FreeStart = uint16(header.size() + uint64(len(p.Blocks))*2)

	freeEnd := len(buf)
	slotBase := header.size()
	for i, block := range p.Blocks {
		blockBuf := make([]byte, block.size())
		if err := block.Marshal(blockBuf); err != nil {
			return err
		}
		if freeEnd-len(blockBuf) < int(header.FreeStart) {
			return fmt.Errorf("inverted posting page has insufficient free space")
		}
		freeEnd -= len(blockBuf)
		copy(buf[freeEnd:freeEnd+len(blockBuf)], blockBuf)
		marshalUint16(buf, uint16(freeEnd), slotBase+uint64(i)*2)
	}
	header.FreeEnd = uint16(freeEnd)
	if err := header.Marshal(buf); err != nil {
		return err
	}
	return nil
}

// Unmarshal decodes a slotted posting page and reconstructs posting blocks from
// the slot directory.
func (p *invertedPostingPage) Unmarshal(buf []byte) error {
	var header invertedPostingPageHeader
	if err := header.Unmarshal(buf); err != nil {
		return err
	}
	slotBase := header.size()
	blocks := make([]invertedPostingBlock, 0, header.ItemCount)
	for i := uint16(0); i < header.ItemCount; i++ {
		offset := uint64(unmarshalUint16(buf, slotBase+uint64(i)*2))
		if offset >= uint64(len(buf)) {
			return fmt.Errorf("inverted posting page slot %d out of range", i)
		}
		var block invertedPostingBlock
		if _, err := block.Unmarshal(buf[offset:]); err != nil {
			return err
		}
		blocks = append(blocks, block)
	}
	p.Header = header
	p.Blocks = blocks
	return nil
}

func (b invertedPostingBlock) size() uint64 {
	return 8 + 8 + 4 + 1 + 4 + 4 + uint64(len(b.Payload))
}

// Marshal serializes a posting-tree block, including row range metadata used by
// future block-skipping intersections.
func (b invertedPostingBlock) Marshal(buf []byte) error {
	if len(buf) < int(b.size()) {
		return fmt.Errorf("inverted posting block buffer too small")
	}
	i := uint64(0)
	marshalUint64(buf, uint64(b.FirstRowID), i)
	i += 8
	marshalUint64(buf, uint64(b.LastRowID), i)
	i += 8
	marshalUint32(buf, b.PostingCount, i)
	i += 4
	buf[i] = b.CodecVersion
	i += 1
	marshalUint32(buf, uint32(b.Child), i)
	i += 4
	marshalUint32(buf, uint32(len(b.Payload)), i)
	i += 4
	copy(buf[i:i+uint64(len(b.Payload))], b.Payload)
	return nil
}

// Unmarshal decodes a posting-tree block and returns the number of bytes consumed.
func (b *invertedPostingBlock) Unmarshal(buf []byte) (uint64, error) {
	if len(buf) < 29 {
		return 0, fmt.Errorf("inverted posting block buffer too small")
	}
	i := uint64(0)
	b.FirstRowID = RowID(unmarshalUint64(buf, i))
	i += 8
	b.LastRowID = RowID(unmarshalUint64(buf, i))
	i += 8
	b.PostingCount = unmarshalUint32(buf, i)
	i += 4
	b.CodecVersion = buf[i]
	i += 1
	b.Child = PageIndex(unmarshalUint32(buf, i))
	i += 4
	payloadLen := uint64(unmarshalUint32(buf, i))
	i += 4
	if len(buf) < int(i+payloadLen) {
		return 0, fmt.Errorf("inverted posting block payload truncated")
	}
	b.Payload = append(b.Payload[:0], buf[i:i+payloadLen]...)
	i += payloadLen
	return i, nil
}

func marshalUint16(buf []byte, n uint16, i uint64) []byte {
	buf[i+0] = byte(n >> 0)
	buf[i+1] = byte(n >> 8)
	return buf
}

func unmarshalUint16(buf []byte, i uint64) uint16 {
	return 0 |
		(uint16(buf[i+0]) << 0) |
		(uint16(buf[i+1]) << 8)
}
