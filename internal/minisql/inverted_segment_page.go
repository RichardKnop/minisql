package minisql

import "fmt"

type invertedSegmentPageHeader struct {
	NextPage      PageIndex
	CellCount     uint16
	FreeStart     uint16
	FreeEnd       uint16
	FormatVersion byte
}

func (h invertedSegmentPageHeader) size() uint64 {
	return 1 + 1 + 2 + 2 + 2 + 4
}

// Marshal writes the segment page header into buf.
func (h invertedSegmentPageHeader) Marshal(buf []byte) error {
	if len(buf) < int(h.size()) {
		return fmt.Errorf("inverted segment page header buffer too small")
	}
	i := uint64(0)
	buf[i] = PageTypeInvertedSegment
	i += 1
	buf[i] = h.FormatVersion
	i += 1
	marshalUint16(buf, h.CellCount, i)
	i += 2
	marshalUint16(buf, h.FreeStart, i)
	i += 2
	marshalUint16(buf, h.FreeEnd, i)
	i += 2
	marshalUint32(buf, uint32(h.NextPage), i)
	return nil
}

// Unmarshal decodes the segment page header from buf.
func (h *invertedSegmentPageHeader) Unmarshal(buf []byte) error {
	if len(buf) < int(h.size()) {
		return fmt.Errorf("inverted segment page header buffer too small")
	}
	i := uint64(0)
	if buf[i] != PageTypeInvertedSegment {
		return fmt.Errorf("unmarshal inverted segment page: invalid page type %d", buf[i])
	}
	i += 1
	h.FormatVersion = buf[i]
	if h.FormatVersion != invertedPageFormatVersion {
		return fmt.Errorf("unmarshal inverted segment page: unsupported format version %d", h.FormatVersion)
	}
	i += 1
	h.CellCount = unmarshalUint16(buf, i)
	i += 2
	h.FreeStart = unmarshalUint16(buf, i)
	i += 2
	h.FreeEnd = unmarshalUint16(buf, i)
	i += 2
	h.NextPage = PageIndex(unmarshalUint32(buf, i))
	return nil
}

type invertedSegmentCell struct {
	Term         string
	Block        invertedPostingBlock
	DocFreq      uint32
	PostingCount uint32
	Kind         byte
}

type invertedSegmentPage struct {
	Header invertedSegmentPageHeader
	Cells  []invertedSegmentCell
}

// NewInvertedSegmentPage creates an empty log-structured inverted-index segment page.
func NewInvertedSegmentPage() *invertedSegmentPage {
	header := invertedSegmentPageHeader{
		FormatVersion: invertedPageFormatVersion,
		FreeStart:     uint16((invertedSegmentPageHeader{}).size()),
		FreeEnd:       PageSize - pageChecksumSize,
	}
	return &invertedSegmentPage{Header: header}
}

// Clone returns a deep copy of the segment page.
func (p *invertedSegmentPage) Clone() *invertedSegmentPage {
	if p == nil {
		return nil
	}
	clone := &invertedSegmentPage{
		Header: p.Header,
		Cells:  make([]invertedSegmentCell, len(p.Cells)),
	}
	for i, cell := range p.Cells {
		clone.Cells[i] = cell
		clone.Cells[i].Block.Payload = append([]byte(nil), cell.Block.Payload...)
	}
	return clone
}

// Marshal writes the segment page into buf.
func (p *invertedSegmentPage) Marshal(buf []byte) error {
	if len(buf) < int(p.Header.size()) {
		return fmt.Errorf("inverted segment page buffer too small")
	}
	header := p.Header
	header.FormatVersion = invertedPageFormatVersion
	header.CellCount = uint16(len(p.Cells))
	header.FreeStart = uint16(header.size() + uint64(len(p.Cells))*2)

	freeEnd := len(buf)
	slotBase := header.size()
	for i, cell := range p.Cells {
		sz := int(cell.size())
		if freeEnd-sz < int(header.FreeStart) {
			return fmt.Errorf("inverted segment page has insufficient free space")
		}
		freeEnd -= sz
		if err := cell.Marshal(buf[freeEnd : freeEnd+sz]); err != nil {
			return err
		}
		marshalUint16(buf, uint16(freeEnd), slotBase+uint64(i)*2)
	}
	header.FreeEnd = uint16(freeEnd)
	if err := header.Marshal(buf); err != nil {
		return err
	}
	return nil
}

// Unmarshal decodes the segment page from buf.
func (p *invertedSegmentPage) Unmarshal(buf []byte) error {
	var header invertedSegmentPageHeader
	if err := header.Unmarshal(buf); err != nil {
		return err
	}
	slotBase := header.size()
	cells := make([]invertedSegmentCell, 0, header.CellCount)
	for i := uint16(0); i < header.CellCount; i++ {
		offset := uint64(unmarshalUint16(buf, slotBase+uint64(i)*2))
		if offset >= uint64(len(buf)) {
			return fmt.Errorf("inverted segment page slot %d out of range", i)
		}
		var cell invertedSegmentCell
		if _, err := cell.Unmarshal(buf[offset:]); err != nil {
			return err
		}
		cells = append(cells, cell)
	}
	p.Header = header
	p.Cells = cells
	return nil
}

func (c invertedSegmentCell) size() uint64 {
	return 2 + uint64(len([]byte(c.Term))) + 1 + 4 + 4 + c.Block.size()
}

// Marshal writes the segment cell into buf.
func (c invertedSegmentCell) Marshal(buf []byte) error {
	if len([]byte(c.Term)) > MaxIndexKeySize {
		return fmt.Errorf("inverted segment term exceeds max index key size %d", MaxIndexKeySize)
	}
	if len(buf) < int(c.size()) {
		return fmt.Errorf("inverted segment cell buffer too small")
	}
	i := uint64(0)
	marshalUint16(buf, uint16(len([]byte(c.Term))), i)
	i += 2
	copy(buf[i:i+uint64(len([]byte(c.Term)))], []byte(c.Term))
	i += uint64(len([]byte(c.Term)))
	buf[i] = c.Kind
	i += 1
	marshalUint32(buf, c.DocFreq, i)
	i += 4
	marshalUint32(buf, c.PostingCount, i)
	i += 4
	if err := c.Block.Marshal(buf[i:]); err != nil {
		return err
	}
	return nil
}

// Unmarshal decodes the segment cell from buf.
func (c *invertedSegmentCell) Unmarshal(buf []byte) (uint64, error) {
	if len(buf) < 11 {
		return 0, fmt.Errorf("inverted segment cell buffer too small")
	}
	i := uint64(0)
	termLen := uint64(unmarshalUint16(buf, i))
	i += 2
	if len(buf) < int(i+termLen+9) {
		return 0, fmt.Errorf("inverted segment cell truncated")
	}
	c.Term = string(buf[i : i+termLen])
	i += termLen
	c.Kind = buf[i]
	if c.Kind != invertedSegmentKindInsert && c.Kind != invertedSegmentKindDelete {
		return 0, fmt.Errorf("inverted segment cell has unknown kind %d", c.Kind)
	}
	i += 1
	c.DocFreq = unmarshalUint32(buf, i)
	i += 4
	c.PostingCount = unmarshalUint32(buf, i)
	i += 4
	n, err := c.Block.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += n
	return i, nil
}
