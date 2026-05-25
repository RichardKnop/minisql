package minisql

import "fmt"

const (
	invertedSegmentKindInsert byte = 1
	invertedSegmentKindDelete byte = 2
	invertedSegmentKindMixed  byte = 3
)

type invertedSegmentDescriptor struct {
	Generation   uint64
	RootPage     PageIndex
	PostingCount uint32
	Kind         byte
	Level        byte
	FirstTerm    string
	LastTerm     string
}

func (d invertedSegmentDescriptor) size() uint64 {
	return 1 + 1 + 4 + 4 + 8 + 2 + uint64(len([]byte(d.FirstTerm))) + 2 + uint64(len([]byte(d.LastTerm)))
}

// Marshal writes the segment descriptor into buf.
func (d invertedSegmentDescriptor) Marshal(buf []byte) error {
	if len([]byte(d.FirstTerm)) > MaxIndexKeySize {
		return fmt.Errorf("inverted segment first term exceeds max index key size %d", MaxIndexKeySize)
	}
	if len([]byte(d.LastTerm)) > MaxIndexKeySize {
		return fmt.Errorf("inverted segment last term exceeds max index key size %d", MaxIndexKeySize)
	}
	if len(buf) < int(d.size()) {
		return fmt.Errorf("inverted segment descriptor buffer too small")
	}
	i := uint64(0)
	buf[i] = d.Kind
	i += 1
	buf[i] = d.Level
	i += 1
	marshalUint32(buf, uint32(d.RootPage), i)
	i += 4
	marshalUint32(buf, d.PostingCount, i)
	i += 4
	marshalUint64(buf, d.Generation, i)
	i += 8
	marshalUint16(buf, uint16(len([]byte(d.FirstTerm))), i)
	i += 2
	copy(buf[i:i+uint64(len([]byte(d.FirstTerm)))], []byte(d.FirstTerm))
	i += uint64(len([]byte(d.FirstTerm)))
	marshalUint16(buf, uint16(len([]byte(d.LastTerm))), i)
	i += 2
	copy(buf[i:i+uint64(len([]byte(d.LastTerm)))], []byte(d.LastTerm))
	return nil
}

// Unmarshal decodes the segment descriptor from buf.
func (d *invertedSegmentDescriptor) Unmarshal(buf []byte) error {
	const fixedSize = 1 + 1 + 4 + 4 + 8 + 2 + 2
	if len(buf) < fixedSize {
		return fmt.Errorf("inverted segment descriptor buffer too small")
	}
	i := uint64(0)
	d.Kind = buf[i]
	if d.Kind != invertedSegmentKindInsert && d.Kind != invertedSegmentKindDelete && d.Kind != invertedSegmentKindMixed {
		return fmt.Errorf("inverted segment descriptor has unknown kind %d", d.Kind)
	}
	i += 1
	d.Level = buf[i]
	i += 1
	d.RootPage = PageIndex(unmarshalUint32(buf, i))
	i += 4
	d.PostingCount = unmarshalUint32(buf, i)
	i += 4
	d.Generation = unmarshalUint64(buf, i)
	i += 8
	firstTermLen := uint64(unmarshalUint16(buf, i))
	i += 2
	if len(buf) < int(i+firstTermLen+2) {
		return fmt.Errorf("inverted segment descriptor first term truncated")
	}
	d.FirstTerm = string(buf[i : i+firstTermLen])
	i += firstTermLen
	lastTermLen := uint64(unmarshalUint16(buf, i))
	i += 2
	if len(buf) < int(i+lastTermLen) {
		return fmt.Errorf("inverted segment descriptor last term truncated")
	}
	d.LastTerm = string(buf[i : i+lastTermLen])
	return nil
}

type invertedMetaPage struct {
	FormatVersion  byte
	Mode           invertedPostingMode
	BaseRoot       PageIndex
	NextGeneration uint64
	Segments       []invertedSegmentDescriptor
}

// NewInvertedMetaPage creates metadata for a log-structured inverted index.
func NewInvertedMetaPage(mode invertedPostingMode, baseRoot PageIndex) *invertedMetaPage {
	return &invertedMetaPage{
		FormatVersion:  invertedPageFormatVersion,
		Mode:           mode,
		BaseRoot:       baseRoot,
		NextGeneration: 1,
	}
}

func (p *invertedMetaPage) headerSize() uint64 {
	return 1 + 1 + 1 + 2 + 4 + 8
}

// Clone returns a deep copy of the inverted metadata page.
func (p *invertedMetaPage) Clone() *invertedMetaPage {
	if p == nil {
		return nil
	}
	clone := &invertedMetaPage{
		FormatVersion:  p.FormatVersion,
		Mode:           p.Mode,
		BaseRoot:       p.BaseRoot,
		NextGeneration: p.NextGeneration,
		Segments:       make([]invertedSegmentDescriptor, len(p.Segments)),
	}
	copy(clone.Segments, p.Segments)
	return clone
}

// Marshal writes the inverted metadata page into buf.
func (p *invertedMetaPage) Marshal(buf []byte) error {
	used := p.usedBytes()
	if len(buf) < int(used) {
		return fmt.Errorf("inverted meta page buffer too small")
	}
	if used > uint64(len(buf)) {
		return fmt.Errorf("inverted meta page is full")
	}

	i := uint64(0)
	buf[i] = PageTypeInvertedMeta
	i += 1
	buf[i] = invertedPageFormatVersion
	i += 1
	buf[i] = byte(p.Mode)
	i += 1
	marshalUint16(buf, uint16(len(p.Segments)), i)
	i += 2
	marshalUint32(buf, uint32(p.BaseRoot), i)
	i += 4
	marshalUint64(buf, p.NextGeneration, i)
	i += 8

	for _, segment := range p.Segments {
		if err := segment.Marshal(buf[i:]); err != nil {
			return err
		}
		i += segment.size()
	}
	return nil
}

func (p *invertedMetaPage) usedBytes() uint64 {
	used := p.headerSize()
	for _, segment := range p.Segments {
		used += segment.size()
	}
	return used
}

// Unmarshal decodes the inverted metadata page from buf.
func (p *invertedMetaPage) Unmarshal(buf []byte) error {
	if len(buf) < int(p.headerSize()) {
		return fmt.Errorf("inverted meta page buffer too small")
	}
	i := uint64(0)
	if buf[i] != PageTypeInvertedMeta {
		return fmt.Errorf("unmarshal inverted meta page: invalid page type %d", buf[i])
	}
	i += 1
	p.FormatVersion = buf[i]
	if p.FormatVersion != invertedPageFormatVersion {
		return fmt.Errorf("unmarshal inverted meta page: unsupported format version %d", p.FormatVersion)
	}
	i += 1
	p.Mode = invertedPostingMode(buf[i])
	if p.Mode != invertedPostingModeRowIDs && p.Mode != invertedPostingModePositions {
		return fmt.Errorf("unmarshal inverted meta page: unknown posting mode %d", p.Mode)
	}
	i += 1
	segmentCount := int(unmarshalUint16(buf, i))
	i += 2
	p.BaseRoot = PageIndex(unmarshalUint32(buf, i))
	i += 4
	p.NextGeneration = unmarshalUint64(buf, i)
	i += 8

	segments := make([]invertedSegmentDescriptor, 0, segmentCount)
	for range segmentCount {
		const fixedDescriptorSize = 1 + 1 + 4 + 4 + 8 + 2 + 2
		if len(buf) < int(i+fixedDescriptorSize) {
			return fmt.Errorf("inverted meta page segment list truncated")
		}
		var segment invertedSegmentDescriptor
		if err := segment.Unmarshal(buf[i:]); err != nil {
			return err
		}
		segments = append(segments, segment)
		i += segment.size()
	}
	p.Segments = segments
	return nil
}
