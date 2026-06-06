package minisql

import (
	"bytes"
	"context"
	"fmt"
)

// TextPointer is stored in the main row; text of length <= MaxInlineVarchar is stored inline,
// otherwise it points to an overflow page.
type TextPointer struct {
	Data      []byte
	Length    uint32
	FirstPage PageIndex
}

// NewTextPointer constructs a TextPointer wrapping the given byte slice.
// Text shorter than or equal to MaxInlineVarchar will be stored inline.
func NewTextPointer(data []byte) TextPointer {
	return TextPointer{
		Length: uint32(len(data)),
		Data:   data,
	}
}

// Size returns the serialised byte size of the pointer: for inline text, a
// 4-byte length prefix plus the data; for overflow text, a 4-byte length prefix
// plus a 4-byte first-page index.
func (tp TextPointer) Size() uint64 {
	if tp.IsInline() {
		// 4 bytes length prefix + data
		return varcharLengthPrefixSize + uint64(tp.Length)
	}
	// 4 bytes length prefix + 4 bytes first page index
	return varcharLengthPrefixSize + 4
}

// IsInline reports whether the text fits within MaxInlineVarchar bytes and is
// stored directly in the leaf cell rather than on overflow pages.
func (tp TextPointer) IsInline() bool {
	return tp.Length <= MaxInlineVarchar
}

func (tp TextPointer) String() string {
	return string(tp.Data)
}

// NumberOfPages returns the number of overflow pages required to store the text.
func (tp TextPointer) NumberOfPages() uint32 {
	return tp.Length/MaxOverflowPageData + 1
}

// Marshal serialises the pointer into buf at offset i: a 4-byte length prefix
// followed by either the inline data or the first overflow page index.
func (tp *TextPointer) Marshal(buf []byte, i uint64) error {
	// Write length prefix
	marshalUint32(buf, tp.Length, i)
	i += 4

	if tp.IsInline() {
		// Write actual text
		n := copy(buf[i:i+uint64(tp.Length)], tp.Data)
		i += uint64(n)
		return nil
	}

	// Write first overflow page index
	marshalUint32(buf, uint32(tp.FirstPage), i)
	i += 4

	return nil
}

// Unmarshal reads a text pointer from buf at offset i. For inline text, Data
// is sub-sliced directly into the page buffer (zero-copy). For overflow text,
// only FirstPage is set; actual data is loaded later by readOverflowTexts.
func (tp *TextPointer) Unmarshal(buf []byte, i uint64) error {
	if i+4 > uint64(len(buf)) {
		return fmt.Errorf("text pointer unmarshal: buffer too short for length prefix at offset %d (have %d bytes)", i, len(buf))
	}
	// Read length prefix
	tp.Length = unmarshalUint32(buf, i)
	i += 4

	if tp.IsInline() {
		if i+uint64(tp.Length) > uint64(len(buf)) {
			return fmt.Errorf("text pointer unmarshal: buffer too short for inline data (need %d bytes at offset %d, have %d)", tp.Length, i, len(buf))
		}
		// Sub-slice page buffer directly — zero allocation, zero copy.
		// Inline text is read-only after unmarshal; Marshal copies it out via copy().
		tp.Data = buf[i : i+uint64(tp.Length)]
		i += uint64(tp.Length)
		return nil
	}

	// Read first overflow page index
	if i+4 > uint64(len(buf)) {
		return fmt.Errorf("text pointer unmarshal: buffer too short for overflow page index at offset %d (have %d bytes)", i, len(buf))
	}
	tp.FirstPage = PageIndex(unmarshalUint32(buf, i))
	i += 4
	return nil
}

// IsEqual reports whether two TextPointers represent the same value by
// comparing Data content, Length, and FirstPage.
func (tp TextPointer) IsEqual(tp2 TextPointer) bool {
	if string(tp.Data) != string(tp2.Data) {
		return false
	}

	if tp.Length != tp2.Length {
		return false
	}

	if tp.FirstPage != tp2.FirstPage {
		return false
	}

	return true
}

func (r Row) storeOverflowTexts(ctx context.Context, pager TxPager) (Row, error) {
	for i, col := range r.Columns {
		if !col.Kind.IsText() {
			continue
		}
		value := r.Values[i]
		if !value.Valid {
			continue
		}
		textPointer, ok := value.Value.(TextPointer)
		if !ok {
			return r, fmt.Errorf("expected TextPointer value for text column %s", col.Name)
		}
		// Inline text needs no overflow page and the TextPointer is unchanged —
		// skip the re-assignment that would box TextPointer into any (heap alloc).
		if textPointer.IsInline() {
			continue
		}
		if err := textPointer.storeOverflowText(ctx, pager); err != nil {
			return r, err
		}
		r.Values[i] = OptionalValue{Valid: true, Value: textPointer}
	}
	return r, nil
}

func (tp *TextPointer) storeOverflowText(ctx context.Context, pager TxPager) error {
	if tp.IsInline() {
		return nil
	}

	if len(tp.Data) > MaxOverflowTextSize {
		return fmt.Errorf("text size %d exceeds maximum overflow text size %d", len(tp.Data), MaxOverflowTextSize)
	}

	// Calculate how many overflow pages are needed
	numPages := tp.NumberOfPages()
	dataSizeToStore := tp.Length

	// Store text in overflow pages
	var previousPage *Page
	for i := range numPages {
		freePage, err := pager.GetFreePage(ctx)
		if err != nil {
			return fmt.Errorf("allocate overflow page: %w", err)
		}
		if i == 0 {
			tp.FirstPage = freePage.Index
		}
		dataSize := min(dataSizeToStore, MaxOverflowPageData)
		dataSizeToStore -= dataSize
		freePage.OverflowPage = &OverflowPage{
			Header: OverflowPageHeader{
				DataSize: dataSize,
			},
			Data: tp.Data[i*MaxOverflowPageData : i*MaxOverflowPageData+dataSize],
		}
		if previousPage != nil {
			previousPage.OverflowPage.Header.NextPage = freePage.Index
		}
		previousPage = freePage
	}

	return nil
}

func (tp TextPointer) readOverflowText(ctx context.Context, pager TxPager) (TextPointer, error) {
	if tp.IsInline() {
		return tp, nil
	}

	// Read overflow data; pre-allocate to the known total length to avoid
	// repeated reallocation as overflow pages are appended.
	var (
		overflowData   = make([]byte, 0, tp.Length)
		currentPageIdx = tp.FirstPage
		remainingSize  = tp.Length
	)
	for remainingSize > 0 {
		overflowPage, err := pager.ReadPage(ctx, currentPageIdx)
		if err != nil {
			return TextPointer{}, fmt.Errorf("read overflow page %d: %w", currentPageIdx, err)
		}
		if overflowPage.OverflowPage == nil {
			return TextPointer{}, fmt.Errorf("page %d is not an overflow page", currentPageIdx)
		}
		dataSize := min(remainingSize, overflowPage.OverflowPage.Header.DataSize)
		overflowData = append(overflowData, overflowPage.OverflowPage.Data[:dataSize]...)
		remainingSize -= dataSize
		currentPageIdx = overflowPage.OverflowPage.Header.NextPage
	}
	tp.Data = bytes.Trim(overflowData, "\x00")
	return tp, nil
}

func (r Row) readOverflowTexts(ctx context.Context, pager TxPager) (Row, error) {
	for i, col := range r.Columns {
		if !col.Kind.IsText() {
			continue
		}
		if i >= len(r.Values) || !r.Values[i].Valid {
			continue
		}
		textPointer, ok := r.Values[i].Value.(TextPointer)
		if !ok {
			return Row{}, fmt.Errorf("expected TextPointer value for text column %s", col.Name)
		}
		if textPointer.IsInline() {
			continue
		}
		if pager == nil {
			return Row{}, fmt.Errorf("overflow text column %d requires a pager", i)
		}
		textPointer, err := textPointer.readOverflowText(ctx, pager)
		if err != nil {
			return Row{}, err
		}
		r.Values[i] = OptionalValue{Value: textPointer, Valid: true}
	}
	return r, nil
}
