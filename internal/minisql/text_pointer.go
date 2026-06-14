package minisql

import (
	"context"
	"fmt"
)

// textOverflowFlag occupies bit 31 of the on-disk uint32 length field.
// When set the value is stored on overflow pages; when clear the value is inline.
// The lower 31 bits always hold the actual byte length, so text up to 2 GiB is supported.
const textOverflowFlag uint32 = 1 << 31

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

// Marshal serialises the pointer into buf at offset i.
// Inline:   [uint32 length (bit31=0)][length bytes of data]
// Overflow: [uint32 length | textOverflowFlag (bit31=1)][uint32 first_page_index]
func (tp *TextPointer) Marshal(buf []byte, i uint64) error {
	if tp.IsInline() {
		marshalUint32(buf, tp.Length, i) // bit31 = 0: inline
		i += 4
		copy(buf[i:i+uint64(tp.Length)], tp.Data)
		return nil
	}

	marshalUint32(buf, tp.Length|textOverflowFlag, i) // bit31 = 1: overflow
	i += 4
	marshalUint32(buf, uint32(tp.FirstPage), i)
	return nil
}

// Unmarshal reads a text pointer from buf at offset i. For inline text, Data
// is sub-sliced directly into the page buffer (zero-copy). For overflow text,
// only FirstPage is set; actual data is loaded later by readOverflowTexts.
//
// The on-disk format uses bit 31 of the uint32 length field as an overflow flag
// (textOverflowFlag) so the inline/overflow distinction is stored on disk and
// does not depend on the value of MaxInlineVarchar at read time.
func (tp *TextPointer) Unmarshal(buf []byte, i uint64) error {
	if i+4 > uint64(len(buf)) {
		return fmt.Errorf("text pointer unmarshal: buffer too short for length prefix at offset %d (have %d bytes)", i, len(buf))
	}
	stored := unmarshalUint32(buf, i)
	i += 4

	if stored&textOverflowFlag != 0 {
		// Overflow: actual length is stored in bits 0–30.
		tp.Length = stored &^ textOverflowFlag
		if i+4 > uint64(len(buf)) {
			return fmt.Errorf("text pointer unmarshal: buffer too short for overflow page index at offset %d (have %d bytes)", i, len(buf))
		}
		tp.FirstPage = PageIndex(unmarshalUint32(buf, i))
		return nil
	}

	// Inline: stored value is the actual length (bit31 = 0).
	tp.Length = stored
	if i+uint64(tp.Length) > uint64(len(buf)) {
		return fmt.Errorf("text pointer unmarshal: buffer too short for inline data (need %d bytes at offset %d, have %d)", tp.Length, i, len(buf))
	}
	// Sub-slice page buffer directly — zero allocation, zero copy.
	// Inline text is read-only after unmarshal; Marshal copies it out via copy().
	tp.Data = buf[i : i+uint64(tp.Length)]
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

// updateOverflowText writes overflow pages for tp, reusing the chain rooted at
// oldFirstPage wherever possible instead of free-then-reallocating.
//
// GetOverflowPage calls ModifyPage internally, so the pages it returns are
// already in the write set and can be overwritten without an additional pager
// call. The three cases are:
//
//   - same size (most common on UPDATE): reuse all old pages, zero free/alloc calls.
//   - new > old: reuse old pages, call GetFreePage for the tail extension.
//   - new < old: reuse new-length prefix, call AddFreePage for the excess tail.
//
// When oldFirstPage == 0 (old value was inline or absent) the call falls
// through to storeOverflowText so callers need not special-case this.
func (tp *TextPointer) updateOverflowText(ctx context.Context, pager TxPager, oldFirstPage PageIndex) error {
	if tp.IsInline() {
		return nil
	}
	if oldFirstPage == 0 {
		return tp.storeOverflowText(ctx, pager)
	}
	if len(tp.Data) > MaxOverflowTextSize {
		return fmt.Errorf("text size %d exceeds maximum overflow text size %d", len(tp.Data), MaxOverflowTextSize)
	}

	// Walk old chain. GetOverflowPage calls ModifyPage, so each page is already
	// writable; we record the NextPage before the reuse loop overwrites it.
	type entry struct {
		page *Page
		next PageIndex
	}
	old := make([]entry, 0, tp.NumberOfPages())
	for curIdx := oldFirstPage; curIdx > 0; {
		p, err := pager.GetOverflowPage(ctx, curIdx)
		if err != nil {
			return fmt.Errorf("read old overflow page %d: %w", curIdx, err)
		}
		next := p.OverflowPage.Header.NextPage
		old = append(old, entry{p, next})
		curIdx = next
	}

	numNewPages := tp.NumberOfPages()
	numOldPages := uint32(len(old))
	reuse := min(numNewPages, numOldPages)
	dataSizeToStore := tp.Length

	var previousPage *Page
	for i := range reuse {
		p := old[i].page
		if i == 0 {
			tp.FirstPage = p.Index
		}
		dataSize := min(dataSizeToStore, MaxOverflowPageData)
		dataSizeToStore -= dataSize
		p.OverflowPage.Header.DataSize = dataSize
		p.OverflowPage.Header.NextPage = 0
		p.OverflowPage.Data = tp.Data[i*MaxOverflowPageData : i*MaxOverflowPageData+dataSize]
		if previousPage != nil {
			previousPage.OverflowPage.Header.NextPage = p.Index
		}
		previousPage = p
	}

	// New text is longer than old chain: allocate extra pages at the tail.
	for i := reuse; i < numNewPages; i++ {
		freePage, err := pager.GetFreePage(ctx)
		if err != nil {
			return fmt.Errorf("allocate overflow page: %w", err)
		}
		dataSize := min(dataSizeToStore, MaxOverflowPageData)
		dataSizeToStore -= dataSize
		freePage.OverflowPage = &OverflowPage{
			Header: OverflowPageHeader{DataSize: dataSize},
			Data:   tp.Data[i*MaxOverflowPageData : i*MaxOverflowPageData+dataSize],
		}
		if previousPage != nil {
			previousPage.OverflowPage.Header.NextPage = freePage.Index
		}
		previousPage = freePage
	}

	// New text is shorter than old chain: return excess tail pages to the free list.
	for i := reuse; i < numOldPages; i++ {
		if err := pager.AddFreePage(ctx, old[i].page.Index); err != nil {
			return fmt.Errorf("free excess overflow page: %w", err)
		}
	}

	return nil
}

// updateOverflowTexts handles the text-overflow UPDATE hot path, replacing the
// freeOverflowPages + storeOverflowTexts pair. It only touches columns present
// in changedCols, which:
//   - avoids re-storing unchanged overflow columns (which would orphan their
//     existing pages and create duplicate chains), and
//   - eliminates the free-then-reallocate cycle by reusing old pages in-place.
func (r Row) updateOverflowTexts(ctx context.Context, pager TxPager, oldRow Row, changedCols map[string]Column) (Row, error) {
	for i, col := range r.Columns {
		if !col.Kind.IsText() {
			continue
		}
		if _, isChanged := changedCols[col.Name]; !isChanged {
			continue
		}
		value := r.Values[i]
		if !value.Valid {
			continue
		}
		newTP, ok := value.Value.(TextPointer)
		if !ok {
			return r, fmt.Errorf("expected TextPointer value for text column %s", col.Name)
		}

		// Determine whether the old value occupied an overflow chain.
		var oldFirstPage PageIndex
		if oldVal, ok2 := oldRow.GetValue(col.Name); ok2 && oldVal.Valid {
			if oldTP, ok3 := oldVal.Value.(TextPointer); ok3 && !oldTP.IsInline() {
				oldFirstPage = oldTP.FirstPage
			}
		}

		if newTP.IsInline() {
			// New value fits inline; free the old overflow chain if one existed.
			if oldFirstPage != 0 {
				for curIdx := oldFirstPage; curIdx > 0; {
					p, err := pager.GetOverflowPage(ctx, curIdx)
					if err != nil {
						return r, fmt.Errorf("read overflow page %d: %w", curIdx, err)
					}
					next := p.OverflowPage.Header.NextPage
					if err := pager.AddFreePage(ctx, curIdx); err != nil {
						return r, fmt.Errorf("free overflow page %d: %w", curIdx, err)
					}
					curIdx = next
				}
			}
			continue
		}

		// New value needs overflow pages — reuse old chain where possible.
		if err := newTP.updateOverflowText(ctx, pager, oldFirstPage); err != nil {
			return r, fmt.Errorf("update overflow text for column %s: %w", col.Name, err)
		}
		r.Values[i] = OptionalValue{Valid: true, Value: newTP}
	}
	return r, nil
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
	tp.Data = overflowData
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
