package minisql

import (
	"bytes"
	"context"
	"fmt"
)

// Stored in the main row, text of length <= MaxInlineVarchar is stored inline,
// oterwise we point to an overflow page.
type TextPointer struct {
	Length    uint32    // Total size of text
	FirstPage PageIndex // First overflow page (if not inline)
	Data      []byte
}

func NewTextPointer(data []byte) TextPointer {
	return TextPointer{
		Length: uint32(len(data)),
		Data:   data,
	}
}

func (tp TextPointer) Size() uint64 {
	if tp.IsInline() {
		// 4 bytes length prefix + data
		return varcharLengthPrefixSize + uint64(tp.Length)
	}
	// 4 bytes length prefix + 4 bytes first page index
	return varcharLengthPrefixSize + 4
}

func (tp TextPointer) IsInline() bool {
	return tp.Length <= MaxInlineVarchar
}

func (tp TextPointer) String() string {
	return string(tp.Data)
}

func (tp TextPointer) NumberOfPages() uint32 {
	return tp.Length/MaxOverflowPageData + 1
}

func (tp *TextPointer) Marshal(buf []byte, i uint64) ([]byte, error) {
	// Write length prefix
	marshalUint32(buf, tp.Length, i)
	i += 4

	if tp.IsInline() {
		// Write actual text
		n := copy(buf[i:i+uint64(tp.Length)], tp.Data)
		i += uint64(n)
	} else {
		// Write first overflow page index
		marshalUint32(buf, uint32(tp.FirstPage), i)
		i += 4
	}

	return buf, nil
}

func (tp *TextPointer) Unmarshal(buf []byte, i uint64) error {
	// Read length prefix
	tp.Length = unmarshalUint32(buf, i)
	i += 4

	if tp.IsInline() {
		// Read actual text
		tp.Data = make([]byte, tp.Length)
		copy(tp.Data, buf[i:i+uint64(tp.Length)])
		i += uint64(tp.Length)
	} else {
		// Read first overflow page index
		tp.FirstPage = PageIndex(unmarshalUint32(buf, i))
		i += 4
	}

	return nil
}

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

func (r Row) storeOverflowTexts(ctx context.Context, aPager TxPager) (Row, error) {
	for i, aColumn := range r.Columns {
		if !aColumn.Kind.IsText() {
			continue
		}
		value, ok := r.GetValue(aColumn.Name)
		if !ok || !value.Valid {
			continue
		}
		textPointer, ok := value.Value.(TextPointer)
		if !ok {
			return r, fmt.Errorf("expected TextPointer value for text column %s", aColumn.Name)
		}
		if err := textPointer.storeOverflowText(ctx, aPager); err != nil {
			return r, err
		}
		r.Values[i] = OptionalValue{
			Valid: true,
			Value: textPointer,
		}
	}
	return r, nil
}

func (tp *TextPointer) storeOverflowText(ctx context.Context, aPager TxPager) error {
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
		freePage, err := aPager.GetFreePage(ctx)
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

func (r Row) readOverflowTexts(ctx context.Context, aPager TxPager) (Row, error) {
	if len(r.Values) == 0 {
		return r, nil
	}
	for _, aColumn := range r.Columns {
		if !aColumn.Kind.IsText() {
			continue
		}
		value, ok := r.GetValue(aColumn.Name)
		if !ok || !value.Valid {
			continue
		}
		textPointer, ok := value.Value.(TextPointer)
		if !ok {
			return Row{}, fmt.Errorf("expected TextPointer value for text column %s", aColumn.Name)
		}
		if textPointer.IsInline() {
			continue
		}
		// Read overflow data
		var (
			overflowData   []byte
			currentPageIdx = textPointer.FirstPage
			remainingSize  = textPointer.Length
		)
		for remainingSize > 0 {
			overflowPage, err := aPager.ReadPage(ctx, currentPageIdx)
			if err != nil {
				return Row{}, fmt.Errorf("read overflow page %d: %w", currentPageIdx, err)
			}
			if overflowPage.OverflowPage == nil {
				return Row{}, fmt.Errorf("page %d is not an overflow page", currentPageIdx)
			}
			dataSize := min(remainingSize, overflowPage.OverflowPage.Header.DataSize)
			overflowData = append(overflowData, overflowPage.OverflowPage.Data[:dataSize]...)
			remainingSize -= dataSize
			currentPageIdx = overflowPage.OverflowPage.Header.NextPage
		}
		textPointer.Data = bytes.Trim(overflowData, "\x00")
		r, _ = r.SetValue(aColumn.Name, OptionalValue{Value: textPointer, Valid: true})
	}
	return r, nil
}
