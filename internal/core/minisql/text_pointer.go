package minisql

import (
	"bytes"
	"context"
	"fmt"
)

// Stored in the main row, text of length <= MaxInlineVarchar is stored inline,
// oterwise we point to an overflow page.
type TextPointer struct {
	Length    uint32 // Total size of text
	FirstPage uint32 // First overflow page (if not inline)
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
		marshalUint32(buf, tp.FirstPage, i)
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
		tp.FirstPage = unmarshalUint32(buf, i)
		i += 4
	}

	return nil
}

// Wrap text values in the row into TextPointer structures
func wrapTextValues(ctx context.Context, aPager TxPager, aRow *Row) error {
	for i, aColumn := range aRow.Columns {
		if !aColumn.Kind.IsText() {
			continue
		}
		value, ok := aRow.GetValue(aColumn.Name)
		if !ok || !value.Valid {
			continue
		}
		strValue, ok := value.Value.(string)
		if !ok {
			return fmt.Errorf("expected string value for text column %s", aColumn.Name)
		}
		textPointer, err := wrapTexInPointer(ctx, aPager, []byte(strValue))
		if err != nil {
			return err
		}
		aRow.Values[i] = OptionalValue{
			Valid: true,
			Value: textPointer,
		}
	}
	return nil
}

func wrapTexInPointer(ctx context.Context, aPager TxPager, text []byte) (TextPointer, error) {
	textPointer := NewTextPointer([]byte(text))

	if textPointer.IsInline() {
		return textPointer, nil
	}

	if len([]byte(text)) > MaxOverflowTextSize {
		return TextPointer{}, fmt.Errorf("text size %d exceeds maximum overflow text size %d", len([]byte(text)), MaxOverflowTextSize)
	}

	// Calculate how many overflow pages are needed
	numPages := len([]byte(text))/MaxOverflowPageData + 1
	dataSizeToStore := uint32(len([]byte(text)))

	// Store text in overflow pages
	var previousPage *Page
	for i := range numPages {
		freePage, err := aPager.GetFreePage(ctx)
		if err != nil {
			return TextPointer{}, fmt.Errorf("allocate overflow page: %w", err)
		}
		if i == 0 {
			textPointer.FirstPage = freePage.Index
		}
		dataSize := min(dataSizeToStore, MaxOverflowPageData)
		dataSizeToStore -= dataSize
		freePage.OverflowPage = &OverflowPage{
			Header: OverflowPageHeader{
				DataSize: dataSize,
			},
			Data: text[i*MaxOverflowPageData : i*MaxOverflowPageData+int(dataSize)],
		}
		if previousPage != nil {
			previousPage.OverflowPage.Header.NextPage = freePage.Index
		}
		previousPage = freePage
	}

	return textPointer, nil
}

// Unwrap TextPointer structures in the row into actual text values
func unwrapTextPointers(ctx context.Context, aPager TxPager, aRow *Row) error {
	for i, aColumn := range aRow.Columns {
		if !aColumn.Kind.IsText() {
			continue
		}
		value, ok := aRow.GetValue(aColumn.Name)
		if !ok || !value.Valid {
			continue
		}
		textPointer, ok := value.Value.(TextPointer)
		if !ok {
			return fmt.Errorf("expected TextPointer value for text column %s", aColumn.Name)
		}
		aRow.Values[i] = OptionalValue{
			Valid: true,
			Value: string(textPointer.Data),
		}
		if !textPointer.IsInline() {
			// Load overflow data
			var (
				overflowData   []byte
				currentPageIdx = textPointer.FirstPage
				remainingSize  = textPointer.Length
			)
			for remainingSize > 0 {
				overflowPage, err := aPager.ReadPage(ctx, currentPageIdx)
				if err != nil {
					return fmt.Errorf("read overflow page %d: %w", currentPageIdx, err)
				}
				if overflowPage.OverflowPage == nil {
					return fmt.Errorf("page %d is not an overflow page", currentPageIdx)
				}
				dataSize := min(remainingSize, overflowPage.OverflowPage.Header.DataSize)
				overflowData = append(overflowData, overflowPage.OverflowPage.Data[:dataSize]...)
				remainingSize -= dataSize
				currentPageIdx = overflowPage.OverflowPage.Header.NextPage
			}
			textPointer.Data = overflowData
			aRow.Values[i] = OptionalValue{
				Valid: true,
				Value: string(bytes.Trim(overflowData, "\x00")),
			}
		}
	}
	return nil
}
