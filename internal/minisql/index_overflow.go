package minisql

import (
	"context"
	"fmt"
)

const (
	// MaxInlineRowIDs ...
	MaxInlineRowIDs = 4
	// MaxOverflowRowIDsPerPage ...
	MaxOverflowRowIDsPerPage = 510 // (4096 - 1 - 8 ) / 8
	rowIDsLengthPrefixSize   = 4
)

// IndexOverflowPageHeader ...
type IndexOverflowPageHeader struct {
	NextPage  PageIndex // 0 if last page
	ItemCount uint32    // how many row IDs are stored in this page
}

// Size ...
func (h *IndexOverflowPageHeader) Size() uint64 {
	return 1 + 4 + 4
}

// IndexOverflowPage ...
type IndexOverflowPage struct {
	RowIDs []RowID
	Header IndexOverflowPageHeader
}

// Size ...
func (h *IndexOverflowPage) Size() uint64 {
	return h.Header.Size() + uint64(h.Header.ItemCount)*8
}

// Marshal ...
func (h *IndexOverflowPage) Marshal(buf []byte) error {
	i := uint64(0)

	buf[i] = PageTypeIndexOverflow
	i += 1

	marshalUint32(buf, uint32(h.Header.NextPage), i)
	i += 4

	marshalUint32(buf, h.Header.ItemCount, i)
	i += 4

	for j := range h.Header.ItemCount {
		marshalUint64(buf, uint64(h.RowIDs[j]), i)
		i += 8
	}

	return nil
}

// Unmarshal ...
func (h *IndexOverflowPage) Unmarshal(buf []byte) error {
	i := uint64(0)

	if buf[i] != PageTypeIndexOverflow {
		return fmt.Errorf("invalid index overflow page type byte %d", buf[i])
	}
	i += 1

	h.Header.NextPage = PageIndex(unmarshalUint32(buf, i))
	i += 4

	h.Header.ItemCount = unmarshalUint32(buf, i)
	i += 4

	h.RowIDs = make([]RowID, 0, h.Header.ItemCount)
	for range h.Header.ItemCount {
		h.RowIDs = append(h.RowIDs, RowID(unmarshalUint64(buf, i)))
		i += 8
	}

	return nil
}

// LastRowID ...
func (h *IndexOverflowPage) LastRowID() RowID {
	if h.Header.ItemCount == 0 {
		return 0
	}
	return h.RowIDs[h.Header.ItemCount-1]
}

// RemoveLastRowID ...
func (h *IndexOverflowPage) RemoveLastRowID() RowID {
	rowID := h.RowIDs[h.Header.ItemCount-1]
	h.Header.ItemCount -= 1
	h.RowIDs = h.RowIDs[:h.Header.ItemCount]
	return rowID
}

func appendRowID[T IndexKey](ctx context.Context, pager TxPager, node *IndexNode[T], cellIdx uint32, rowID RowID) error {
	cell := node.Cells[cellIdx]
	if cell.Overflow == 0 && len(cell.RowIDs) < MaxInlineRowIDs {
		// Just append to inline row IDs
		node.Cells[cellIdx].RowIDs = append(node.Cells[cellIdx].RowIDs, rowID)
		node.Cells[cellIdx].InlineRowIDs += 1
		return nil
	}
	if cell.Overflow == 0 && len(cell.RowIDs) == MaxInlineRowIDs {
		// First overflow page
		freePage, err := pager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		freePage.IndexOverflowNode = &IndexOverflowPage{
			Header: IndexOverflowPageHeader{
				ItemCount: 1,
			},
			RowIDs: []RowID{rowID},
		}
		node.Cells[cellIdx].Overflow = freePage.Index
		return nil
	}
	// Add to existing overflow pages

	var (
		overflowIdx  = cell.Overflow
		overflowPage *Page
	)
	for overflowIdx != 0 {
		var err error
		overflowPage, err = pager.ModifyPage(ctx, overflowIdx)
		if err != nil {
			return err
		}
		overflowIdx = overflowPage.IndexOverflowNode.Header.NextPage
	}
	if overflowPage.IndexOverflowNode.Header.ItemCount < MaxOverflowRowIDsPerPage {
		overflowPage.IndexOverflowNode.Header.ItemCount += 1
		overflowPage.IndexOverflowNode.RowIDs = append(overflowPage.IndexOverflowNode.RowIDs, rowID)
		return nil
	}
	// We need to append new overflow page
	freePage, err := pager.GetFreePage(ctx)
	if err != nil {
		return err
	}
	freePage.IndexOverflowNode = &IndexOverflowPage{
		Header: IndexOverflowPageHeader{
			ItemCount: 1,
		},
		RowIDs: []RowID{rowID},
	}
	overflowPage.IndexOverflowNode.Header.NextPage = freePage.Index
	return nil
}

func removeRowID[T IndexKey](ctx context.Context, pager TxPager, node *IndexNode[T], cellIdx uint32, key T, rowID RowID) error {
	// For non-unique index, we need to remove specific row ID,
	// check for overflow page to free and potentially remove the key
	// if no row IDs left.
	if node.Cells[cellIdx].InlineRowIDs == 1 {
		// If there is only one inline row ID and it matches, remove the key
		if node.Cells[cellIdx].RowIDs[0] == rowID {
			return node.DeleteKeyAndRightChild(uint32(cellIdx))
		}
		return fmt.Errorf("row ID %d not found for key %v", rowID, key)
	}
	if node.Cells[cellIdx].Overflow == 0 {
		// No overflow page, just remove inline row ID
		if node.Cells[cellIdx].RemoveRowID(rowID) < 0 {
			return fmt.Errorf("row ID %d not found for key %v", rowID, key)
		}
		return nil
	}
	// Otherwise, we need to find the row in overflow pages, remove it,
	// shift all row IDs and potentially free an overflow page. We will do this
	// by finding the last row ID in overflow pages and moving it to fill the gap.
	var (
		overflowIdx            = node.Cells[cellIdx].Overflow
		previousPage, lastPage *Page
		foundPage              *Page // overflow page where we found the row ID to remove
		foundIdx               int   // index within foundPage where we found the row ID to remove
	)
	for overflowIdx != 0 {
		if lastPage != nil {
			previousPage = lastPage
		}
		var err error
		lastPage, err = pager.ReadPage(ctx, overflowIdx)
		if err != nil {
			return fmt.Errorf("read index overflow page %d: %w", overflowIdx, err)
		}
		if foundPage == nil {
			// Look for the row ID to remove and keep track of the page where we found it
			for i := 0; i < int(lastPage.IndexOverflowNode.Header.ItemCount); i++ {
				if lastPage.IndexOverflowNode.RowIDs[i] != rowID {
					continue
				}
				foundPage = lastPage
				foundIdx = i
				break
			}
		}
		overflowIdx = lastPage.IndexOverflowNode.Header.NextPage
	}
	lastOverflowNode := lastPage.IndexOverflowNode
	switch {
	case lastOverflowNode.LastRowID() == rowID:
		// The row ID to remove is the last one, just remove it
		lastOverflowNode.RemoveLastRowID()
	case foundPage != nil:
		// Remove the row ID by replacing it with the last row ID
		foundPage.IndexOverflowNode.RowIDs[foundIdx] = lastOverflowNode.RemoveLastRowID()
	default:
		// Row ID is inlined, replace it with last overflow row ID
		if node.Cells[cellIdx].ReplaceRowID(rowID, lastOverflowNode.RemoveLastRowID()) < 0 {
			return fmt.Errorf("row ID %d not found for key %v", rowID, key)
		}
	}

	if lastOverflowNode.Header.ItemCount == 0 {
		// Free the last overflow page
		if previousPage == nil {
			// This was the only overflow page, update the cell to remove overflow
			node.Cells[cellIdx].Overflow = 0
		} else {
			previousPage.IndexOverflowNode.Header.NextPage = 0
		}
		if err := pager.AddFreePage(ctx, lastPage.Index); err != nil {
			return fmt.Errorf("free index overflow page %d: %w", lastPage.Index, err)
		}
	}

	return nil
}

func readOverflowRowIDs[T IndexKey](ctx context.Context, pager TxPager, overflowIdx PageIndex) ([]RowID, error) {
	if overflowIdx == 0 {
		return nil, nil
	}

	rowIDs := make([]RowID, 0, 1)
	for overflowIdx != 0 {
		overflowPage, err := pager.ReadPage(ctx, overflowIdx)
		if err != nil {
			return nil, fmt.Errorf("read index overflow page %d: %w", overflowIdx, err)
		}
		rowIDs = append(
			rowIDs,
			overflowPage.IndexOverflowNode.RowIDs[0:overflowPage.IndexOverflowNode.Header.ItemCount]...,
		)
		overflowIdx = overflowPage.IndexOverflowNode.Header.NextPage
	}

	return rowIDs, nil
}
