package minisql

import (
	"context"
	"fmt"
)

const (
	MaxInlineRowIDs          = 4
	MaxOverflowRowIDsPerPage = 510 // (4096 - 1 - 8 ) / 8
	rowIDsLengthPrefixSize   = 4
)

type IndexOverflowPageHeader struct {
	NextPage  PageIndex // 0 if last page
	ItemCount uint32    // how many row IDs are stored in this page
}

func (h *IndexOverflowPageHeader) Size() uint64 {
	return 1 + 4 + 4
}

type IndexOverflowPage struct {
	Header IndexOverflowPageHeader
	RowIDs []RowID
}

func (h *IndexOverflowPage) Size() uint64 {
	return h.Header.Size() + uint64(h.Header.ItemCount)*8
}

func (n *IndexOverflowPage) Marshal(buf []byte) ([]byte, error) {
	i := uint64(0)

	buf[i] = PageTypeIndexOverflow
	i += 1

	marshalUint32(buf, uint32(n.Header.NextPage), i)
	i += 4

	marshalUint32(buf, n.Header.ItemCount, i)
	i += 4

	for j := range n.Header.ItemCount {
		marshalUint64(buf, uint64(n.RowIDs[j]), i)
		i += 8
	}

	return buf[:i], nil
}

func (n *IndexOverflowPage) Unmarshal(buf []byte) error {
	i := uint64(0)

	if buf[i] != PageTypeIndexOverflow {
		return fmt.Errorf("invalid index overflow page type byte %d", buf[i])
	}
	i += 1

	n.Header.NextPage = PageIndex(unmarshalUint32(buf, i))
	i += 4

	n.Header.ItemCount = unmarshalUint32(buf, i)
	i += 4

	n.RowIDs = make([]RowID, 0, n.Header.ItemCount)
	for range n.Header.ItemCount {
		n.RowIDs = append(n.RowIDs, RowID(unmarshalUint64(buf, i)))
		i += 8
	}

	return nil
}

func (n *IndexOverflowPage) LastRowID() RowID {
	if n.Header.ItemCount == 0 {
		return 0
	}
	return n.RowIDs[n.Header.ItemCount-1]
}

func (n *IndexOverflowPage) RemoveLastRowID() RowID {
	rowID := n.RowIDs[n.Header.ItemCount-1]
	n.Header.ItemCount -= 1
	n.RowIDs = n.RowIDs[:n.Header.ItemCount]
	return rowID
}

func appendRowID[T IndexKey](ctx context.Context, aPager TxPager, aNode *IndexNode[T], cellIdx uint32, rowID RowID) error {
	aCell := aNode.Cells[cellIdx]
	if aCell.Overflow == 0 && len(aCell.RowIDs) < MaxInlineRowIDs {
		// Just append to inline row IDs
		aNode.Cells[cellIdx].RowIDs = append(aNode.Cells[cellIdx].RowIDs, rowID)
		aNode.Cells[cellIdx].InlineRowIDs += 1
		return nil
	}
	if aCell.Overflow == 0 && len(aCell.RowIDs) == MaxInlineRowIDs {
		// First overflow page
		freePage, err := aPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		freePage.IndexOverflowNode = &IndexOverflowPage{
			Header: IndexOverflowPageHeader{
				ItemCount: 1,
			},
			RowIDs: []RowID{rowID},
		}
		aNode.Cells[cellIdx].Overflow = freePage.Index
		return nil
	}
	// Add to existing overflow pages

	var (
		overflowIdx  = aCell.Overflow
		overflowPage *Page
	)
	for overflowIdx != 0 {
		var err error
		overflowPage, err = aPager.ModifyPage(ctx, overflowIdx)
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
	freePage, err := aPager.GetFreePage(ctx)
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

func removeRowID[T IndexKey](ctx context.Context, aPager TxPager, aNode *IndexNode[T], cellIdx uint32, key T, rowID RowID) error {
	// For non-unique index, we need to remove specific row ID,
	// check for overflow page to free and potentially remove the key
	// if no row IDs left.
	if aNode.Cells[cellIdx].InlineRowIDs == 1 {
		// If there is only one inline row ID and it matches, remove the key
		if aNode.Cells[cellIdx].RowIDs[0] == rowID {
			return aNode.DeleteKeyAndRightChild(uint32(cellIdx))
		}
		return fmt.Errorf("row ID %d not found for key %v", rowID, key)
	}
	if aNode.Cells[cellIdx].Overflow == 0 {
		// No overflow page, just remove inline row ID
		if aNode.Cells[cellIdx].RemoveRowID(rowID) < 0 {
			return fmt.Errorf("row ID %d not found for key %v", rowID, key)
		}
		return nil
	}
	// Otherwise, we need to find the row in overflow pages, remove it,
	// shift all row IDs and potentially free an overflow page. We will do this
	// by finding the last row ID in overflow pages and moving it to fill the gap.
	var (
		overflowIdx            = aNode.Cells[cellIdx].Overflow
		previousPage, lastPage *Page
		foundPage              *Page // overflow page where we found the row ID to remove
		foundIdx               int   // index within foundPage where we found the row ID to remove
	)
	for overflowIdx != 0 {
		if lastPage != nil {
			previousPage = lastPage
		}
		var err error
		lastPage, err = aPager.ReadPage(ctx, overflowIdx)
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
	if lastOverflowNode.LastRowID() == rowID {
		// The row ID to remove is the last one, just remove it
		lastOverflowNode.RemoveLastRowID()
	} else if foundPage != nil {
		// Remove the row ID by replacing it with the last row ID
		foundPage.IndexOverflowNode.RowIDs[foundIdx] = lastOverflowNode.RemoveLastRowID()
	} else {
		// Row ID is inlined, replace it with last overflow row ID
		if aNode.Cells[cellIdx].ReplaceRowID(rowID, lastOverflowNode.RemoveLastRowID()) < 0 {
			return fmt.Errorf("row ID %d not found for key %v", rowID, key)
		}
	}

	if lastOverflowNode.Header.ItemCount == 0 {
		// Free the last overflow page
		if previousPage == nil {
			// This was the only overflow page, update the cell to remove overflow
			aNode.Cells[cellIdx].Overflow = 0
		} else {
			previousPage.IndexOverflowNode.Header.NextPage = 0
		}
		if err := aPager.AddFreePage(ctx, lastPage.Index); err != nil {
			return fmt.Errorf("free index overflow page %d: %w", lastPage.Index, err)
		}
	}

	return nil
}

func readOverflowRowIDs[T IndexKey](ctx context.Context, aPager TxPager, overflowIdx PageIndex) ([]RowID, error) {
	if overflowIdx == 0 {
		return nil, nil
	}

	rowIDs := make([]RowID, 0, 1)
	for overflowIdx != 0 {
		overflowPage, err := aPager.ReadPage(ctx, overflowIdx)
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
