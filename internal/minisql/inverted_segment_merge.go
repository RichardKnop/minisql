package minisql

import (
	"context"
	"fmt"
	"sort"
)

type invertedSegmentCellCursor struct {
	ctx     context.Context
	index   *logStructuredInvertedIndex
	pageIdx PageIndex
	page    *Page
	cellIdx int
	cell    invertedSegmentCell
	ok      bool
}

func (idx *logStructuredInvertedIndex) newSegmentCellCursor(
	ctx context.Context,
	rootPage PageIndex,
) (*invertedSegmentCellCursor, error) {
	cursor := &invertedSegmentCellCursor{
		ctx:     ctx,
		index:   idx,
		pageIdx: rootPage,
	}
	if err := cursor.advance(); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (c *invertedSegmentCellCursor) advance() error {
	for {
		if c.page != nil && c.cellIdx < len(c.page.InvertedSegmentPage.Cells) {
			c.cell = c.page.InvertedSegmentPage.Cells[c.cellIdx]
			c.cellIdx++
			c.ok = true
			return nil
		}
		if c.page != nil {
			c.pageIdx = c.page.InvertedSegmentPage.Header.NextPage
		}
		if c.pageIdx == 0 {
			c.ok = false
			return nil
		}
		page, err := c.index.pager.ReadPage(c.ctx, c.pageIdx)
		if err != nil {
			return fmt.Errorf("read inverted segment page %d: %w", c.pageIdx, err)
		}
		if page.InvertedSegmentPage == nil {
			return fmt.Errorf("inverted segment page %d has unexpected page type", c.pageIdx)
		}
		c.page = page
		c.cellIdx = 0
	}
}

func (idx *logStructuredInvertedIndex) mergeRowIDSegmentRun(
	ctx context.Context,
	segments []invertedSegmentDescriptor,
) (invertedSegmentWriteResult, error) {
	segments = append([]invertedSegmentDescriptor(nil), segments...)
	sort.SliceStable(segments, func(i, j int) bool {
		return segments[i].Generation < segments[j].Generation
	})
	cursors := make([]*invertedSegmentCellCursor, 0, len(segments))
	for _, segment := range segments {
		cursor, err := idx.newSegmentCellCursor(ctx, segment.RootPage)
		if err != nil {
			return invertedSegmentWriteResult{}, err
		}
		cursors = append(cursors, cursor)
	}

	writer := idx.newSegmentWriter(ctx)
	for {
		term, ok := nextRowIDSegmentTerm(cursors)
		if !ok {
			break
		}
		state := rowIDSegmentTermState{}
		for i, cursor := range cursors {
			for cursor.ok && cursor.cell.Term == term {
				if err := idx.applyRowIDSegmentCellState(segments[i], cursor.cell, &state); err != nil {
					return invertedSegmentWriteResult{}, err
				}
				if err := cursor.advance(); err != nil {
					return invertedSegmentWriteResult{}, err
				}
			}
		}
		if err := appendRowIDSegmentTermState(writer, term, state); err != nil {
			return invertedSegmentWriteResult{}, err
		}
	}
	return writer.finish()
}

func nextRowIDSegmentTerm(cursors []*invertedSegmentCellCursor) (string, bool) {
	var term string
	ok := false
	for _, cursor := range cursors {
		if !cursor.ok || ok && cursor.cell.Term >= term {
			continue
		}
		term = cursor.cell.Term
		ok = true
	}
	return term, ok
}

func (idx *logStructuredInvertedIndex) applyRowIDSegmentCellState(
	segment invertedSegmentDescriptor,
	cell invertedSegmentCell,
	state *rowIDSegmentTermState,
) error {
	kind := segment.Kind
	if kind == invertedSegmentKindMixed {
		kind = cell.Kind
	}
	if kind == invertedSegmentKindInsert && state.inserts == nil {
		state.inserts = make(map[RowID]struct{})
	}
	if kind == invertedSegmentKindDelete && state.deletes == nil {
		state.deletes = make(map[RowID]struct{})
	}
	mode, err := forEachInvertedPostingRowID(cell.Block.Payload, func(rowID RowID) error {
		switch kind {
		case invertedSegmentKindInsert:
			applyRowIDSegmentStateInsert(*state, rowID)
		case invertedSegmentKindDelete:
			applyRowIDSegmentStateDelete(*state, rowID)
		default:
			return fmt.Errorf("unknown inverted segment kind %d", kind)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if mode != idx.Mode() {
		return fmt.Errorf("inverted segment block uses posting mode %d, expected %d", mode, idx.Mode())
	}
	return nil
}

func appendRowIDSegmentTermState(
	writer *invertedSegmentWriter,
	term string,
	state rowIDSegmentTermState,
) error {
	deleteCells, _, err := segmentCellsForRowIDs(invertedSegmentKindDelete, term, sortedRowIDsFromSet(state.deletes))
	if err != nil {
		return err
	}
	insertCells, _, err := segmentCellsForRowIDs(invertedSegmentKindInsert, term, sortedRowIDsFromSet(state.inserts))
	if err != nil {
		return err
	}
	for _, cell := range deleteCells {
		if err := writer.append(cell); err != nil {
			return err
		}
	}
	for _, cell := range insertCells {
		if err := writer.append(cell); err != nil {
			return err
		}
	}
	return nil
}
