package minisql

import (
	"context"
	"fmt"
)

type invertedSegmentWriteResult struct {
	rootPage     PageIndex
	postingCount uint32
	kind         byte
	firstTerm    string
	lastTerm     string
}

type invertedSegmentWriter struct {
	ctx         context.Context
	index       *logStructuredInvertedIndex
	currentPage *Page
	currentSize uint64
	cellCount   int
	result      invertedSegmentWriteResult
}

func (idx *logStructuredInvertedIndex) newSegmentWriter(ctx context.Context) *invertedSegmentWriter {
	return &invertedSegmentWriter{
		ctx:   ctx,
		index: idx,
	}
}

func (w *invertedSegmentWriter) append(cell invertedSegmentCell) error {
	cellSize := cell.size()
	const segmentPageUsable = uint64(PageSize - pageChecksumSize)
	if (invertedSegmentPageHeader{}).size()+2+cellSize > segmentPageUsable {
		return fmt.Errorf("inverted segment cell for term %q exceeds page size", cell.Term)
	}
	if w.currentPage == nil {
		if err := w.appendPage(); err != nil {
			return err
		}
	}
	if w.currentSize+2+cellSize > segmentPageUsable {
		if err := w.appendPage(); err != nil {
			return err
		}
	}
	w.currentPage.InvertedSegmentPage.Cells = append(w.currentPage.InvertedSegmentPage.Cells, cell)
	w.currentSize += 2 + cellSize
	w.updateResult(cell)
	return nil
}

func (w *invertedSegmentWriter) finish() (invertedSegmentWriteResult, error) {
	if w.cellCount == 0 {
		return invertedSegmentWriteResult{}, fmt.Errorf("cannot write empty inverted segment")
	}
	return w.result, nil
}

func (w *invertedSegmentWriter) appendPage() error {
	nextPage, err := w.index.newSegmentPage(w.ctx)
	if err != nil {
		return err
	}
	if w.currentPage == nil {
		w.result.rootPage = nextPage.Index
	} else {
		w.currentPage.InvertedSegmentPage.Header.NextPage = nextPage.Index
	}
	w.currentPage = nextPage
	w.currentSize = currentSegmentPageSize(nextPage.InvertedSegmentPage)
	return nil
}

func (w *invertedSegmentWriter) updateResult(cell invertedSegmentCell) {
	w.result.postingCount += cell.PostingCount
	if w.cellCount == 0 {
		w.result.kind = cell.Kind
		w.result.firstTerm = cell.Term
		w.result.lastTerm = cell.Term
		w.cellCount++
		return
	}
	if cell.Kind != w.result.kind {
		w.result.kind = invertedSegmentKindMixed
	}
	if cell.Term < w.result.firstTerm {
		w.result.firstTerm = cell.Term
	}
	if cell.Term > w.result.lastTerm {
		w.result.lastTerm = cell.Term
	}
	w.cellCount++
}
