package minisql

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

type parallelScanResult struct {
	row Row
	err error
}

type parallelRowViewScanResult struct {
	view RowView
	err  error
}

// drainParallelScanCh reads and discards all remaining values from ch until it is
// closed.  Called after cancel() to let workers unblock their channel sends and
// exit cleanly before the consumer returns.
func drainParallelScanCh(ch <-chan parallelScanResult) {
	for v := range ch {
		_ = v
	}
}

func drainParallelRowViewScanCh(ch <-chan parallelRowViewScanResult) {
	for v := range ch {
		_ = v
	}
}

// leafPageList walks the leaf chain from the leftmost leaf and returns every leaf PageIndex.
func (t *Table) leafPageList(ctx context.Context) ([]PageIndex, error) {
	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return nil, err
	}
	if cursor.EndOfTable {
		return nil, nil
	}

	var pages []PageIndex
	pageIdx := cursor.PageIdx
	for {
		pages = append(pages, pageIdx)
		page, err := t.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return nil, fmt.Errorf("leaf page list: %w", err)
		}
		if page.LeafNode.Header.NextLeaf == 0 {
			break
		}
		pageIdx = page.LeafNode.Header.NextLeaf
	}
	return pages, nil
}

// parallelSequentialScan partitions leaf pages across up to runtime.NumCPU() goroutines.
// Each worker reads its own page slice independently; matching rows are fanned in through
// a buffered channel and delivered to out in arrival order (not row-ID order).
func (t *Table) parallelSequentialScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error) error {
	pages, err := t.leafPageList(ctx)
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	fullMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)

	filterMask := fullMask
	twoPhase := tableFilter != nil
	if twoPhase {
		filterMask = filterOnlyMask(t.Columns, scan.Filters)
		twoPhase = maskHasTrue(filterMask) && !masksEqual(filterMask, fullMask)
	}

	numWorkers := min(runtime.NumCPU(), len(pages))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan parallelScanResult, numWorkers*16)

	var wg sync.WaitGroup
	pagesPerWorker := (len(pages) + numWorkers - 1) / numWorkers
	for i := range numWorkers {
		start := i * pagesPerWorker
		end := min(start+pagesPerWorker, len(pages))
		if start >= len(pages) {
			break
		}
		wg.Add(1)
		go func(workerPages []PageIndex) {
			defer wg.Done()
			t.parallelScanWorker(ctx, workerPages, fullMask, filterMask, twoPhase, tableFilter, ch)
		}(pages[start:end])
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var received int64
	scanLimit := scan.ScanLimit
	for result := range ch {
		if result.err != nil {
			cancel()
			drainParallelScanCh(ch)
			return result.err
		}
		if err := out(result.row); err != nil {
			cancel()
			drainParallelScanCh(ch)
			return err
		}
		received += 1
		if scanLimit > 0 && received >= scanLimit {
			cancel()
			drainParallelScanCh(ch)
			return nil
		}
	}

	return nil
}

func (t *Table) parallelSequentialRowViewIteratorFactory(
	ctx context.Context,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) (func() RowViewIterator, error) {
	pages, err := t.leafPageList(ctx)
	if err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return func() RowViewIterator {
			return NewRowViewIterator(func(context.Context) (RowView, error) {
				return RowView{}, ErrNoMoreRows
			})
		}, nil
	}

	numWorkers := min(runtime.NumCPU(), len(pages))
	pagesPerWorker := (len(pages) + numWorkers - 1) / numWorkers

	return func() RowViewIterator {
		iterCtx, cancel := context.WithCancel(ctx)
		ch := make(chan parallelRowViewScanResult, numWorkers*16)

		var wg sync.WaitGroup
		for i := range numWorkers {
			start := i * pagesPerWorker
			end := min(start+pagesPerWorker, len(pages))
			if start >= len(pages) {
				break
			}
			wg.Add(1)
			go func(workerPages []PageIndex) {
				defer wg.Done()
				t.parallelRowViewScanWorker(iterCtx, workerPages, tableFilter, ch)
			}(pages[start:end])
		}

		go func() {
			wg.Wait()
			close(ch)
		}()

		iterRemaining := remaining
		iterOffset := offset
		return newRowViewIteratorWithClose(func(nextCtx context.Context) (RowView, error) {
			if err := nextCtx.Err(); err != nil {
				cancel()
				drainParallelRowViewScanCh(ch)
				return RowView{}, err
			}
			if hasLimit && iterRemaining == 0 {
				cancel()
				drainParallelRowViewScanCh(ch)
				return RowView{}, ErrNoMoreRows
			}

			for result := range ch {
				if result.err != nil {
					cancel()
					drainParallelRowViewScanCh(ch)
					return RowView{}, result.err
				}
				if hasOffset && iterOffset > 0 {
					iterOffset -= 1
					continue
				}
				if hasLimit {
					iterRemaining -= 1
				}
				return result.view, nil
			}
			cancel()
			return RowView{}, ErrNoMoreRows
		}, func() error {
			cancel()
			drainParallelRowViewScanCh(ch)
			return nil
		})
	}, nil
}

func (t *Table) parallelRowViewScanWorker(
	ctx context.Context,
	pages []PageIndex,
	tableFilter func(context.Context, RowView) (bool, error),
	ch chan<- parallelRowViewScanResult,
) {
	send := func(r parallelRowViewScanResult) bool {
		select {
		case ch <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}

	for _, pageIdx := range pages {
		if ctx.Err() != nil {
			return
		}

		page, err := t.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			send(parallelRowViewScanResult{err: fmt.Errorf("parallel row view scan worker: %w", err)})
			return
		}

		for i := range page.LeafNode.Header.Cells {
			if ctx.Err() != nil {
				return
			}

			view := NewRowView(t.Columns, page.LeafNode.Cells[i])
			if tableFilter != nil {
				ok, err := tableFilter(ctx, view)
				if err != nil {
					send(parallelRowViewScanResult{err: err})
					return
				}
				if !ok {
					continue
				}
			}
			if !send(parallelRowViewScanResult{view: view}) {
				return
			}
		}
	}
}

func (t *Table) parallelScanWorker(
	ctx context.Context,
	pages []PageIndex,
	fullMask, filterMask []bool,
	twoPhase bool,
	tableFilter func(Row) (bool, error),
	ch chan<- parallelScanResult,
) {
	send := func(r parallelScanResult) bool {
		select {
		case ch <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}

	for _, pageIdx := range pages {
		if ctx.Err() != nil {
			return
		}

		page, err := t.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			send(parallelScanResult{err: fmt.Errorf("parallel scan worker: %w", err)})
			return
		}

		for i := range page.LeafNode.Header.Cells {
			if ctx.Err() != nil {
				return
			}

			cell := page.LeafNode.Cells[i]
			view := NewRowView(t.Columns, cell)

			if !twoPhase {
				row, err := view.Materialize(fullMask)
				if err != nil {
					send(parallelScanResult{err: err})
					return
				}
				if tableFilter != nil {
					ok, err := tableFilter(row)
					if err != nil {
						send(parallelScanResult{err: err})
						return
					}
					if !ok {
						continue
					}
				}
				row, err = row.readOverflowTexts(ctx, t.pager)
				if err != nil {
					send(parallelScanResult{err: err})
					return
				}
				if !send(parallelScanResult{row: row}) {
					return
				}
				continue
			}

			// Two-phase: decode only predicate columns first to skip non-matching rows cheaply.
			filterRow, err := view.Materialize(filterMask)
			if err != nil {
				send(parallelScanResult{err: err})
				return
			}
			ok, err := tableFilter(filterRow)
			if err != nil {
				send(parallelScanResult{err: err})
				return
			}
			if !ok {
				continue
			}

			row, err := view.MaterializeWithOverflow(ctx, t.pager, fullMask)
			if err != nil {
				send(parallelScanResult{err: err})
				return
			}
			if !send(parallelScanResult{row: row}) {
				return
			}
		}
	}
}
