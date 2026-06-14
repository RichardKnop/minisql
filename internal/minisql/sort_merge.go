package minisql

import (
	"container/heap"
	"fmt"
	"os"
)

type mergeItem struct {
	row       Row
	readerIdx int
}

type mergeHeap struct {
	items   []mergeItem
	orderBy []OrderBy
}

func (h *mergeHeap) Len() int { return len(h.items) }

func (h *mergeHeap) Less(i, j int) bool {
	for _, clause := range h.orderBy {
		vi, foundI, _ := evalOrderByValue(clause, h.items[i].row)
		vj, foundJ, _ := evalOrderByValue(clause, h.items[j].row)
		if !foundI || !foundJ {
			continue
		}
		cmp := compareValues(vi, vj)
		if cmp == 0 {
			continue
		}
		if clause.Direction == Desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

func (h *mergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

// Push appends an item to the heap's backing slice; called by container/heap internals.
func (h *mergeHeap) Push(x any) { h.items = append(h.items, x.(mergeItem)) }

// Pop removes and returns the minimum item from the heap's backing slice; called by container/heap internals.
func (h *mergeHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

// externalSortMerge N-way merges sorted on-disk run files with any remaining
// in-memory rows, producing a single fully-sorted []Row.
// tmpPaths are removed from disk before this function returns.
func (t *Table) externalSortMerge(tmpPaths []string, inMemory []Row, columns []Column, orderBy []OrderBy) ([]Row, error) {
	readers := make([]*runReader, 0, len(tmpPaths))
	for _, path := range tmpPaths {
		rr, err := newRunReader(path, columns)
		if err != nil {
			for _, r := range readers {
				_ = r.close()
			}
			return nil, err
		}
		readers = append(readers, rr)
	}
	defer func() {
		for _, r := range readers {
			_ = r.close()
		}
		for _, p := range tmpPaths {
			_ = os.Remove(p)
		}
	}()

	h := &mergeHeap{orderBy: orderBy, items: make([]mergeItem, 0, len(readers)+1)}
	heap.Init(h)

	for i, rr := range readers {
		if !rr.Done() {
			heap.Push(h, mergeItem{row: rr.Row(), readerIdx: i})
		}
	}

	// Treat the in-memory slice as an additional virtual reader.
	const inMemReaderIdx = -1
	inMemIdx := 0
	if len(inMemory) > 0 {
		heap.Push(h, mergeItem{row: inMemory[0], readerIdx: inMemReaderIdx})
		inMemIdx = 1
	}

	var result []Row
	for h.Len() > 0 {
		item := heap.Pop(h).(mergeItem)
		result = append(result, item.row)

		if item.readerIdx == inMemReaderIdx {
			if inMemIdx < len(inMemory) {
				heap.Push(h, mergeItem{row: inMemory[inMemIdx], readerIdx: inMemReaderIdx})
				inMemIdx += 1
			}
		} else {
			rr := readers[item.readerIdx]
			rr.Next()
			if err := rr.Err(); err != nil {
				return nil, fmt.Errorf("sort merge: reader %d: %w", item.readerIdx, err)
			}
			if !rr.Done() {
				heap.Push(h, mergeItem{row: rr.Row(), readerIdx: item.readerIdx})
			}
		}
	}

	for i, rr := range readers {
		if err := rr.Err(); err != nil {
			return nil, fmt.Errorf("sort merge: reader %d final error: %w", i, err)
		}
	}

	return result, nil
}
