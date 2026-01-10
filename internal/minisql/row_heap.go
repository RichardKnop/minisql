package minisql

import (
	"container/heap"
)

// rowHeap implements a min-heap or max-heap for efficiently maintaining top-N rows
// This is used for optimizing ORDER BY ... LIMIT queries
// Instead of sorting all rows, we only keep the top N in a heap
type rowHeap struct {
	rows    []Row
	orderBy []OrderBy
	maxSize int
	table   *Table // needed for comparison logic
}

func newRowHeap(orderBy []OrderBy, maxSize int) *rowHeap {
	h := &rowHeap{
		rows:    make([]Row, 0, maxSize),
		orderBy: orderBy,
		maxSize: maxSize,
	}
	heap.Init(h)
	return h
}

// Implement heap.Interface
func (h *rowHeap) Len() int { return len(h.rows) }

func (h *rowHeap) Less(i, j int) bool {
	// For a min-heap that keeps largest elements, we reverse the comparison
	// This way, the smallest element is at the top and gets popped first
	for _, clause := range h.orderBy {
		valI, foundI := h.rows[i].GetValue(clause.Field.Name)
		valJ, foundJ := h.rows[j].GetValue(clause.Field.Name)

		if !foundI || !foundJ {
			continue
		}

		cmp := compareValues(valI, valJ)

		if cmp == 0 {
			continue // Equal, check next ORDER BY column
		}

		// For ASC: keep largest at top (min-heap inverted), so reverse comparison
		// For DESC: keep smallest at top (min-heap inverted), so keep normal comparison
		if clause.Direction == Desc {
			return cmp < 0
		}
		return cmp > 0
	}
	return false
}

func (h *rowHeap) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

func (h *rowHeap) Push(x interface{}) {
	h.rows = append(h.rows, x.(Row))
}

func (h *rowHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[0 : n-1]
	return x
}

// PushRow adds a row to the heap, maintaining max size
func (h *rowHeap) PushRow(row Row) {
	if len(h.rows) < h.maxSize {
		// Heap not full yet, just add
		heap.Push(h, row)
	} else {
		// Heap is full, check if new row should replace the root
		// For ASC order: root is largest in heap, replace if new row is smaller
		// For DESC order: root is smallest in heap, replace if new row is larger
		shouldReplace := false

		for _, clause := range h.orderBy {
			valRoot, foundRoot := h.rows[0].GetValue(clause.Field.Name)
			valNew, foundNew := row.GetValue(clause.Field.Name)

			if !foundRoot || !foundNew {
				continue
			}

			cmp := compareValues(valNew, valRoot)

			if cmp == 0 {
				continue // Equal, check next column
			}

			// For ASC: replace root if new < root (we keep smallest values)
			// For DESC: replace root if new > root (we keep largest values)
			if clause.Direction == Asc {
				shouldReplace = cmp < 0
			} else {
				shouldReplace = cmp > 0
			}
			break
		}

		if shouldReplace {
			h.rows[0] = row
			heap.Fix(h, 0)
		}
	}
}

// ExtractSorted returns all rows in sorted order
func (h *rowHeap) ExtractSorted() []Row {
	result := make([]Row, len(h.rows))

	// Extract in reverse order (heap gives us smallest first, we want largest)
	for i := len(h.rows) - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(Row)
	}

	return result
}
