package minisql

import (
	"testing"
)

// FuzzPageUnmarshal verifies that the B-tree page deserialization paths never
// panic on arbitrary byte input. The only invariant enforced is no-panic:
// every Unmarshal must return either a valid struct or an error.
//
// Run for a fixed time during development:
//
//	go test -fuzz=FuzzPageUnmarshal -fuzztime=60s ./internal/minisql/
//
// Seeds are run as ordinary unit tests on every `go test` invocation.
func FuzzPageUnmarshal(f *testing.F) {
	// --- Seed: minimal valid leaf page (two INT8 rows) ---
	f.Add(makeLeafSeed(), PageTypeLeaf)

	// --- Seed: minimal valid internal page (one separator key) ---
	f.Add(makeInternalSeed(), PageTypeInternal)

	// --- Seed: minimal valid overflow page ---
	f.Add(makeOverflowSeed(), PageTypeOverflow)

	// --- Seed: minimal valid free page ---
	f.Add(makeFreeSeed(), PageTypeFree)

	// --- Seed: zero-length buffer (always an error, must not panic) ---
	f.Add([]byte{}, PageTypeLeaf)

	// --- Seed: single byte (too short for any header) ---
	f.Add([]byte{PageTypeLeaf}, PageTypeLeaf)
	f.Add([]byte{PageTypeInternal}, PageTypeInternal)

	f.Fuzz(func(t *testing.T, buf []byte, hint byte) {
		// Exercise every Unmarshal path. None may panic regardless of input.

		// Cell (innermost unit — used by LeafNode)
		c := &Cell{}
		_, _ = c.Unmarshal(buf)

		// LeafNode
		leaf := NewLeafNode()
		_, _ = leaf.Unmarshal(buf)

		// InternalNode
		internal := new(InternalNode)
		_, _ = internal.Unmarshal(buf)

		// OverflowPage
		overflow := new(OverflowPage)
		_ = overflow.Unmarshal(buf)

		// FreePage
		free := new(FreePage)
		_ = free.Unmarshal(buf)

		// tablePager dispatcher: routes on the first byte, same as on-disk reads.
		// Use hint to force each page-type branch even when buf starts differently.
		if len(buf) > 0 {
			// Drive the dispatcher with the original buf (any leading byte).
			dispatchUnmarshal(buf)

			// Also drive it with hint as the leading type byte so the fuzzer can
			// explore the less-common branches (free, overflow) more easily.
			patched := make([]byte, len(buf))
			copy(patched, buf)
			patched[0] = hint
			dispatchUnmarshal(patched)
		}
	})
}

// dispatchUnmarshal replicates the tablePager.unmarshal dispatcher for a
// non-root page (pageIdx > 0, so no RootPageConfigSize prefix is skipped).
func dispatchUnmarshal(buf []byte) {
	if len(buf) == 0 {
		return
	}
	switch buf[0] {
	case PageTypeLeaf:
		leaf := NewLeafNode()
		_, _ = leaf.Unmarshal(buf)
	case PageTypeInternal:
		internal := new(InternalNode)
		_, _ = internal.Unmarshal(buf)
	case PageTypeOverflow:
		overflow := new(OverflowPage)
		_ = overflow.Unmarshal(buf)
	case PageTypeFree:
		free := new(FreePage)
		_ = free.Unmarshal(buf)
	}
}

// --- seed helpers -----------------------------------------------------------

// makeLeafSeed builds a valid 4096-byte leaf-page buffer containing two INT8
// cells. This gives the fuzzer a realistic starting point.
func makeLeafSeed() []byte {
	cells := []Cell{
		makeInt8Cell(1, 100),
		makeInt8Cell(2, 200),
	}
	leaf := &LeafNode{
		Header: LeafNodeHeader{
			Header: Header{IsInternal: false, IsRoot: true, Parent: 0},
			Cells:  uint32(len(cells)),
		},
		Cells: cells,
	}
	buf := make([]byte, PageSize)
	_ = leaf.Marshal(buf)
	return buf
}

// makeInternalSeed builds a valid 4096-byte internal-page buffer with one
// separator key and two child pointers.
func makeInternalSeed() []byte {
	node := NewInternalNode()
	node.Header.IsRoot = true
	node.Header.KeysNum = 1
	node.Header.RightChild = PageIndex(2)
	node.ICells[0] = ICell{Key: RowID(50), Child: PageIndex(1)}
	buf := make([]byte, PageSize)
	_ = node.Marshal(buf)
	return buf
}

// makeOverflowSeed builds a valid 4096-byte overflow page with a small payload.
func makeOverflowSeed() []byte {
	data := []byte("hello overflow world")
	op := &OverflowPage{
		Header: OverflowPageHeader{
			NextPage: 0,
			DataSize: uint32(len(data)),
		},
		Data: data,
	}
	buf := make([]byte, PageSize)
	_ = op.Marshal(buf)
	return buf
}

// makeFreeSeed builds a valid 4096-byte free page.
func makeFreeSeed() []byte {
	fp := &FreePage{NextFreePage: 0}
	buf := make([]byte, PageSize)
	_ = fp.Marshal(buf)
	return buf
}

// makeInt8Cell constructs a self-describing Cell with a single INT8 column.
func makeInt8Cell(key RowID, value int64) Cell {
	valueBuf := make([]byte, 8)
	marshalInt64(valueBuf, value, 0)
	return Cell{
		NullBitmask: 0,
		Key:         key,
		ColumnCount: 1,
		TypeCodes:   []byte{byte(TypeCodeInt8)},
		Value:       valueBuf,
	}
}
