package minisql

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// IndexCursor holds a position within an index, grouping page and cell index.
// TODO - currently this struct has no methods; consider merging into Index or adding useful methods.
type IndexCursor[T IndexKey] struct {
	Index   *Index[T]
	PageIdx PageIndex
	CellIdx uint32
}

type rowIDNextFunc func(context.Context) (RowID, error)

// ErrNotFound is returned by VisitRowIDs and related lookups when the requested
// key does not exist in the index.
var ErrNotFound = errors.New("not found")

// VisitRowIDs calls fn for each row ID stored under key, reading overflow pages
// one at a time so the caller never holds more than one page worth of IDs in memory.
// fn may return an error to stop iteration early; that error is returned unchanged.
func (ui *Index[T]) VisitRowIDs(ctx context.Context, keyAny any, fn func(RowID) error) error {
	key, cell, err := ui.pointCell(ctx, keyAny)
	if err != nil {
		return err
	}

	return ui.visitCellRowIDs(ctx, key, cell, fn)
}

func (ui *Index[T]) pointCell(ctx context.Context, keyAny any) (T, IndexCell[T], error) {
	key, ok := keyAny.(T)
	if !ok {
		return key, IndexCell[T]{}, fmt.Errorf("invalid key type: %T", keyAny)
	}

	rootPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return key, IndexCell[T]{}, err
	}

	cursor, ok, err := ui.Seek(ctx, rootPage, key)
	if err != nil {
		return key, IndexCell[T]{}, err
	}
	if !ok {
		return key, IndexCell[T]{}, fmt.Errorf("%w: %v", ErrNotFound, key)
	}

	page, err := cursor.Index.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return key, IndexCell[T]{}, fmt.Errorf("read page: %w", err)
	}
	node := page.IndexNode.(*IndexNode[T])
	if cursor.CellIdx >= node.Header.Keys {
		return key, IndexCell[T]{}, fmt.Errorf("invalid cell index: %d", cursor.CellIdx)
	}

	return key, node.Cells[cursor.CellIdx], nil
}

func (ui *Index[T]) visitCellRowIDs(ctx context.Context, key T, cell IndexCell[T], fn func(RowID) error) error {
	if cell.unique {
		return fn(cell.UniqueRowID)
	}
	if len(cell.RowIDs) == 0 {
		return fmt.Errorf("no row IDs for key: %v", key)
	}
	for _, rowID := range cell.RowIDs {
		if err := fn(rowID); err != nil {
			return err
		}
	}
	return visitOverflowRowIDs(ctx, ui.pager, cell.Overflow, fn)
}

// PointUniqueRowID returns the row ID for keyAny when this index cell is unique.
func (ui *Index[T]) PointUniqueRowID(ctx context.Context, keyAny any) (RowID, error) {
	key, cell, err := ui.pointCell(ctx, keyAny)
	if err != nil {
		return 0, err
	}
	if !cell.unique {
		return 0, fmt.Errorf("index cell for key %v is not unique", key)
	}
	return cell.UniqueRowID, nil
}

// PointRowIDIterator returns a pull iterator over row IDs stored under keyAny.
func (ui *Index[T]) PointRowIDIterator(ctx context.Context, keyAny any) (rowIDNextFunc, error) {
	key, cell, err := ui.pointCell(ctx, keyAny)
	if err != nil {
		return nil, err
	}
	if cell.unique {
		emitted := false
		return func(context.Context) (RowID, error) {
			if emitted {
				return 0, ErrNoMoreRows
			}
			emitted = true
			return cell.UniqueRowID, nil
		}, nil
	}
	if len(cell.RowIDs) == 0 {
		return nil, fmt.Errorf("no row IDs for key: %v", key)
	}

	return ui.pointRowIDIterator(cell.RowIDs, cell.Overflow), nil
}

func (ui *Index[T]) pointRowIDIterator(inlineRowIDs []RowID, overflowIdx PageIndex) rowIDNextFunc {
	var (
		inlineIdx       int
		overflowRowIDs  []RowID
		overflowRowIdx  int
		nextOverflowIdx = overflowIdx
	)
	return func(ctx context.Context) (RowID, error) {
		if inlineIdx < len(inlineRowIDs) {
			rowID := inlineRowIDs[inlineIdx]
			inlineIdx += 1
			return rowID, nil
		}
		for {
			if overflowRowIdx < len(overflowRowIDs) {
				rowID := overflowRowIDs[overflowRowIdx]
				overflowRowIdx += 1
				return rowID, nil
			}
			if nextOverflowIdx == 0 {
				return 0, ErrNoMoreRows
			}
			overflowPage, err := ui.pager.ReadPage(ctx, nextOverflowIdx)
			if err != nil {
				return 0, fmt.Errorf("read index overflow page %d: %w", nextOverflowIdx, err)
			}
			node := overflowPage.IndexOverflowNode
			overflowRowIDs = node.RowIDs[:node.Header.ItemCount]
			overflowRowIdx = 0
			nextOverflowIdx = node.Header.NextPage
		}
	}
}

// FindRowIDs returns all row IDs for key as a slice.
// For large non-unique secondary indexes prefer VisitRowIDs to avoid materialising the full list.
func (ui *Index[T]) FindRowIDs(ctx context.Context, keyAny any) ([]RowID, error) {
	var rowIDs []RowID
	err := ui.VisitRowIDs(ctx, keyAny, func(rowID RowID) error {
		rowIDs = append(rowIDs, rowID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rowIDs, nil
}

// Seek performs a binary-search descent from page to find the cell whose key
// equals keyAny. Returns the cursor and true if found, or the zero cursor and
// false if the key does not exist in the subtree rooted at page.
func (ui *Index[T]) Seek(ctx context.Context, page *Page, keyAny any) (IndexCursor[T], bool, error) {
	key, ok := keyAny.(T)
	if !ok {
		return IndexCursor[T]{}, false, fmt.Errorf("invalid key type: %T", keyAny)
	}

	node := page.IndexNode.(*IndexNode[T])

	// Binary search: first position where Cells[pos].Key >= key.
	pos := sort.Search(int(node.Header.Keys), func(i int) bool {
		return compare(node.Cells[i].Key, key) >= 0
	})
	if pos < int(node.Header.Keys) && compare(node.Cells[pos].Key, key) == 0 {
		return IndexCursor[T]{
			Index:   ui,
			PageIdx: page.Index,
			CellIdx: uint32(pos),
		}, true, nil
	}
	if node.Header.IsLeaf {
		return IndexCursor[T]{}, false, nil
	}
	childIdx, err := node.Child(uint32(pos))
	if err != nil {
		return IndexCursor[T]{}, false, fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ReadPage(ctx, childIdx)
	if err != nil {
		return IndexCursor[T]{}, false, fmt.Errorf("get child page: %w", err)
	}
	return ui.Seek(ctx, childPage, key)
}

// SeekWithPrefix descends from page to find the first cell whose CompositeKey
// matches prefixAny on its first prefixColumns columns. Only CompositeKey
// indexes support this operation; all other key types return an error.
func (ui *Index[T]) SeekWithPrefix(ctx context.Context, page *Page, prefixAny any, prefixColumns int) (IndexCursor[T], bool, error) {
	prefix, ok := prefixAny.(T)
	if !ok {
		return IndexCursor[T]{}, false, fmt.Errorf("invalid prefix type: %T", prefixAny)
	}
	// We can only seek by prefix for CompositeKey types
	if _, ok := any(prefix).(CompositeKey); !ok {
		return IndexCursor[T]{}, false, fmt.Errorf("SeekWithPrefix only supports CompositeKey prefix, got: %T", prefixAny)
	}

	i := uint32(0)
	node := page.IndexNode.(*IndexNode[T])

	for i < node.Header.Keys && compare(prefixAny.(CompositeKey), any(node.Cells[i].Key).(CompositeKey).Prefix(prefixColumns)) > 0 {
		i += 1
	}
	if i < node.Header.Keys && compare(prefixAny.(CompositeKey), any(node.Cells[i].Key).(CompositeKey).Prefix(prefixColumns)) == 0 {
		return IndexCursor[T]{
			Index:   ui,
			PageIdx: page.Index,
			CellIdx: uint32(i),
		}, true, nil
	}
	if node.Header.IsLeaf {
		return IndexCursor[T]{}, false, nil
	}
	childIdx, err := node.Child(uint32(i))
	if err != nil {
		return IndexCursor[T]{}, false, fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ReadPage(ctx, childIdx)
	if err != nil {
		return IndexCursor[T]{}, false, fmt.Errorf("get child page: %w", err)
	}
	return ui.SeekWithPrefix(ctx, childPage, prefixAny, prefixColumns)
}

// SeekLastKey returns the largest key in the index, used for autoincrement primary keys.
func (ui *Index[T]) SeekLastKey(ctx context.Context, pageIdx PageIndex) (any, error) {
	page, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("seek next row ID: %w", err)
	}
	node := page.IndexNode.(*IndexNode[T])
	if !node.Header.IsLeaf {
		return ui.SeekLastKey(ctx, node.Header.RightChild)
	}
	if node.Header.Keys == 0 {
		return int64(0), nil
	}
	return node.Cells[node.Header.Keys-1].Key, nil
}
