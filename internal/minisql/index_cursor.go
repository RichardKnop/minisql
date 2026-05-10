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

// ErrNotFound ...
var ErrNotFound = errors.New("not found")

// VisitRowIDs calls fn for each row ID stored under key, reading overflow pages
// one at a time so the caller never holds more than one page worth of IDs in memory.
// fn may return an error to stop iteration early; that error is returned unchanged.
func (ui *Index[T]) VisitRowIDs(ctx context.Context, keyAny any, fn func(RowID) error) error {
	key, ok := keyAny.(T)
	if !ok {
		return fmt.Errorf("invalid key type: %T", keyAny)
	}

	rootPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return err
	}

	cursor, ok, err := ui.Seek(ctx, rootPage, key)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %v", ErrNotFound, key)
	}

	page, err := cursor.Index.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return fmt.Errorf("read page: %w", err)
	}
	node := page.IndexNode.(*IndexNode[T])
	if cursor.CellIdx >= node.Header.Keys {
		return fmt.Errorf("invalid cell index: %d", cursor.CellIdx)
	}

	cell := node.Cells[cursor.CellIdx]

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

	overflowIdx := cell.Overflow
	for overflowIdx != 0 {
		overflowPage, err := cursor.Index.pager.ReadPage(ctx, overflowIdx)
		if err != nil {
			return fmt.Errorf("read index overflow page %d: %w", overflowIdx, err)
		}
		node := overflowPage.IndexOverflowNode
		for _, rowID := range node.RowIDs[:node.Header.ItemCount] {
			if err := fn(rowID); err != nil {
				return err
			}
		}
		overflowIdx = node.Header.NextPage
	}

	return nil
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

// Seek ...
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

// SeekWithPrefix ...
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
