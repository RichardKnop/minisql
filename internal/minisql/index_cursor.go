package minisql

import (
	"context"
	"errors"
	"fmt"
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

// FindRowIDs ...
func (ui *Index[T]) FindRowIDs(ctx context.Context, keyAny any) ([]RowID, error) {
	key, ok := keyAny.(T)
	if !ok {
		return nil, fmt.Errorf("invalid key type: %T", keyAny)
	}

	rootPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return nil, err
	}

	cursor, ok, err := ui.Seek(ctx, rootPage, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrNotFound, key)
	}

	page, err := cursor.Index.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return nil, fmt.Errorf("read page: %w", err)
	}
	node := page.IndexNode.(*IndexNode[T])
	if cursor.CellIdx >= node.Header.Keys {
		return nil, fmt.Errorf("invalid cell index: %d", cursor.CellIdx)
	}

	if node.Cells[cursor.CellIdx].unique {
		return []RowID{node.Cells[cursor.CellIdx].UniqueRowID}, nil
	}

	if len(node.Cells[cursor.CellIdx].RowIDs) == 0 {
		return nil, fmt.Errorf("no row IDs for key: %v", key)
	}

	return node.Cells[cursor.CellIdx].RowIDs, nil
}

// Seek ...
func (ui *Index[T]) Seek(ctx context.Context, page *Page, keyAny any) (IndexCursor[T], bool, error) {
	key, ok := keyAny.(T)
	if !ok {
		return IndexCursor[T]{}, false, fmt.Errorf("invalid key type: %T", keyAny)
	}

	i := uint32(0)
	node := page.IndexNode.(*IndexNode[T])

	for i < node.Header.Keys && compare(key, node.Cells[i].Key) > 0 {
		i += 1
	}
	if i < node.Header.Keys && compare(key, node.Cells[i].Key) == 0 {
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
