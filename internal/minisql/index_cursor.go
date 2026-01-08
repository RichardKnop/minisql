package minisql

import (
	"context"
	"fmt"
)

// TODO - currently this struct has no methods and only exists to group page and cell index.
// Consider merging its functionality into Index or adding useful methods.
type IndexCursor[T IndexKey] struct {
	Index   *Index[T]
	PageIdx PageIndex
	CellIdx uint32
}

var ErrNotFound = fmt.Errorf("not found")

func (ui *Index[T]) FindRowIDs(ctx context.Context, keyAny any) ([]RowID, error) {
	key, ok := keyAny.(T)
	if !ok {
		return nil, fmt.Errorf("invalid key type: %T", keyAny)
	}

	aRootPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return nil, err
	}

	aCursor, ok, err := ui.Seek(ctx, aRootPage, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrNotFound, key)
	}

	aPage, err := aCursor.Index.pager.ReadPage(ctx, aCursor.PageIdx)
	if err != nil {
		return nil, fmt.Errorf("read page: %w", err)
	}
	aNode := aPage.IndexNode.(*IndexNode[T])
	if aCursor.CellIdx >= aNode.Header.Keys {
		return nil, fmt.Errorf("invalid cell index: %d", aCursor.CellIdx)
	}

	if len(aNode.Cells[aCursor.CellIdx].RowIDs) == 0 {
		return nil, fmt.Errorf("no row IDs for key: %v", key)
	}

	return aNode.Cells[aCursor.CellIdx].RowIDs, nil
}

func (ui *Index[T]) Seek(ctx context.Context, aPage *Page, keyAny any) (IndexCursor[T], bool, error) {
	key, ok := keyAny.(T)
	if !ok {
		return IndexCursor[T]{}, false, fmt.Errorf("invalid key type: %T", keyAny)
	}

	i := uint32(0)
	aNode := aPage.IndexNode.(*IndexNode[T])

	for i < aNode.Header.Keys && compare(key, aNode.Cells[i].Key) > 0 {
		i++
	}
	if i < aNode.Header.Keys && compare(key, aNode.Cells[i].Key) == 0 {
		return IndexCursor[T]{
			Index:   ui,
			PageIdx: aPage.Index,
			CellIdx: uint32(i),
		}, true, nil
	}
	if aNode.Header.IsLeaf {
		return IndexCursor[T]{}, false, nil
	}
	childIdx, err := aNode.Child(uint32(i))
	if err != nil {
		return IndexCursor[T]{}, false, fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ReadPage(ctx, childIdx)
	if err != nil {
		return IndexCursor[T]{}, false, fmt.Errorf("get child page: %w", err)
	}
	return ui.Seek(ctx, childPage, key)
}

func (ui *Index[T]) SeekWithPrefix(ctx context.Context, aPage *Page, prefixAny any, prefixColumns int) (IndexCursor[T], bool, error) {
	prefix, ok := prefixAny.(T)
	if !ok {
		return IndexCursor[T]{}, false, fmt.Errorf("invalid prefix type: %T", prefixAny)
	}
	// We can only seek by prefix for CompositeKey types
	if _, ok := any(prefix).(CompositeKey); !ok {
		return IndexCursor[T]{}, false, fmt.Errorf("SeekWithPrefix only supports CompositeKey prefix, got: %T", prefixAny)
	}

	i := uint32(0)
	aNode := aPage.IndexNode.(*IndexNode[T])

	for i < aNode.Header.Keys && compare(prefixAny.(CompositeKey), any(aNode.Cells[i].Key).(CompositeKey).Prefix(prefixColumns)) > 0 {
		i++
	}
	if i < aNode.Header.Keys && compare(prefixAny.(CompositeKey), any(aNode.Cells[i].Key).(CompositeKey).Prefix(prefixColumns)) == 0 {
		return IndexCursor[T]{
			Index:   ui,
			PageIdx: aPage.Index,
			CellIdx: uint32(i),
		}, true, nil
	}
	if aNode.Header.IsLeaf {
		return IndexCursor[T]{}, false, nil
	}
	childIdx, err := aNode.Child(uint32(i))
	if err != nil {
		return IndexCursor[T]{}, false, fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ReadPage(ctx, childIdx)
	if err != nil {
		return IndexCursor[T]{}, false, fmt.Errorf("get child page: %w", err)
	}
	return ui.SeekWithPrefix(ctx, childPage, prefixAny, prefixColumns)
}

// Used for autoincrement primary keys
func (ui *Index[T]) SeekLastKey(ctx context.Context, pageIdx PageIndex) (any, error) {
	aPage, err := ui.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("seek next row ID: %w", err)
	}
	aNode := aPage.IndexNode.(*IndexNode[T])
	if aNode.Header.IsLeaf == false {
		return ui.SeekLastKey(ctx, aNode.Header.RightChild)
	}
	if aNode.Header.Keys == 0 {
		return int64(0), nil
	}
	return aNode.Cells[aNode.Header.Keys-1].Key, nil
}
