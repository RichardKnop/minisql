package minisql

import (
	"context"
	"fmt"
)

type IndexCursor struct {
	PageIdx PageIndex
	CellIdx uint32
}

var ErrNotFound = fmt.Errorf("not found")

func (ui *UniqueIndex[T]) Find(ctx context.Context, keyAny any) (RowID, error) {
	key, ok := keyAny.(T)
	if !ok {
		return 0, fmt.Errorf("invalid key type: %T", keyAny)
	}

	aRootPage, err := ui.pager.ReadPage(ctx, ui.GetRootPageIdx())
	if err != nil {
		return 0, err
	}

	aCursor, ok, err := ui.Seek(ctx, aRootPage, key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("%w: %v", ErrNotFound, key)
	}

	aPage, err := ui.pager.ReadPage(ctx, aCursor.PageIdx)
	if err != nil {
		return 0, fmt.Errorf("read page: %w", err)
	}
	aNode := aPage.IndexNode.(*IndexNode[T])
	if aCursor.CellIdx >= aNode.Header.Keys {
		return 0, fmt.Errorf("invalid cell index: %d", aCursor.CellIdx)
	}
	return RowID(aNode.Cells[aCursor.CellIdx].RowID), nil
}

func (ui *UniqueIndex[T]) Seek(ctx context.Context, aPage *Page, keyAny any) (IndexCursor, bool, error) {
	key, ok := keyAny.(T)
	if !ok {
		return IndexCursor{}, false, fmt.Errorf("invalid key type: %T", keyAny)
	}

	i := uint32(0)
	aNode := aPage.IndexNode.(*IndexNode[T])

	for i < aNode.Header.Keys && key > aNode.Cells[i].Key {
		i++
	}
	if i < aNode.Header.Keys && key == aNode.Cells[i].Key {
		return IndexCursor{
			PageIdx: aPage.Index,
			CellIdx: uint32(i),
		}, true, nil
	}
	if aNode.Header.IsLeaf {
		return IndexCursor{}, false, nil
	}
	childIdx, err := aNode.Child(uint32(i))
	if err != nil {
		return IndexCursor{}, false, fmt.Errorf("get child: %w", err)
	}
	childPage, err := ui.pager.ReadPage(ctx, childIdx)
	if err != nil {
		return IndexCursor{}, false, fmt.Errorf("get child page: %w", err)
	}
	return ui.Seek(ctx, childPage, key)
}

func (ui *UniqueIndex[T]) SeekLastKey(ctx context.Context, pageIdx PageIndex) (any, error) {
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
