package minisql

import (
	"context"
	"fmt"
)

type IndexCursor struct {
	PageIdx uint32
	CellIdx uint32
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
