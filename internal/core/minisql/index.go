package minisql

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type Index struct {
	Name        string
	Column      Column
	RootPageIdx uint32
	pager       IndexPager
	writeLock   *sync.RWMutex
	logger      *zap.Logger
}

func NewIndex(logger *zap.Logger, name string, column Column, pager IndexPager, rootPageIdx uint32) *Index {
	return &Index{
		Name:        name,
		Column:      column,
		RootPageIdx: rootPageIdx,
		pager:       pager,
		writeLock:   new(sync.RWMutex),
		logger:      logger,
	}
}

func (i *Index) Seek(ctx context.Context, pageIdx uint32) (*Cursor, uint64, error) {
	return nil, 0, fmt.Errorf("index seek not implemented")
}
