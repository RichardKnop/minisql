package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIndex_NonUnique_Insert(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		aColumn        = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager     = aPager.ForIndex([]Column{aColumn}, false)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), aPager, nil)
		txPager        = NewTransactionalPager(indexPager, txManager, testTableName, "test_index")
	)
	anIndex, err := NewNonUniqueIndex[int64](testLogger, txManager, "test_index", []Column{aColumn}, txPager, 0)
	require.NoError(t, err)

	var (
		key            = int64(1)
		rowID          = RowID(101)
		insertedKeys   = make([]int64, 0)
		insertedRowIDs = make([]RowID, 0)
	)

	t.Run("Insert max inline row IDs, should not overflow", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for range MaxInlineRowIDs {
				if err := anIndex.Insert(ctx, key, rowID); err != nil {
					return err
				}
				insertedRowIDs = append(insertedRowIDs, rowID)
				rowID += 1
			}
			insertedKeys = append(insertedKeys, key)
			return nil
		})
		require.NoError(t, err)

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
		)

		assert.Equal(t, 4, int(rootNode.Cells[0].InlineRowIDs))
		assert.Equal(t, []RowID{101, 102, 103, 104}, rootNode.Cells[0].RowIDs)
		assert.Equal(t, 0, int(rootNode.Cells[0].Overflow))
	})

	t.Run("When inline row IDs are maxed, create an overflow page", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			if err := anIndex.Insert(ctx, key, rowID); err != nil {
				return err
			}
			insertedRowIDs = append(insertedRowIDs, rowID)
			rowID += 1
			return nil
		})
		require.NoError(t, err)

		var (
			rootNode     = aPager.pages[0].IndexNode.(*IndexNode[int64])
			overflowNode = aPager.pages[rootNode.Cells[0].Overflow].IndexOverflowNode
		)

		assert.Equal(t, 4, int(rootNode.Cells[0].InlineRowIDs))
		assert.Equal(t, []RowID{101, 102, 103, 104}, rootNode.Cells[0].RowIDs)
		assert.Equal(t, 1, int(rootNode.Cells[0].Overflow))

		assert.Equal(t, 0, int(overflowNode.Header.NextPage))
		assert.Equal(t, 1, int(overflowNode.Header.ItemCount))
		assert.Equal(t, []RowID{105}, overflowNode.RowIDs)
	})

	t.Run("Max out the overflow page", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for range MaxOverflowRowIDsPerPage - 1 {
				if err := anIndex.Insert(ctx, key, rowID); err != nil {
					return err
				}
				insertedRowIDs = append(insertedRowIDs, rowID)
				rowID += 1
			}
			return nil
		})
		require.NoError(t, err)

		var (
			rootNode     = aPager.pages[0].IndexNode.(*IndexNode[int64])
			overflowNode = aPager.pages[rootNode.Cells[0].Overflow].IndexOverflowNode
		)

		assert.Equal(t, 4, int(rootNode.Cells[0].InlineRowIDs))
		assert.Equal(t, []RowID{101, 102, 103, 104}, rootNode.Cells[0].RowIDs)
		assert.Equal(t, 1, int(rootNode.Cells[0].Overflow))

		assert.Equal(t, 0, int(overflowNode.Header.NextPage))
		assert.Equal(t, MaxOverflowRowIDsPerPage, int(overflowNode.Header.ItemCount))
		assert.Len(t, overflowNode.RowIDs, MaxOverflowRowIDsPerPage)
	})

	t.Run("When last overflow page is maxed out, create a new one", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			if err := anIndex.Insert(ctx, key, rowID); err != nil {
				return err
			}
			insertedRowIDs = append(insertedRowIDs, rowID)
			rowID += 1
			key += 1
			return nil
		})
		require.NoError(t, err)

		var (
			rootNode        = aPager.pages[0].IndexNode.(*IndexNode[int64])
			overflowNode    = aPager.pages[rootNode.Cells[0].Overflow].IndexOverflowNode
			newOverflowNode = aPager.pages[overflowNode.Header.NextPage].IndexOverflowNode
		)

		assert.Equal(t, 4, int(rootNode.Cells[0].InlineRowIDs))
		assert.Equal(t, []RowID{101, 102, 103, 104}, rootNode.Cells[0].RowIDs)
		assert.Equal(t, 1, int(rootNode.Cells[0].Overflow))

		assert.Equal(t, 2, int(overflowNode.Header.NextPage))
		assert.Equal(t, MaxOverflowRowIDsPerPage, int(overflowNode.Header.ItemCount))
		assert.Len(t, overflowNode.RowIDs, MaxOverflowRowIDsPerPage)

		assert.Equal(t, 0, int(newOverflowNode.Header.NextPage))
		assert.Equal(t, 1, int(newOverflowNode.Header.ItemCount))
		assert.Equal(t, []RowID{rowID - 1}, newOverflowNode.RowIDs)
	})

	t.Run("Insert a thousand row IDs randomly", func(t *testing.T) {
		for i := 0; i < 1000; {
			rowsPerKey := gen.Number(1, 1000)
			err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
				for range rowsPerKey {
					if err := anIndex.Insert(ctx, key, rowID); err != nil {
						return err
					}
					insertedRowIDs = append(insertedRowIDs, rowID)
					rowID += 1
				}
				insertedKeys = append(insertedKeys, key)
				key += 1
				return nil
			})
			require.NoError(t, err)
			i += rowsPerKey
		}

		actualKeys, actualRowIDs := collectAllKeysAndRowIDs(t, anIndex, ctx)
		require.Len(t, actualKeys, len(insertedKeys))
		require.Len(t, actualRowIDs, len(insertedRowIDs))
		assert.ElementsMatch(t, insertedKeys, actualKeys)
		assert.ElementsMatch(t, insertedRowIDs, actualRowIDs)
	})
}

func collectAllKeysAndRowIDs[T IndexKey](t *testing.T, anIndex *Index[T], ctx context.Context) ([]T, []RowID) {
	var (
		actualKeys   = make([]T, 0, 10)
		actualRowIDs = make([]RowID, 0, 10)
	)
	anIndex.BFS(ctx, func(aPage *Page) {
		aNode := aPage.IndexNode.(*IndexNode[T])
		for cellIdx := uint32(0); cellIdx < aNode.Header.Keys; cellIdx++ {
			aCell := aNode.Cells[cellIdx]
			actualKeys = append(actualKeys, aCell.Key)
			actualRowIDs = append(actualRowIDs, aCell.RowIDs...)
			if aCell.Overflow != 0 {
				overflowRowIDs, err := readOverflowRowIDs[T](ctx, anIndex.pager, aCell.Overflow)
				require.NoError(t, err)
				actualRowIDs = append(actualRowIDs, overflowRowIDs...)
			}
		}
	})
	return actualKeys, actualRowIDs
}
