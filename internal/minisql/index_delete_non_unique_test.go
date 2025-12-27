package minisql

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Small helper to manage row IDs per key for testing
type rowIDsPerKey map[int64][]RowID

func (r rowIDsPerKey) Remove(key int64, idx int) {
	r[key] = append(r[key][:idx], r[key][idx+1:]...)
}

func (r rowIDsPerKey) Append(key int64, rowID RowID) {
	r[key] = append(r[key], rowID)
}

func (r rowIDsPerKey) RowIDs(key int64) []RowID {
	return r[key]
}

func (r rowIDsPerKey) LastRowID(key int64) RowID {
	return r[key][len(r[key])-1]
}

func (r rowIDsPerKey) AllRowIDs() []RowID {
	rowIDs := make([]RowID, 0)
	for _, ids := range r {
		rowIDs = append(rowIDs, ids...)
	}
	return rowIDs
}

func TestIndex_NonUnique_Delete(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		aColumn    = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager = aPager.ForIndex(aColumn.Kind, true)
		txManager  = NewTransactionManager(zap.NewNop(), testDbName, mockPagerFactory(indexPager), aPager, nil)
		txPager    = NewTransactionalPager(indexPager, txManager, testTableName, "test_index")
	)
	anIndex, err := NewNonUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, txPager, 0)
	require.NoError(t, err)

	var (
		key            = int64(1)
		rowID          = RowID(101)
		insertedKeys   = make([]int64, 0)
		insertedRowIDs = make(rowIDsPerKey)
	)

	t.Run("Insert max inline row IDs, should not overflow", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for range MaxInlineRowIDs {
				if err := anIndex.Insert(ctx, key, rowID); err != nil {
					return err
				}
				insertedRowIDs.Append(key, rowID)
				rowID += 1
			}
			insertedKeys = append(insertedKeys, key)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("Delete inline row IDs from cell with no overflow", func(t *testing.T) {
		// Try deleting one of 4 inline row IDs
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, key, insertedRowIDs[key][2])
		})
		require.NoError(t, err)
		insertedRowIDs.Remove(key, 2)

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
		)

		assert.Equal(t, 3, int(rootNode.Cells[0].InlineRowIDs))
		assert.Equal(t, []RowID{101, 102, 104}, rootNode.Cells[0].RowIDs)
		assert.Equal(t, 0, int(rootNode.Cells[0].Overflow))

		// Now delete the rest
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for _, rowID := range insertedRowIDs.RowIDs(key) {
				if err := anIndex.Delete(ctx, key, rowID); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)

		// Index should be empty now
		rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
		assert.Equal(t, 0, int(rootNode.Header.Keys))

		actualKeys, actualRowIDs := collectAllKeysAndRowIDs(t, anIndex, ctx)
		require.Empty(t, actualKeys)
		require.Empty(t, actualRowIDs)
	})

	insertedKeys = []int64{}
	insertedRowIDs = make(rowIDsPerKey)
	t.Run("Delete from maxed out overflow page", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for range MaxInlineRowIDs + MaxOverflowRowIDsPerPage {
				if err := anIndex.Insert(ctx, key, rowID); err != nil {
					return err
				}
				insertedRowIDs.Append(key, rowID)
				rowID += 1
			}
			return nil
		})
		require.NoError(t, err)

		// Delete one of the inline row IDs
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, key, insertedRowIDs[key][2])
		})
		require.NoError(t, err)
		insertedRowIDs.Remove(key, 2)

		var (
			rootNode     = aPager.pages[0].IndexNode.(*IndexNode[int64])
			overflowNode = aPager.pages[rootNode.Cells[0].Overflow].IndexOverflowNode
		)

		assert.Equal(t, 4, int(rootNode.Cells[0].InlineRowIDs))
		assert.Equal(t, 1, int(rootNode.Cells[0].Overflow))
		// The removed inlined row ID should be replaced with the last overflow row ID
		assert.Equal(t, insertedRowIDs.LastRowID(key), rootNode.Cells[0].RowIDs[2])

		assert.Equal(t, 0, int(overflowNode.Header.NextPage))
		assert.Equal(t, MaxOverflowRowIDsPerPage-1, int(overflowNode.Header.ItemCount))
		assert.Len(t, overflowNode.RowIDs, MaxOverflowRowIDsPerPage-1)

		// Now delete the rest
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for _, rowID := range insertedRowIDs.RowIDs(key) {
				if err := anIndex.Delete(ctx, key, rowID); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)

		// Index should be empty now
		rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
		assert.Equal(t, 0, int(rootNode.Header.Keys))

		actualKeys, actualRowIDs := collectAllKeysAndRowIDs(t, anIndex, ctx)
		require.Empty(t, actualKeys)
		require.Empty(t, actualRowIDs)
	})

	insertedKeys = []int64{}
	insertedRowIDs = make(rowIDsPerKey)
	t.Run("Insert and delete a thousand row IDs randomly", func(t *testing.T) {
		for i := 0; i < 1000; {
			rowsPerKey := gen.Number(1, 1000)
			err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
				for range rowsPerKey {
					if err := anIndex.Insert(ctx, key, rowID); err != nil {
						return err
					}
					insertedRowIDs.Append(key, rowID)
					rowID += 1
				}
				insertedKeys = append(insertedKeys, key)
				key += 1
				return nil
			})
			require.NoError(t, err)
			i += rowsPerKey
		}

		expectedKeys := insertedKeys
		expectedRowIDs := insertedRowIDs.AllRowIDs()
		actualKeys, actualRowIDs := collectAllKeysAndRowIDs(t, anIndex, ctx)
		require.Len(t, actualKeys, len(expectedKeys))
		require.Len(t, actualRowIDs, len(expectedRowIDs))
		assert.ElementsMatch(t, expectedKeys, actualKeys)
		assert.ElementsMatch(t, expectedRowIDs, actualRowIDs)

		// Delete all inserted row IDs randomly
		rand.Shuffle(len(insertedKeys), func(i, j int) { insertedKeys[i], insertedKeys[j] = insertedKeys[j], insertedKeys[i] })
		for _, key := range insertedKeys {
			rowIDs := insertedRowIDs.RowIDs(key)
			rand.Shuffle(len(rowIDs), func(i, j int) { rowIDs[i], rowIDs[j] = rowIDs[j], rowIDs[i] })

			for _, rowID := range rowIDs {
				err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
					return anIndex.Delete(ctx, key, rowID)
				})
				require.NoError(t, err)
			}
		}

		actualKeys, actualRowIDs = collectAllKeysAndRowIDs(t, anIndex, ctx)
		require.Empty(t, actualKeys)
		require.Empty(t, actualRowIDs)

		// Check that all pages other than root index node have been freed
		for i := 1; i < len(aPager.pages); i++ {
			assert.NotNil(t, aPager.pages[i].FreePage)
		}
	})
}
