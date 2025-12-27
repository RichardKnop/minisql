package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIndex_Seek(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		keys           = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn        = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager     = aPager.ForIndex(aColumn.Kind, true)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), aPager, nil)
		txPager        = NewTransactionalPager(indexPager, txManager, testTableName, "test_index")
	)
	anIndex, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, txPager, 0)
	anIndex.maximumKeys = 3

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	/*
									+------------------------------------------------+
									|        9              ,             16         |
									+------------------------------------------------+
				                   /                        |                         \
					+-------------+                       +---------+                  +---------------+
					|   2  ,  5   |                       | 11 , 13 |                  |      19       |
					+-------------+                       +---------+                  +---------------+
				   /       |       \                     /     |     \                /                 \
		 	  +---+    +-------+  +-----------+    +----+   +-----+  +---------+   +---------+         +---------+
			  | 1 |    | 3 , 4 |  | 6 , 7 , 8 |    | 10 |   |  12 |  | 14 , 15 |   | 17 , 18 |         | 20 , 21 |
			  +---+    +-------+  +-----------+    +----+   +-----+  +---------+   +---------+         +---------+
	*/

	aRootPage := aPager.pages[0]
	for _, key := range keys {
		_, ok, err := anIndex.Seek(context.Background(), aRootPage, key)
		require.NoError(t, err)
		assert.True(t, ok)
	}

	t.Run("non existent key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, int64(27))
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, IndexCursor[int64]{}, aCursor)
	})

	t.Run("root node key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, int64(16))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(0), aCursor.PageIdx)
		assert.Equal(t, uint32(1), aCursor.CellIdx)
	})

	t.Run("internal node first key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, int64(11))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(6), aCursor.PageIdx)
		assert.Equal(t, uint32(0), aCursor.CellIdx)
	})

	t.Run("internal node second key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, int64(5))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(5), aCursor.PageIdx)
		assert.Equal(t, uint32(1), aCursor.CellIdx)
	})

	t.Run("leaf node first key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, int64(3))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(9), aCursor.PageIdx)
		assert.Equal(t, uint32(0), aCursor.CellIdx)
	})

	t.Run("leaf node middle key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, int64(7))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(4), aCursor.PageIdx)
		assert.Equal(t, uint32(1), aCursor.CellIdx)
	})

	t.Run("leaf node last key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, int64(18))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(3), aCursor.PageIdx)
		assert.Equal(t, uint32(1), aCursor.CellIdx)
	})
}

func TestIndex_SeekLastKey(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		keys           = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn        = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager     = aPager.ForIndex(aColumn.Kind, true)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), aPager, nil)
		txPager        = NewTransactionalPager(indexPager, txManager, testTableName, "test_index")
	)

	// Initialize empty index, this normally happens in the database init step
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := txPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		indexNode := NewIndexNode[int64](true)
		indexNode.Header.IsRoot = true
		indexNode.Header.IsLeaf = true
		freePage.IndexNode = indexNode
		return nil
	})
	require.NoError(t, err)

	anIndex, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, txPager, 0)
	require.NoError(t, err)
	anIndex.maximumKeys = 3

	t.Run("empty index", func(t *testing.T) {
		lastKey, err := anIndex.SeekLastKey(ctx, anIndex.GetRootPageIdx())
		require.NoError(t, err)
		assert.Equal(t, int64(0), lastKey)
	})

	t.Run("populated index", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for _, key := range keys {
				if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)

		/*
										+------------------------------------------------+
										|        9              ,             16         |
										+------------------------------------------------+
					                   /                        |                         \
						+-------------+                       +---------+                  +---------------+
						|   2  ,  5   |                       | 11 , 13 |                  |      19       |
						+-------------+                       +---------+                  +---------------+
					   /       |       \                     /     |     \                /                 \
			 	  +---+    +-------+  +-----------+    +----+   +-----+  +---------+   +---------+         +---------+
				  | 1 |    | 3 , 4 |  | 6 , 7 , 8 |    | 10 |   |  12 |  | 14 , 15 |   | 17 , 18 |         | 20 , 21 |
				  +---+    +-------+  +-----------+    +----+   +-----+  +---------+   +---------+         +---------+
		*/

		lastKey, err := anIndex.SeekLastKey(ctx, anIndex.GetRootPageIdx())
		require.NoError(t, err)
		assert.Equal(t, int64(21), lastKey)
	})
}
