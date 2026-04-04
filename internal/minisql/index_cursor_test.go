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
		pager, dbFile = initTest(t)
		ctx            = context.Background()
		keys           = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		col        = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager     = pager.ForIndex([]Column{col}, true)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), pager, nil)
		txPager        = NewTransactionalPager(indexPager, txManager, testTableName, "test_index")
	)
	idx, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", []Column{col}, txPager, 0)
	require.NoError(t, err)
	idx.maximumKeys = 3

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			if err := idx.Insert(ctx, key, RowID(key+100)); err != nil {
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

	rootPage := pager.pages[0]
	for _, key := range keys {
		_, ok, err := idx.Seek(context.Background(), rootPage, key)
		require.NoError(t, err)
		assert.True(t, ok)
	}

	t.Run("non existent key", func(t *testing.T) {
		cursor, ok, err := idx.Seek(context.Background(), rootPage, int64(27))
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, IndexCursor[int64]{}, cursor)
	})

	t.Run("root node key", func(t *testing.T) {
		cursor, ok, err := idx.Seek(context.Background(), rootPage, int64(16))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(0), cursor.PageIdx)
		assert.Equal(t, uint32(1), cursor.CellIdx)
	})

	t.Run("internal node first key", func(t *testing.T) {
		cursor, ok, err := idx.Seek(context.Background(), rootPage, int64(11))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(6), cursor.PageIdx)
		assert.Equal(t, uint32(0), cursor.CellIdx)
	})

	t.Run("internal node second key", func(t *testing.T) {
		cursor, ok, err := idx.Seek(context.Background(), rootPage, int64(5))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(5), cursor.PageIdx)
		assert.Equal(t, uint32(1), cursor.CellIdx)
	})

	t.Run("leaf node first key", func(t *testing.T) {
		cursor, ok, err := idx.Seek(context.Background(), rootPage, int64(3))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(9), cursor.PageIdx)
		assert.Equal(t, uint32(0), cursor.CellIdx)
	})

	t.Run("leaf node middle key", func(t *testing.T) {
		cursor, ok, err := idx.Seek(context.Background(), rootPage, int64(7))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(4), cursor.PageIdx)
		assert.Equal(t, uint32(1), cursor.CellIdx)
	})

	t.Run("leaf node last key", func(t *testing.T) {
		cursor, ok, err := idx.Seek(context.Background(), rootPage, int64(18))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, PageIndex(3), cursor.PageIdx)
		assert.Equal(t, uint32(1), cursor.CellIdx)
	})
}

func TestIndex_SeekLastKey(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx            = context.Background()
		keys           = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		col        = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager     = pager.ForIndex([]Column{col}, true)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), pager, nil)
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

	idx, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", []Column{col}, txPager, 0)
	require.NoError(t, err)
	idx.maximumKeys = 3

	t.Run("empty index", func(t *testing.T) {
		lastKey, err := idx.SeekLastKey(ctx, idx.GetRootPageIdx())
		require.NoError(t, err)
		assert.Equal(t, int64(0), lastKey)
	})

	t.Run("populated index", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for _, key := range keys {
				if err := idx.Insert(ctx, key, RowID(key+100)); err != nil {
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

		lastKey, err := idx.SeekLastKey(ctx, idx.GetRootPageIdx())
		require.NoError(t, err)
		assert.Equal(t, int64(21), lastKey)
	})
}
