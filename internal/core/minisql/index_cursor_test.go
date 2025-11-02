package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUniqueIndex_Seek(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)

	var (
		ctx        = context.Background()
		keys       = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn    = Column{Name: "test_column", Kind: Int8, Size: 8}
		txManager  = NewTransactionManager()
		indexPager = NewTransactionalPager(
			aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size)),
			txManager,
		)
		anIndex = NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, indexPager, 0)
	)
	anIndex.maximumKeys = 3

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			if err := anIndex.Insert(ctx, key, uint64(key+100)); err != nil {
				return err
			}
		}
		return nil
	}, aPager)
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
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, 27)
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, IndexCursor{}, aCursor)
	})

	t.Run("root node key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, 16)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(0), aCursor.PageIdx)
		assert.Equal(t, uint32(1), aCursor.CellIdx)
	})

	t.Run("internal node first key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, 11)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(6), aCursor.PageIdx)
		assert.Equal(t, uint32(0), aCursor.CellIdx)
	})

	t.Run("internal node second key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, 5)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(5), aCursor.PageIdx)
		assert.Equal(t, uint32(1), aCursor.CellIdx)
	})

	t.Run("leaf node first key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, 3)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(9), aCursor.PageIdx)
		assert.Equal(t, uint32(0), aCursor.CellIdx)
	})

	t.Run("leaf node middle key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, 7)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(4), aCursor.PageIdx)
		assert.Equal(t, uint32(1), aCursor.CellIdx)
	})

	t.Run("leaf node last key", func(t *testing.T) {
		aCursor, ok, err := anIndex.Seek(context.Background(), aRootPage, 18)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(3), aCursor.PageIdx)
		assert.Equal(t, uint32(1), aCursor.CellIdx)
	})
}
