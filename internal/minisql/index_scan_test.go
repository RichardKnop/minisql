package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIndex_ScanAll(t *testing.T) {
	var (
		aPager    = initTest(t)
		ctx       = context.Background()
		keys      = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn   = Column{Name: "test_column", Kind: Int8, Size: 8}
		txManager = NewTransactionManager(zap.NewNop())
		idxPager  = NewTransactionalPager(
			aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size), true),
			txManager,
		)
	)
	anIndex, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, idxPager, 0)
	require.NoError(t, err)
	anIndex.maximumKeys = 3

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
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

	// require.NoError(t, anIndex.print())

	t.Run("scan in ascending order", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanAll(ctx, false, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, scannedKeys)
	})

	t.Run("scan in descending order", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanAll(ctx, true, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, scannedKeys)
	})
}

func TestIndex_ScanRange(t *testing.T) {
	var (
		aPager    = initTest(t)
		ctx       = context.Background()
		keys      = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn   = Column{Name: "test_column", Kind: Int8, Size: 8}
		txManager = NewTransactionManager(zap.NewNop())
		idxPager  = NewTransactionalPager(
			aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size), true),
			txManager,
		)
	)
	anIndex, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, idxPager, 0)
	require.NoError(t, err)
	anIndex.maximumKeys = 3

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
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

	// require.NoError(t, anIndex.print())

	t.Run("scan range < 18", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanRange(ctx, RangeCondition{
			Upper: &RangeBound{
				Value:     int64(18),
				Inclusive: false,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, scannedKeys)
	})

	t.Run("scan range <= 18", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanRange(ctx, RangeCondition{
			Upper: &RangeBound{
				Value:     int64(18),
				Inclusive: true,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, scannedKeys)
	})

	t.Run("scan range > 18", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanRange(ctx, RangeCondition{
			Lower: &RangeBound{
				Value:     int64(18),
				Inclusive: false,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{19, 20, 21}, scannedKeys)
	})

	t.Run("scan range >= 18", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanRange(ctx, RangeCondition{
			Lower: &RangeBound{
				Value:     int64(18),
				Inclusive: true,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{18, 19, 20, 21}, scannedKeys)
	})

	t.Run("scan range (7; 18)", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanRange(ctx, RangeCondition{
			Lower: &RangeBound{
				Value:     int64(7),
				Inclusive: false,
			},
			Upper: &RangeBound{
				Value:     int64(18),
				Inclusive: false,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, scannedKeys)
	})

	t.Run("scan range <7; 18)", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanRange(ctx, RangeCondition{
			Lower: &RangeBound{
				Value:     int64(7),
				Inclusive: true,
			},
			Upper: &RangeBound{
				Value:     int64(18),
				Inclusive: false,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, scannedKeys)
	})

	t.Run("scan range (7; 18>", func(t *testing.T) {
		var scannedKeys []int64
		err := anIndex.ScanRange(ctx, RangeCondition{
			Lower: &RangeBound{
				Value:     int64(7),
				Inclusive: false,
			},
			Upper: &RangeBound{
				Value:     int64(18),
				Inclusive: true,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, scannedKeys)
	})
}
