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
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		keys           = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn        = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager     = aPager.ForIndex([]Column{aColumn}, true)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), aPager, nil)
		txPager        = NewTransactionalPager(indexPager, txManager, testTableName, "test_index")
	)
	anIndex, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", []Column{aColumn}, txPager, 0)
	require.NoError(t, err)
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

	// require.NoError(t, anIndex.print())

	t.Run("scan in ascending order", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanAll(ctx, false, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, scannedKeys)
		assert.Equal(t, []RowID{101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121}, scannedRowIDs)
	})

	t.Run("scan in descending order", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanAll(ctx, true, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, scannedKeys)
		assert.Equal(t, []RowID{121, 120, 119, 118, 117, 116, 115, 114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101}, scannedRowIDs)
	})
}

func TestIndex_ScanAll_NonUnique(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		keys           = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn        = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager     = aPager.ForIndex([]Column{aColumn}, true)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), aPager, nil)
		txPager        = NewTransactionalPager(indexPager, txManager, testTableName, "test_index")
	)
	anIndex, err := NewNonUniqueIndex[int64](testLogger, txManager, "test_index", []Column{aColumn}, txPager, 0)
	require.NoError(t, err)
	anIndex.maximumKeys = 3

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
				return err
			}
			// Insert a duplicate key to test non-unique index scanning of all row IDs for a key
			if key == 21 {
				for i := 1; i <= 5; i++ {
					if err := anIndex.Insert(ctx, key, RowID(key+100+int64(i))); err != nil {
						return err
					}
				}
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

	// require.NoError(t, anIndex.print())

	t.Run("scan in ascending order", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanAll(ctx, false, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 21, 21, 21, 21, 21}, scannedKeys)
		assert.Equal(t, []RowID{101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126}, scannedRowIDs)
	})

	t.Run("scan in descending order", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanAll(ctx, true, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{21, 21, 21, 21, 21, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, scannedKeys)
		assert.Equal(t, []RowID{121, 122, 123, 124, 125, 126, 120, 119, 118, 117, 116, 115, 114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101}, scannedRowIDs)
	})
}

func TestIndex_ScanRange(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		keys           = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn        = Column{Name: "test_column", Kind: Int8, Size: 8}
		indexPager     = aPager.ForIndex([]Column{aColumn}, true)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), aPager, nil)
		txPager        = NewTransactionalPager(indexPager, txManager, testTableName, "test_index")
	)
	anIndex, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", []Column{aColumn}, txPager, 0)
	require.NoError(t, err)
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

	// require.NoError(t, anIndex.print())

	t.Run("scan range all", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanRange(ctx, RangeCondition{}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, scannedKeys)
		assert.Equal(t, []RowID{101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121}, scannedRowIDs)
	})

	t.Run("scan range < 18", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanRange(ctx, RangeCondition{
			Upper: &RangeBound{
				Value:     int64(18),
				Inclusive: false,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, scannedKeys)
		assert.Equal(t, []RowID{101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117}, scannedRowIDs)
	})

	t.Run("scan range <= 18", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanRange(ctx, RangeCondition{
			Upper: &RangeBound{
				Value:     int64(18),
				Inclusive: true,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, scannedKeys)
		assert.Equal(t, []RowID{101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118}, scannedRowIDs)
	})

	t.Run("scan range > 18", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanRange(ctx, RangeCondition{
			Lower: &RangeBound{
				Value:     int64(18),
				Inclusive: false,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{19, 20, 21}, scannedKeys)
		assert.Equal(t, []RowID{119, 120, 121}, scannedRowIDs)
	})

	t.Run("scan range >= 18", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
		err := anIndex.ScanRange(ctx, RangeCondition{
			Lower: &RangeBound{
				Value:     int64(18),
				Inclusive: true,
			},
		}, func(key any, rowID RowID) error {
			scannedKeys = append(scannedKeys, key.(int64))
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{18, 19, 20, 21}, scannedKeys)
		assert.Equal(t, []RowID{118, 119, 120, 121}, scannedRowIDs)
	})

	t.Run("scan range (7; 18)", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
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
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, scannedKeys)
		assert.Equal(t, []RowID{108, 109, 110, 111, 112, 113, 114, 115, 116, 117}, scannedRowIDs)
	})

	t.Run("scan range <7; 18)", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
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
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, scannedKeys)
		assert.Equal(t, []RowID{107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117}, scannedRowIDs)
	})

	t.Run("scan range (7; 18>", func(t *testing.T) {
		var (
			scannedKeys   []int64
			scannedRowIDs []RowID
		)
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
			scannedRowIDs = append(scannedRowIDs, rowID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, scannedKeys)
		assert.Equal(t, []RowID{108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118}, scannedRowIDs)
	})
}
