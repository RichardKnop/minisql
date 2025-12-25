package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIndex_Insert(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		key        = int64(1)
		aColumn    = Column{Name: "test_column", Kind: Int8, Size: 8}
		txManager  = NewTransactionManager(zap.NewNop())
		indexPager = NewTransactionalPager(
			aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size), true),
			txManager,
		)
	)
	anIndex, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, indexPager, 0)
	require.NoError(t, err)
	anIndex.maximumKeys = 3

	t.Run("Insert first three keys into root node", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for i := 0; i < 3; i++ {
				if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
					return err
				}
				key++
			}
			return nil
		}, TxCommitter{aPager, nil})
		require.NoError(t, err)

		/*
			+----------+
			|  1, 2, 3 |
			+----------+
		*/

		// require.NoError(t, anIndex.print())

		rootNode := aPager.pages[0].IndexNode.(*IndexNode[int64])
		assertIndexNode(t, rootNode, true, true, 0, []int64{1, 2, 3}, nil)
	})

	t.Run("Insert duplicate key fails", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Insert(ctx, key-1, RowID(key-1+100))
		}, TxCommitter{aPager, nil})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("Insert 4th key, causes a split", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Insert(ctx, key, RowID(key+100))
		}, TxCommitter{aPager, nil})
		require.NoError(t, err)
		key++

		/*
						        +-----+
						        |  2  |
						        +-----+
			                   /       \
						+-----+         +--------+
						|  1  |         |  3, 4  |
						+-----+         +--------+
		*/

		// require.NoError(t, anIndex.print())

		rootNode := aPager.pages[0].IndexNode.(*IndexNode[int64])
		leftChild := aPager.pages[1].IndexNode.(*IndexNode[int64])
		rightChild := aPager.pages[2].IndexNode.(*IndexNode[int64])

		assertIndexNode(t, rootNode, true, false, PageIndex(0), []int64{2}, []PageIndex{1, 2})
		assertIndexNode(t, leftChild, false, true, PageIndex(0), []int64{1}, nil)
		assertIndexNode(t, rightChild, false, true, PageIndex(0), []int64{3, 4}, nil)
	})

	t.Run("Insert 2 more keys, another split", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for i := 0; i < 2; i++ {
				if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
					return err
				}
				key++
			}
			return nil
		}, TxCommitter{aPager, nil})
		require.NoError(t, err)

		/*
						        +---------------------+
						        |     2    ,     4    |
						        +---------------------+
			                   /           |           \
						+-----+          +-----+        +--------+
						|  1  |          |  3  |        |  5, 6  |
						+-----+          +-----+        +--------+
		*/

		//require.NoError(t, anIndex.print())

		var (
			rootNode    = aPager.pages[0].IndexNode.(*IndexNode[int64])
			leftChild   = aPager.pages[1].IndexNode.(*IndexNode[int64])
			middleChild = aPager.pages[2].IndexNode.(*IndexNode[int64])
			rightChild  = aPager.pages[3].IndexNode.(*IndexNode[int64])
		)

		assertIndexNode(t, rootNode, true, false, PageIndex(0), []int64{2, 4}, []PageIndex{1, 2, 3})
		assertIndexNode(t, leftChild, false, true, PageIndex(0), []int64{1}, nil)
		assertIndexNode(t, middleChild, false, true, PageIndex(0), []int64{3}, nil)
		assertIndexNode(t, rightChild, false, true, PageIndex(0), []int64{5, 6}, nil)
	})

	t.Run("Insert 2 more keys, another split", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for i := 0; i < 2; i++ {
				if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
					return err
				}
				key++
			}
			return nil
		}, TxCommitter{aPager, nil})
		require.NoError(t, err)

		/*
						        +-------------------------+
						        |   2   ,    4    ,   6   |
						        +-------------------------+
			                   /        |         |        \
						+-----+      +-----+   +-----+      +---------+
						|  1  |      |  3  |   |  5  |      |  7 , 8  |
						+-----+      +-----+   +-----+      +---------+
		*/

		//require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
			leaf1    = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2    = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf3    = aPager.pages[3].IndexNode.(*IndexNode[int64])
			leaf4    = aPager.pages[4].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, PageIndex(0), []int64{2, 4, 6}, []PageIndex{1, 2, 3, 4})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, PageIndex(0), []int64{1}, nil)
		assertIndexNode(t, leaf2, false, true, PageIndex(0), []int64{3}, nil)
		assertIndexNode(t, leaf3, false, true, PageIndex(0), []int64{5}, nil)
		assertIndexNode(t, leaf4, false, true, PageIndex(0), []int64{7, 8}, nil)
	})

	t.Run("Insert 1 more key, internal split", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Insert(ctx, key, RowID(key+100))
		}, TxCommitter{aPager, nil})
		require.NoError(t, err)
		key++

		/*
									        +--------------------+
									        |          4         |
									        +--------------------+
			                               /                      \
								    +-----+                        +-----+
									|  2  |                        |  6  |
									+-----+                        +-----+
						           /       \                      /       \
							+-----+         +-----+        +-----+         +-------------+
							|  1  |         |  3  |        |  5  |         |  7 , 8 , 9  |
							+-----+         +-----+        +-----+         +-------------+
		*/

		//require.NoError(t, anIndex.print())

		var (
			rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
			internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
			internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
			leaf1     = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2     = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf3     = aPager.pages[3].IndexNode.(*IndexNode[int64])
			leaf4     = aPager.pages[4].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, PageIndex(0), []int64{4}, []PageIndex{5, 6})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, PageIndex(0), []int64{2}, []PageIndex{1, 2})
		assertIndexNode(t, internal2, false, false, PageIndex(0), []int64{6}, []PageIndex{3, 4})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, PageIndex(5), []int64{1}, nil)
		assertIndexNode(t, leaf2, false, true, PageIndex(5), []int64{3}, nil)
		assertIndexNode(t, leaf3, false, true, PageIndex(6), []int64{5}, nil)
		assertIndexNode(t, leaf4, false, true, PageIndex(6), []int64{7, 8, 9}, nil)
	})

	t.Run("Keep inserting more keys", func(t *testing.T) {
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			for i := 0; i < 5; i++ {
				if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
					return err
				}
				key++
			}
			return nil
		}, TxCommitter{aPager, nil})
		require.NoError(t, err)

		/*
									    +-------------------------------+
									    |       4       ,       8       |
									    +-------------------------------+
			                            /               |                \
								+-----+              +------+             +-----------+
								|  2  |              |  6   |             |  10 , 12  |
								+-----+              +------+             +-----------+
						       /      \             /        \            /      |     \
						+-----+        +-----+  +-----+    +-----+   +-----+  +-----+   +----------+
						|  1  |        |  3  |  |  5  |    |  7  |   |  9  |  | 11  |   | 13 , 14  |
						+-----+        +-----+  +-----+    +-----+   +-----+  +-----+   +----------+
		*/

		//require.NoError(t, anIndex.print())

		var (
			rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
			internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
			internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
			internal3 = aPager.pages[9].IndexNode.(*IndexNode[int64])
			leaf1     = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2     = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf3     = aPager.pages[3].IndexNode.(*IndexNode[int64])
			leaf4     = aPager.pages[4].IndexNode.(*IndexNode[int64])
			leaf5     = aPager.pages[7].IndexNode.(*IndexNode[int64])
			leaf6     = aPager.pages[8].IndexNode.(*IndexNode[int64])
			leaf7     = aPager.pages[10].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, PageIndex(0), []int64{4, 8}, []PageIndex{5, 6, 9})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, PageIndex(0), []int64{2}, []PageIndex{1, 2})
		assertIndexNode(t, internal2, false, false, PageIndex(0), []int64{6}, []PageIndex{3, 4})
		assertIndexNode(t, internal3, false, false, PageIndex(0), []int64{10, 12}, []PageIndex{7, 8, 10})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, PageIndex(5), []int64{1}, nil)
		assertIndexNode(t, leaf2, false, true, PageIndex(5), []int64{3}, nil)
		assertIndexNode(t, leaf3, false, true, PageIndex(6), []int64{5}, nil)
		assertIndexNode(t, leaf4, false, true, PageIndex(6), []int64{7}, nil)
		assertIndexNode(t, leaf5, false, true, PageIndex(9), []int64{9}, nil)
		assertIndexNode(t, leaf6, false, true, PageIndex(9), []int64{11}, nil)
		assertIndexNode(t, leaf7, false, true, PageIndex(9), []int64{13, 14}, nil)
	})

	expectedKeys := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

	checkIndexKeys(ctx, t, anIndex, expectedKeys)
}

func TestIndex_Insert_OutOfOrder(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		keys       = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn    = Column{Name: "test_column", Kind: Int8, Size: 8}
		txManager  = NewTransactionManager(zap.NewNop())
		indexPager = NewTransactionalPager(
			aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size), true),
			txManager,
		)
	)
	anIndex, err := NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, indexPager, 0)
	require.NoError(t, err)
	anIndex.maximumKeys = 3

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			if err := anIndex.Insert(ctx, key, RowID(key+100)); err != nil {
				return err
			}
		}
		return nil
	}, TxCommitter{aPager, nil})
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

	checkIndexKeys(ctx, t, anIndex, keys)

	// require.NoError(t, anIndex.print())

	var (
		rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
		internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
		internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
		internal3 = aPager.pages[10].IndexNode.(*IndexNode[int64])
		// leaves of first internal node
		leaf1 = aPager.pages[1].IndexNode.(*IndexNode[int64])
		leaf2 = aPager.pages[9].IndexNode.(*IndexNode[int64])
		leaf3 = aPager.pages[4].IndexNode.(*IndexNode[int64])
		// leaves of second internal node
		leaf4 = aPager.pages[2].IndexNode.(*IndexNode[int64])
		leaf5 = aPager.pages[7].IndexNode.(*IndexNode[int64])
		leaf6 = aPager.pages[11].IndexNode.(*IndexNode[int64])
		// leaves of third node
		leaf7 = aPager.pages[3].IndexNode.(*IndexNode[int64])
		leaf8 = aPager.pages[8].IndexNode.(*IndexNode[int64])
	)

	// Root node
	assertIndexNode(t, rootNode, true, false, PageIndex(0), []int64{9, 16}, []PageIndex{5, 6, 10})
	// Internal nodes
	assertIndexNode(t, internal1, false, false, PageIndex(0), []int64{2, 5}, []PageIndex{1, 9, 4})
	assertIndexNode(t, internal2, false, false, PageIndex(0), []int64{11, 13}, []PageIndex{2, 7, 11})
	assertIndexNode(t, internal3, false, false, PageIndex(0), []int64{19}, []PageIndex{3, 8})
	// // Leaf nodes
	assertIndexNode(t, leaf1, false, true, PageIndex(5), []int64{1}, nil)
	assertIndexNode(t, leaf2, false, true, PageIndex(5), []int64{3, 4}, nil)
	assertIndexNode(t, leaf3, false, true, PageIndex(5), []int64{6, 7, 8}, nil)
	assertIndexNode(t, leaf4, false, true, PageIndex(6), []int64{10}, nil)
	assertIndexNode(t, leaf5, false, true, PageIndex(6), []int64{12}, nil)
	assertIndexNode(t, leaf6, false, true, PageIndex(6), []int64{14, 15}, nil)
	assertIndexNode(t, leaf7, false, true, PageIndex(10), []int64{17, 18}, nil)
	assertIndexNode(t, leaf8, false, true, PageIndex(10), []int64{20, 21}, nil)
}

func assertIndexNode(t *testing.T, aNode *IndexNode[int64], isRoot, isLeaf bool, parent PageIndex, keys []int64, children []PageIndex) {
	if isRoot {
		assert.True(t, aNode.Header.IsRoot, "should be a root node")
	} else {
		assert.False(t, aNode.Header.IsRoot, "should not be a root node")
	}
	if isLeaf {
		assert.True(t, aNode.Header.IsLeaf, "should be a leaf node")
	} else {
		assert.False(t, aNode.Header.IsLeaf, "should not be a leaf node")
	}
	assert.Equal(t, parent, aNode.Header.Parent, "parent index mismatch")
	assert.Equal(t, len(keys), int(aNode.Header.Keys), "number of keys mismatch")
	assert.Equal(t, keys, aNode.Keys(), "keys mismatch")
	assert.Equal(t, children, aNode.Children(), "children mismatch")
	expectedRowIDs := make([]RowID, len(keys))
	for i := range keys {
		expectedRowIDs[i] = RowID(keys[i] + 100)
	}
	assert.Equal(t, expectedRowIDs, aNode.RowIDs(), "row IDs mismatch")
}
