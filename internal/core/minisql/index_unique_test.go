package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUniqueIndex_Insert(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	aColumn := Column{Name: "test_column", Kind: Int8, Size: 8}
	indexPager := aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size))
	anIndex := NewUniqueIndex[int64](zap.NewNop(), "test_index", aColumn, indexPager, 0)
	anIndex.maximumKeys = 3

	key := int64(1)

	t.Run("Insert first three keys into root node", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			err = anIndex.Insert(context.Background(), key)
			require.NoError(t, err)
			key++
		}

		/*
			+----------+
			|  1, 2, 3 |
			+----------+
		*/

		// require.NoError(t, anIndex.print())

		rootNode := aPager.pages[0].IndexNode.(*IndexNode[int64])
		assert.True(t, rootNode.Header.IsRoot)
		assert.True(t, rootNode.Header.IsLeaf)
		assert.Equal(t, 3, int(rootNode.Header.Keys))
		assert.Equal(t, []int64{1, 2, 3}, rootNode.Keys())
	})

	t.Run("Insert 4th key, causes a split", func(t *testing.T) {
		err = anIndex.Insert(context.Background(), key)
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

		assert.True(t, rootNode.Header.IsRoot)
		assert.False(t, rootNode.Header.IsLeaf)
		assert.Equal(t, 1, int(rootNode.Header.Keys))
		assert.Equal(t, []int64{2}, rootNode.Keys())
		assert.Equal(t, []uint32{1, 2}, rootNode.Children())

		assert.False(t, leftChild.Header.IsRoot)
		assert.True(t, leftChild.Header.IsLeaf)
		assert.Equal(t, 0, int(leftChild.Header.Parent))
		assert.Equal(t, 1, int(leftChild.Header.Keys))
		assert.Equal(t, []int64{1}, leftChild.Keys())

		assert.False(t, rightChild.Header.IsRoot)
		assert.True(t, rightChild.Header.IsLeaf)
		assert.Equal(t, 0, int(rightChild.Header.Parent))
		assert.Equal(t, 2, int(rightChild.Header.Keys))
		assert.Equal(t, []int64{3, 4}, rightChild.Keys())
	})

	t.Run("Insert 2 more keys, another split", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			err = anIndex.Insert(context.Background(), key)
			require.NoError(t, err)
			key++
		}

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

		assert.True(t, rootNode.Header.IsRoot)
		assert.False(t, rootNode.Header.IsLeaf)
		assert.Equal(t, 2, int(rootNode.Header.Keys))
		assert.Equal(t, []int64{2, 4}, rootNode.Keys())
		assert.Equal(t, []uint32{1, 2, 3}, rootNode.Children())

		assert.False(t, leftChild.Header.IsRoot)
		assert.True(t, leftChild.Header.IsLeaf)
		assert.Equal(t, 0, int(leftChild.Header.Parent))
		assert.Equal(t, 1, int(leftChild.Header.Keys))
		assert.Equal(t, []int64{1}, leftChild.Keys())
		assert.Empty(t, leftChild.Children())

		assert.False(t, middleChild.Header.IsRoot)
		assert.True(t, middleChild.Header.IsLeaf)
		assert.Equal(t, 0, int(middleChild.Header.Parent))
		assert.Equal(t, 1, int(middleChild.Header.Keys))
		assert.Equal(t, []int64{3}, middleChild.Keys())
		assert.Empty(t, middleChild.Children())

		assert.False(t, rightChild.Header.IsRoot)
		assert.True(t, rightChild.Header.IsLeaf)
		assert.Equal(t, 0, int(rightChild.Header.Parent))
		assert.Equal(t, 2, int(rightChild.Header.Keys))
		assert.Equal(t, []int64{5, 6}, rightChild.Keys())
		assert.Empty(t, rightChild.Children())
	})

	t.Run("Insert 2 more keys, another split", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			err = anIndex.Insert(context.Background(), key)
			require.NoError(t, err)
			key++
		}

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

		assert.True(t, rootNode.Header.IsRoot)
		assert.False(t, rootNode.Header.IsLeaf)
		assert.Equal(t, 3, int(rootNode.Header.Keys))
		assert.Equal(t, []int64{2, 4, 6}, rootNode.Keys())
		assert.Equal(t, []uint32{1, 2, 3, 4}, rootNode.Children())

		assert.False(t, leaf1.Header.IsRoot)
		assert.True(t, leaf1.Header.IsLeaf)
		assert.Equal(t, 0, int(leaf1.Header.Parent))
		assert.Equal(t, 1, int(leaf1.Header.Keys))
		assert.Equal(t, []int64{1}, leaf1.Keys())
		assert.Empty(t, leaf1.Children())

		assert.False(t, leaf2.Header.IsRoot)
		assert.True(t, leaf2.Header.IsLeaf)
		assert.Equal(t, 0, int(leaf2.Header.Parent))
		assert.Equal(t, 1, int(leaf2.Header.Keys))
		assert.Equal(t, []int64{3}, leaf2.Keys())
		assert.Empty(t, leaf2.Children())

		assert.False(t, leaf3.Header.IsRoot)
		assert.True(t, leaf3.Header.IsLeaf)
		assert.Equal(t, 0, int(leaf3.Header.Parent))
		assert.Equal(t, 1, int(leaf3.Header.Keys))
		assert.Equal(t, []int64{5}, leaf3.Keys())
		assert.Empty(t, leaf3.Children())

		assert.False(t, leaf4.Header.IsRoot)
		assert.True(t, leaf4.Header.IsLeaf)
		assert.Equal(t, 0, int(leaf4.Header.Parent))
		assert.Equal(t, 2, int(leaf4.Header.Keys))
		assert.Equal(t, []int64{7, 8}, leaf4.Keys())
		assert.Empty(t, leaf4.Children())
	})

	t.Run("Insert 1 more key, internal split", func(t *testing.T) {
		err = anIndex.Insert(context.Background(), key)
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

		assert.True(t, rootNode.Header.IsRoot)
		assert.False(t, rootNode.Header.IsLeaf)
		assert.Equal(t, 1, int(rootNode.Header.Keys))
		assert.Equal(t, []int64{4}, rootNode.Keys())
		assert.Equal(t, []uint32{5, 6}, rootNode.Children())

		// Internal nodes

		assert.False(t, internal1.Header.IsRoot)
		assert.False(t, internal1.Header.IsLeaf)
		assert.Equal(t, 0, int(internal1.Header.Parent))
		assert.Equal(t, 1, int(internal1.Header.Keys))
		assert.Equal(t, []int64{2}, internal1.Keys())
		assert.Equal(t, []uint32{1, 2}, internal1.Children())

		assert.False(t, internal2.Header.IsRoot)
		assert.False(t, internal2.Header.IsLeaf)
		assert.Equal(t, 0, int(internal2.Header.Parent))
		assert.Equal(t, 1, int(internal2.Header.Keys))
		assert.Equal(t, []int64{6}, internal2.Keys())
		assert.Equal(t, []uint32{3, 4}, internal2.Children())

		// Leaf nodes

		assert.False(t, leaf1.Header.IsRoot)
		assert.True(t, leaf1.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf1.Header.Parent))
		assert.Equal(t, 1, int(leaf1.Header.Keys))
		assert.Equal(t, []int64{1}, leaf1.Keys())
		assert.Empty(t, leaf1.Children())

		assert.False(t, leaf2.Header.IsRoot)
		assert.True(t, leaf2.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf2.Header.Parent))
		assert.Equal(t, 1, int(leaf2.Header.Keys))
		assert.Equal(t, []int64{3}, leaf2.Keys())
		assert.Empty(t, leaf2.Children())

		assert.False(t, leaf3.Header.IsRoot)
		assert.True(t, leaf3.Header.IsLeaf)
		assert.Equal(t, 6, int(leaf3.Header.Parent))
		assert.Equal(t, 1, int(leaf3.Header.Keys))
		assert.Equal(t, []int64{5}, leaf3.Keys())
		assert.Empty(t, leaf3.Children())

		assert.False(t, leaf4.Header.IsRoot)
		assert.True(t, leaf4.Header.IsLeaf)
		assert.Equal(t, 6, int(leaf4.Header.Parent))
		assert.Equal(t, 3, int(leaf4.Header.Keys))
		assert.Equal(t, []int64{7, 8, 9}, leaf4.Keys())
		assert.Empty(t, leaf4.Children())
	})

	t.Run("Keep inserting more keys", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			err = anIndex.Insert(context.Background(), key)
			require.NoError(t, err)
			key++
		}

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

		assert.True(t, rootNode.Header.IsRoot)
		assert.False(t, rootNode.Header.IsLeaf)
		assert.Equal(t, 2, int(rootNode.Header.Keys))
		assert.Equal(t, []int64{4, 8}, rootNode.Keys())
		assert.Equal(t, []uint32{5, 6, 9}, rootNode.Children())

		// Internal nodes

		assert.False(t, internal1.Header.IsRoot)
		assert.False(t, internal1.Header.IsLeaf)
		assert.Equal(t, 0, int(internal1.Header.Parent))
		assert.Equal(t, 1, int(internal1.Header.Keys))
		assert.Equal(t, []int64{2}, internal1.Keys())
		assert.Equal(t, []uint32{1, 2}, internal1.Children())

		assert.False(t, internal2.Header.IsRoot)
		assert.False(t, internal2.Header.IsLeaf)
		assert.Equal(t, 0, int(internal2.Header.Parent))
		assert.Equal(t, 1, int(internal2.Header.Keys))
		assert.Equal(t, []int64{6}, internal2.Keys())
		assert.Equal(t, []uint32{3, 4}, internal2.Children())

		assert.False(t, internal3.Header.IsRoot)
		assert.False(t, internal3.Header.IsLeaf)
		assert.Equal(t, 0, int(internal3.Header.Parent))
		assert.Equal(t, 2, int(internal3.Header.Keys))
		assert.Equal(t, []int64{10, 12}, internal3.Keys())
		assert.Equal(t, []uint32{7, 8, 10}, internal3.Children())

		// Leaf nodes

		assert.False(t, leaf1.Header.IsRoot)
		assert.True(t, leaf1.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf1.Header.Parent))
		assert.Equal(t, 1, int(leaf1.Header.Keys))
		assert.Equal(t, []int64{1}, leaf1.Keys())
		assert.Empty(t, leaf1.Children())

		assert.False(t, leaf2.Header.IsRoot)
		assert.True(t, leaf2.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf2.Header.Parent))
		assert.Equal(t, 1, int(leaf2.Header.Keys))
		assert.Equal(t, []int64{3}, leaf2.Keys())
		assert.Empty(t, leaf2.Children())

		assert.False(t, leaf3.Header.IsRoot)
		assert.True(t, leaf3.Header.IsLeaf)
		assert.Equal(t, 6, int(leaf3.Header.Parent))
		assert.Equal(t, 1, int(leaf3.Header.Keys))
		assert.Equal(t, []int64{5}, leaf3.Keys())
		assert.Empty(t, leaf3.Children())

		assert.False(t, leaf4.Header.IsRoot)
		assert.True(t, leaf4.Header.IsLeaf)
		assert.Equal(t, 6, int(leaf4.Header.Parent))
		assert.Equal(t, 1, int(leaf4.Header.Keys))
		assert.Equal(t, []int64{7}, leaf4.Keys())
		assert.Empty(t, leaf4.Children())

		assert.False(t, leaf5.Header.IsRoot)
		assert.True(t, leaf5.Header.IsLeaf)
		assert.Equal(t, 9, int(leaf5.Header.Parent))
		assert.Equal(t, 1, int(leaf5.Header.Keys))
		assert.Equal(t, []int64{9}, leaf5.Keys())
		assert.Empty(t, leaf5.Children())

		assert.False(t, leaf6.Header.IsRoot)
		assert.True(t, leaf6.Header.IsLeaf)
		assert.Equal(t, 9, int(leaf6.Header.Parent))
		assert.Equal(t, 1, int(leaf6.Header.Keys))
		assert.Equal(t, []int64{11}, leaf6.Keys())
		assert.Empty(t, leaf6.Children())

		assert.False(t, leaf7.Header.IsRoot)
		assert.True(t, leaf7.Header.IsLeaf)
		assert.Equal(t, 9, int(leaf7.Header.Parent))
		assert.Equal(t, 2, int(leaf7.Header.Keys))
		assert.Equal(t, []int64{13, 14}, leaf7.Keys())
		assert.Empty(t, leaf7.Children())
	})

	actualKeys := []int64{}
	err = anIndex.BFS(func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[int64])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	expectedKys := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

	assert.ElementsMatch(t, expectedKys, actualKeys)
}

func TestUniqueIndex_Insert_OutOfOrder(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	aColumn := Column{Name: "test_column", Kind: Int8, Size: 8}
	indexPager := aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size))
	anIndex := NewUniqueIndex[int64](zap.NewNop(), "test_index", aColumn, indexPager, 0)
	anIndex.maximumKeys = 3

	keys := []int64{9, 5, 3, 14, 11, 1, 7, 15, 12, 6, 10, 13, 4, 8, 2}

	for _, key := range keys {
		err := anIndex.Insert(context.Background(), key)
		require.NoError(t, err)
	}

	/*
								        +----------------------+
								        |           7          |
								        +----------------------+
		                               /                        \
							+---------+                          +---------------+
							|  3 , 5  |                          |  11   ,   14  |
							+---------+                          +---------------+
					       /     |     \                        /        |        \
			 	 +--------+    +---+    +---+   +--------------+   +-----------+   +----+
				 |  1 , 2 |    | 4 |    | 6 |   |  8 , 9 , 10  |   |  12 , 13  |   | 15 |
				 +--------+    +---+    +---+   +--------------+   +-----------+   +----+
	*/

	actualKeys := []int64{}
	err = anIndex.BFS(func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[int64])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	assert.ElementsMatch(t, keys, actualKeys)

	require.NoError(t, anIndex.print())

	// var (
	// 	rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
	// 	internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
	// 	internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
	// 	leaf1     = aPager.pages[1].IndexNode.(*IndexNode[int64])
	// 	leaf2     = aPager.pages[3].IndexNode.(*IndexNode[int64])
	// 	leaf3     = aPager.pages[2].IndexNode.(*IndexNode[int64])
	// 	leaf4     = aPager.pages[4].IndexNode.(*IndexNode[int64])
	// )

	// // Root node

	// assert.True(t, rootNode.Header.IsRoot)
	// assert.False(t, rootNode.Header.IsLeaf)
	// assert.Equal(t, 1, int(rootNode.Header.Keys))
	// assert.Equal(t, []int64{5}, rootNode.Keys())
	// assert.Equal(t, []uint32{5, 6}, rootNode.Children())

	// // Internal nodes

	// assert.False(t, internal1.Header.IsRoot)
	// assert.False(t, internal1.Header.IsLeaf)
	// assert.Equal(t, 0, int(internal1.Header.Parent))
	// assert.Equal(t, 1, int(internal1.Header.Keys))
	// assert.Equal(t, []int64{2}, internal1.Keys())
	// assert.Equal(t, []uint32{1, 3}, internal1.Children())

	// assert.False(t, internal2.Header.IsRoot)
	// assert.False(t, internal2.Header.IsLeaf)
	// assert.Equal(t, 0, int(internal2.Header.Parent))
	// assert.Equal(t, 1, int(internal2.Header.Keys))
	// assert.Equal(t, []int64{8}, internal2.Keys())
	// assert.Equal(t, []uint32{2, 4}, internal2.Children())

	// // Leaf nodes

	// assert.False(t, leaf1.Header.IsRoot)
	// assert.True(t, leaf1.Header.IsLeaf)
	// assert.Equal(t, 5, int(leaf1.Header.Parent))
	// assert.Equal(t, 1, int(leaf1.Header.Keys))
	// assert.Equal(t, []int64{1}, leaf1.Keys())
	// assert.Empty(t, leaf1.Children())

	// assert.False(t, leaf2.Header.IsRoot)
	// assert.True(t, leaf2.Header.IsLeaf)
	// assert.Equal(t, 5, int(leaf2.Header.Parent))
	// assert.Equal(t, 2, int(leaf2.Header.Keys))
	// assert.Equal(t, []int64{3, 4}, leaf2.Keys())
	// assert.Empty(t, leaf2.Children())

	// assert.False(t, leaf3.Header.IsRoot)
	// assert.True(t, leaf3.Header.IsLeaf)
	// assert.Equal(t, 6, int(leaf3.Header.Parent))
	// assert.Equal(t, 2, int(leaf3.Header.Keys))
	// assert.Equal(t, []int64{6, 7}, leaf3.Keys())
	// assert.Empty(t, leaf3.Children())

	// assert.False(t, leaf4.Header.IsRoot)
	// assert.True(t, leaf4.Header.IsLeaf)
	// assert.Equal(t, 6, int(leaf4.Header.Parent))
	// assert.Equal(t, 3, int(leaf4.Header.Keys))
	// assert.Equal(t, []int64{9, 10, 11}, leaf4.Keys())
	// assert.Empty(t, leaf4.Children())

	// assert.True(t, false)
}
