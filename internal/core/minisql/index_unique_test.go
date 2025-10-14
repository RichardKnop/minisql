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
			err = anIndex.Insert(context.Background(), key, uint64(key+100))
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
		assert.Equal(t, []uint64{101, 102, 103}, rootNode.RowIDs())
	})

	t.Run("Insert duplicate key fails", func(t *testing.T) {
		err = anIndex.Insert(context.Background(), key-1, uint64(key-1+100))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("Insert 4th key, causes a split", func(t *testing.T) {
		err = anIndex.Insert(context.Background(), key, uint64(key+100))
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
		assert.Equal(t, []uint64{102}, rootNode.RowIDs())

		assert.False(t, leftChild.Header.IsRoot)
		assert.True(t, leftChild.Header.IsLeaf)
		assert.Equal(t, 0, int(leftChild.Header.Parent))
		assert.Equal(t, 1, int(leftChild.Header.Keys))
		assert.Equal(t, []int64{1}, leftChild.Keys())
		assert.Equal(t, []uint64{101}, leftChild.RowIDs())

		assert.False(t, rightChild.Header.IsRoot)
		assert.True(t, rightChild.Header.IsLeaf)
		assert.Equal(t, 0, int(rightChild.Header.Parent))
		assert.Equal(t, 2, int(rightChild.Header.Keys))
		assert.Equal(t, []int64{3, 4}, rightChild.Keys())
		assert.Equal(t, []uint64{103, 104}, rightChild.RowIDs())
	})

	t.Run("Insert 2 more keys, another split", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			err = anIndex.Insert(context.Background(), key, uint64(key+100))
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
		assert.Equal(t, []uint64{102, 104}, rootNode.RowIDs())

		assert.False(t, leftChild.Header.IsRoot)
		assert.True(t, leftChild.Header.IsLeaf)
		assert.Equal(t, 0, int(leftChild.Header.Parent))
		assert.Equal(t, 1, int(leftChild.Header.Keys))
		assert.Equal(t, []int64{1}, leftChild.Keys())
		assert.Empty(t, leftChild.Children())
		assert.Equal(t, []uint64{101}, leftChild.RowIDs())

		assert.False(t, middleChild.Header.IsRoot)
		assert.True(t, middleChild.Header.IsLeaf)
		assert.Equal(t, 0, int(middleChild.Header.Parent))
		assert.Equal(t, 1, int(middleChild.Header.Keys))
		assert.Equal(t, []int64{3}, middleChild.Keys())
		assert.Empty(t, middleChild.Children())
		assert.Equal(t, []uint64{103}, middleChild.RowIDs())

		assert.False(t, rightChild.Header.IsRoot)
		assert.True(t, rightChild.Header.IsLeaf)
		assert.Equal(t, 0, int(rightChild.Header.Parent))
		assert.Equal(t, 2, int(rightChild.Header.Keys))
		assert.Equal(t, []int64{5, 6}, rightChild.Keys())
		assert.Empty(t, rightChild.Children())
		assert.Equal(t, []uint64{105, 106}, rightChild.RowIDs())
	})

	t.Run("Insert 2 more keys, another split", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			err = anIndex.Insert(context.Background(), key, uint64(key+100))
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
		assert.Equal(t, []uint64{102, 104, 106}, rootNode.RowIDs())

		assert.False(t, leaf1.Header.IsRoot)
		assert.True(t, leaf1.Header.IsLeaf)
		assert.Equal(t, 0, int(leaf1.Header.Parent))
		assert.Equal(t, 1, int(leaf1.Header.Keys))
		assert.Equal(t, []int64{1}, leaf1.Keys())
		assert.Empty(t, leaf1.Children())
		assert.Equal(t, []uint64{101}, leaf1.RowIDs())

		assert.False(t, leaf2.Header.IsRoot)
		assert.True(t, leaf2.Header.IsLeaf)
		assert.Equal(t, 0, int(leaf2.Header.Parent))
		assert.Equal(t, 1, int(leaf2.Header.Keys))
		assert.Equal(t, []int64{3}, leaf2.Keys())
		assert.Empty(t, leaf2.Children())
		assert.Equal(t, []uint64{103}, leaf2.RowIDs())

		assert.False(t, leaf3.Header.IsRoot)
		assert.True(t, leaf3.Header.IsLeaf)
		assert.Equal(t, 0, int(leaf3.Header.Parent))
		assert.Equal(t, 1, int(leaf3.Header.Keys))
		assert.Equal(t, []int64{5}, leaf3.Keys())
		assert.Empty(t, leaf3.Children())
		assert.Equal(t, []uint64{105}, leaf3.RowIDs())

		assert.False(t, leaf4.Header.IsRoot)
		assert.True(t, leaf4.Header.IsLeaf)
		assert.Equal(t, 0, int(leaf4.Header.Parent))
		assert.Equal(t, 2, int(leaf4.Header.Keys))
		assert.Equal(t, []int64{7, 8}, leaf4.Keys())
		assert.Empty(t, leaf4.Children())
		assert.Equal(t, []uint64{107, 108}, leaf4.RowIDs())
	})

	t.Run("Insert 1 more key, internal split", func(t *testing.T) {
		err = anIndex.Insert(context.Background(), key, uint64(key+100))
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
		assert.Equal(t, []uint64{104}, rootNode.RowIDs())

		// Internal nodes

		assert.False(t, internal1.Header.IsRoot)
		assert.False(t, internal1.Header.IsLeaf)
		assert.Equal(t, 0, int(internal1.Header.Parent))
		assert.Equal(t, 1, int(internal1.Header.Keys))
		assert.Equal(t, []int64{2}, internal1.Keys())
		assert.Equal(t, []uint32{1, 2}, internal1.Children())
		assert.Equal(t, []uint64{102}, internal1.RowIDs())

		assert.False(t, internal2.Header.IsRoot)
		assert.False(t, internal2.Header.IsLeaf)
		assert.Equal(t, 0, int(internal2.Header.Parent))
		assert.Equal(t, 1, int(internal2.Header.Keys))
		assert.Equal(t, []int64{6}, internal2.Keys())
		assert.Equal(t, []uint32{3, 4}, internal2.Children())
		assert.Equal(t, []uint64{106}, internal2.RowIDs())

		// Leaf nodes

		assert.False(t, leaf1.Header.IsRoot)
		assert.True(t, leaf1.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf1.Header.Parent))
		assert.Equal(t, 1, int(leaf1.Header.Keys))
		assert.Equal(t, []int64{1}, leaf1.Keys())
		assert.Empty(t, leaf1.Children())
		assert.Equal(t, []uint64{101}, leaf1.RowIDs())

		assert.False(t, leaf2.Header.IsRoot)
		assert.True(t, leaf2.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf2.Header.Parent))
		assert.Equal(t, 1, int(leaf2.Header.Keys))
		assert.Equal(t, []int64{3}, leaf2.Keys())
		assert.Empty(t, leaf2.Children())
		assert.Equal(t, []uint64{103}, leaf2.RowIDs())

		assert.False(t, leaf3.Header.IsRoot)
		assert.True(t, leaf3.Header.IsLeaf)
		assert.Equal(t, 6, int(leaf3.Header.Parent))
		assert.Equal(t, 1, int(leaf3.Header.Keys))
		assert.Equal(t, []int64{5}, leaf3.Keys())
		assert.Empty(t, leaf3.Children())
		assert.Equal(t, []uint64{105}, leaf3.RowIDs())

		assert.False(t, leaf4.Header.IsRoot)
		assert.True(t, leaf4.Header.IsLeaf)
		assert.Equal(t, 6, int(leaf4.Header.Parent))
		assert.Equal(t, 3, int(leaf4.Header.Keys))
		assert.Equal(t, []int64{7, 8, 9}, leaf4.Keys())
		assert.Empty(t, leaf4.Children())
		assert.Equal(t, []uint64{107, 108, 109}, leaf4.RowIDs())
	})

	t.Run("Keep inserting more keys", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			err = anIndex.Insert(context.Background(), key, uint64(key+100))
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
		assert.Equal(t, []uint64{104, 108}, rootNode.RowIDs())

		// Internal nodes

		assert.False(t, internal1.Header.IsRoot)
		assert.False(t, internal1.Header.IsLeaf)
		assert.Equal(t, 0, int(internal1.Header.Parent))
		assert.Equal(t, 1, int(internal1.Header.Keys))
		assert.Equal(t, []int64{2}, internal1.Keys())
		assert.Equal(t, []uint32{1, 2}, internal1.Children())
		assert.Equal(t, []uint64{102}, internal1.RowIDs())

		assert.False(t, internal2.Header.IsRoot)
		assert.False(t, internal2.Header.IsLeaf)
		assert.Equal(t, 0, int(internal2.Header.Parent))
		assert.Equal(t, 1, int(internal2.Header.Keys))
		assert.Equal(t, []int64{6}, internal2.Keys())
		assert.Equal(t, []uint32{3, 4}, internal2.Children())
		assert.Equal(t, []uint64{106}, internal2.RowIDs())

		assert.False(t, internal3.Header.IsRoot)
		assert.False(t, internal3.Header.IsLeaf)
		assert.Equal(t, 0, int(internal3.Header.Parent))
		assert.Equal(t, 2, int(internal3.Header.Keys))
		assert.Equal(t, []int64{10, 12}, internal3.Keys())
		assert.Equal(t, []uint32{7, 8, 10}, internal3.Children())
		assert.Equal(t, []uint64{110, 112}, internal3.RowIDs())

		// Leaf nodes

		assert.False(t, leaf1.Header.IsRoot)
		assert.True(t, leaf1.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf1.Header.Parent))
		assert.Equal(t, 1, int(leaf1.Header.Keys))
		assert.Equal(t, []int64{1}, leaf1.Keys())
		assert.Empty(t, leaf1.Children())
		assert.Equal(t, []uint64{101}, leaf1.RowIDs())

		assert.False(t, leaf2.Header.IsRoot)
		assert.True(t, leaf2.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf2.Header.Parent))
		assert.Equal(t, 1, int(leaf2.Header.Keys))
		assert.Equal(t, []int64{3}, leaf2.Keys())
		assert.Empty(t, leaf2.Children())
		assert.Equal(t, []uint64{103}, leaf2.RowIDs())

		assert.False(t, leaf3.Header.IsRoot)
		assert.True(t, leaf3.Header.IsLeaf)
		assert.Equal(t, 6, int(leaf3.Header.Parent))
		assert.Equal(t, 1, int(leaf3.Header.Keys))
		assert.Equal(t, []int64{5}, leaf3.Keys())
		assert.Empty(t, leaf3.Children())
		assert.Equal(t, []uint64{105}, leaf3.RowIDs())

		assert.False(t, leaf4.Header.IsRoot)
		assert.True(t, leaf4.Header.IsLeaf)
		assert.Equal(t, 6, int(leaf4.Header.Parent))
		assert.Equal(t, 1, int(leaf4.Header.Keys))
		assert.Equal(t, []int64{7}, leaf4.Keys())
		assert.Empty(t, leaf4.Children())
		assert.Equal(t, []uint64{107}, leaf4.RowIDs())

		assert.False(t, leaf5.Header.IsRoot)
		assert.True(t, leaf5.Header.IsLeaf)
		assert.Equal(t, 9, int(leaf5.Header.Parent))
		assert.Equal(t, 1, int(leaf5.Header.Keys))
		assert.Equal(t, []int64{9}, leaf5.Keys())
		assert.Empty(t, leaf5.Children())
		assert.Equal(t, []uint64{109}, leaf5.RowIDs())

		assert.False(t, leaf6.Header.IsRoot)
		assert.True(t, leaf6.Header.IsLeaf)
		assert.Equal(t, 9, int(leaf6.Header.Parent))
		assert.Equal(t, 1, int(leaf6.Header.Keys))
		assert.Equal(t, []int64{11}, leaf6.Keys())
		assert.Empty(t, leaf6.Children())
		assert.Equal(t, []uint64{111}, leaf6.RowIDs())

		assert.False(t, leaf7.Header.IsRoot)
		assert.True(t, leaf7.Header.IsLeaf)
		assert.Equal(t, 9, int(leaf7.Header.Parent))
		assert.Equal(t, 2, int(leaf7.Header.Keys))
		assert.Equal(t, []int64{13, 14}, leaf7.Keys())
		assert.Empty(t, leaf7.Children())
		assert.Equal(t, []uint64{113, 114}, leaf7.RowIDs())
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

	keys := []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}

	for _, key := range keys {
		err := anIndex.Insert(context.Background(), key, uint64(key+100))
		require.NoError(t, err)
	}

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

	actualKeys := []int64{}
	err = anIndex.BFS(func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[int64])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	assert.ElementsMatch(t, keys, actualKeys)

	require.NoError(t, anIndex.print())

	// anIndex.debug = true
	// err = anIndex.Insert(context.Background(), 13)
	// require.NoError(t, err)

	// fmt.Println("===============After inserting 13")
	// require.NoError(t, anIndex.print())

	// assert.False(t, true)

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

	assert.True(t, rootNode.Header.IsRoot)
	assert.False(t, rootNode.Header.IsLeaf)
	assert.Equal(t, 2, int(rootNode.Header.Keys))
	assert.Equal(t, []int64{9, 16}, rootNode.Keys())
	assert.Equal(t, []uint32{5, 6, 10}, rootNode.Children())
	assert.Equal(t, []uint64{109, 116}, rootNode.RowIDs())

	// Internal nodes

	assert.False(t, internal1.Header.IsRoot)
	assert.False(t, internal1.Header.IsLeaf)
	assert.Equal(t, 0, int(internal1.Header.Parent))
	assert.Equal(t, 2, int(internal1.Header.Keys))
	assert.Equal(t, []int64{2, 5}, internal1.Keys())
	assert.Equal(t, []uint32{1, 9, 4}, internal1.Children())
	assert.Equal(t, []uint64{102, 105}, internal1.RowIDs())

	assert.False(t, internal2.Header.IsRoot)
	assert.False(t, internal2.Header.IsLeaf)
	assert.Equal(t, 0, int(internal2.Header.Parent))
	assert.Equal(t, 2, int(internal2.Header.Keys))
	assert.Equal(t, []int64{11, 13}, internal2.Keys())
	assert.Equal(t, []uint32{2, 7, 11}, internal2.Children())
	assert.Equal(t, []uint64{111, 113}, internal2.RowIDs())

	assert.False(t, internal3.Header.IsRoot)
	assert.False(t, internal3.Header.IsLeaf)
	assert.Equal(t, 0, int(internal3.Header.Parent))
	assert.Equal(t, 1, int(internal3.Header.Keys))
	assert.Equal(t, []int64{19}, internal3.Keys())
	assert.Equal(t, []uint32{3, 8}, internal3.Children())
	assert.Equal(t, []uint64{119}, internal3.RowIDs())

	// Leaf nodes

	assert.False(t, leaf1.Header.IsRoot)
	assert.True(t, leaf1.Header.IsLeaf)
	assert.Equal(t, 5, int(leaf1.Header.Parent))
	assert.Equal(t, 1, int(leaf1.Header.Keys))
	assert.Equal(t, []int64{1}, leaf1.Keys())
	assert.Empty(t, leaf1.Children())
	assert.Equal(t, []uint64{101}, leaf1.RowIDs())

	assert.False(t, leaf2.Header.IsRoot)
	assert.True(t, leaf2.Header.IsLeaf)
	assert.Equal(t, 5, int(leaf2.Header.Parent))
	assert.Equal(t, 2, int(leaf2.Header.Keys))
	assert.Equal(t, []int64{3, 4}, leaf2.Keys())
	assert.Empty(t, leaf2.Children())
	assert.Equal(t, []uint64{103, 104}, leaf2.RowIDs())

	assert.False(t, leaf3.Header.IsRoot)
	assert.True(t, leaf3.Header.IsLeaf)
	assert.Equal(t, 5, int(leaf3.Header.Parent))
	assert.Equal(t, 3, int(leaf3.Header.Keys))
	assert.Equal(t, []int64{6, 7, 8}, leaf3.Keys())
	assert.Empty(t, leaf3.Children())
	assert.Equal(t, []uint64{106, 107, 108}, leaf3.RowIDs())

	assert.False(t, leaf4.Header.IsRoot)
	assert.True(t, leaf4.Header.IsLeaf)
	assert.Equal(t, 6, int(leaf4.Header.Parent))
	assert.Equal(t, 1, int(leaf4.Header.Keys))
	assert.Equal(t, []int64{10}, leaf4.Keys())
	assert.Empty(t, leaf4.Children())
	assert.Equal(t, []uint64{110}, leaf4.RowIDs())

	assert.False(t, leaf5.Header.IsRoot)
	assert.True(t, leaf5.Header.IsLeaf)
	assert.Equal(t, 6, int(leaf5.Header.Parent))
	assert.Equal(t, 1, int(leaf5.Header.Keys))
	assert.Equal(t, []int64{12}, leaf5.Keys())
	assert.Empty(t, leaf5.Children())
	assert.Equal(t, []uint64{112}, leaf5.RowIDs())

	assert.False(t, leaf6.Header.IsRoot)
	assert.True(t, leaf6.Header.IsLeaf)
	assert.Equal(t, 6, int(leaf6.Header.Parent))
	assert.Equal(t, 2, int(leaf6.Header.Keys))
	assert.Equal(t, []int64{14, 15}, leaf6.Keys())
	assert.Empty(t, leaf6.Children())
	assert.Equal(t, []uint64{114, 115}, leaf6.RowIDs())

	assert.False(t, leaf7.Header.IsRoot)
	assert.True(t, leaf7.Header.IsLeaf)
	assert.Equal(t, 10, int(leaf7.Header.Parent))
	assert.Equal(t, 2, int(leaf7.Header.Keys))
	assert.Equal(t, []int64{17, 18}, leaf7.Keys())
	assert.Empty(t, leaf7.Children())
	assert.Equal(t, []uint64{117, 118}, leaf7.RowIDs())

	assert.False(t, leaf8.Header.IsRoot)
	assert.True(t, leaf8.Header.IsLeaf)
	assert.Equal(t, 10, int(leaf8.Header.Parent))
	assert.Equal(t, 2, int(leaf8.Header.Keys))
	assert.Equal(t, []int64{20, 21}, leaf8.Keys())
	assert.Empty(t, leaf8.Children())
	assert.Equal(t, []uint64{120, 121}, leaf8.RowIDs())
}
