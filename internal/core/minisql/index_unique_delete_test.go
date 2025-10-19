package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUniqueIndex_Delete(t *testing.T) {
	t.Parallel()
	// t.Skip()

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

	//require.NoError(t, anIndex.print())

	t.Run("Delete a key from leftmost leaf", func(t *testing.T) {
		err := anIndex.Delete(context.Background(), 1)
		require.NoError(t, err)

		/*
										+------------------------------------------------+
										|        9              ,             16         |
										+------------------------------------------------+
					                   /                        |                         \
						      +---------+                     +---------+                  +---------------+
						      |  3 , 5  |                     | 11 , 13 |                  |      19       |
						      +---------+                     +---------+                  +---------------+
					         /     |     \                   /     |     \                /                 \
			            +---+    +---+    +-----------+  +----+   +----+  +---------+   +---------+         +---------+
			            | 2 |    | 4 |    | 6 , 7 , 8 |  | 10 |   | 12 |  | 14 , 15 |   | 17 , 18 |         | 20 , 21 |
			            +---+    +---+    +-----------+  +----+   +----+  +---------+   +---------+         +---------+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		//require.NoError(t, anIndex.print())

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
		assert.Equal(t, []int64{3, 5}, internal1.Keys())
		assert.Equal(t, []uint32{1, 9, 4}, internal1.Children())
		assert.Equal(t, []uint64{103, 105}, internal1.RowIDs())

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
		assert.Equal(t, []int64{2}, leaf1.Keys())
		assert.Empty(t, leaf1.Children())
		assert.Equal(t, []uint64{102}, leaf1.RowIDs())

		assert.False(t, leaf2.Header.IsRoot)
		assert.True(t, leaf2.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf2.Header.Parent))
		assert.Equal(t, 1, int(leaf2.Header.Keys))
		assert.Equal(t, []int64{4}, leaf2.Keys())
		assert.Empty(t, leaf2.Children())
		assert.Equal(t, []uint64{104}, leaf2.RowIDs())

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
	})

	t.Run("Delete another key, no rebalance needed", func(t *testing.T) {
		err := anIndex.Delete(context.Background(), 4)
		require.NoError(t, err)

		err = anIndex.Delete(context.Background(), 5)
		require.NoError(t, err)

		/*
										+------------------------------------------------+
										|        9              ,             16         |
										+------------------------------------------------+
					                   /                        |                         \
						      +---------+                     +---------+                  +---------------+
						      |  3 , 7  |                     | 11 , 13 |                  |      19       |
						      +---------+                     +---------+                  +---------------+
					         /     |     \                   /     |     \                /                 \
			            +---+    +---+    +---+        +----+   +----+  +---------+   +---------+         +---------+
			            | 2 |    | 6 |    | 8 |        | 10 |   | 12 |  | 14 , 15 |   | 17 , 18 |         | 20 , 21 |
			            +---+    +---+    +---+        +----+   +----+  +---------+   +---------+         +---------+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		//require.NoError(t, anIndex.print())

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
		assert.Equal(t, []int64{3, 7}, internal1.Keys())
		assert.Equal(t, []uint32{1, 9, 4}, internal1.Children())
		assert.Equal(t, []uint64{103, 107}, internal1.RowIDs())

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
		assert.Equal(t, []int64{2}, leaf1.Keys())
		assert.Empty(t, leaf1.Children())
		assert.Equal(t, []uint64{102}, leaf1.RowIDs())

		assert.False(t, leaf2.Header.IsRoot)
		assert.True(t, leaf2.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf2.Header.Parent))
		assert.Equal(t, 1, int(leaf2.Header.Keys))
		assert.Equal(t, []int64{6}, leaf2.Keys())
		assert.Empty(t, leaf2.Children())
		assert.Equal(t, []uint64{106}, leaf2.RowIDs())

		assert.False(t, leaf3.Header.IsRoot)
		assert.True(t, leaf3.Header.IsLeaf)
		assert.Equal(t, 5, int(leaf3.Header.Parent))
		assert.Equal(t, 1, int(leaf3.Header.Keys))
		assert.Equal(t, []int64{8}, leaf3.Keys())
		assert.Empty(t, leaf3.Children())
		assert.Equal(t, []uint64{108}, leaf3.RowIDs())

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
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 8)
		require.NoError(t, err)

		/*
										+------------------------------------------------+
										|        9              ,             16         |
										+------------------------------------------------+
					                   /                        |                         \
						        +-----+                     +---------+                  +---------------+
						        |  3  |                     | 11 , 13 |                  |      19       |
						        +-----+                     +---------+                  +---------------+
					           /       \                   /     |     \                /                 \
			              +---+       +-------+      +----+   +----+  +---------+   +---------+         +---------+
			              | 2 |       | 6 , 7 |      | 10 |   | 12 |  | 14 , 15 |   | 17 , 18 |         | 20 , 21 |
			              +---+       +-------+      +----+   +----+  +---------+   +---------+         +---------+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())
	})

	// t.Run("Delete another key, no rebalance needed", func(t *testing.T) {
	// 	anIndex.debug = true
	// 	err = anIndex.Delete(context.Background(), 19)
	// 	require.NoError(t, err)

	// 	/*
	// 									+------------------------------------------------+
	// 									|        9              ,             13         |
	// 									+------------------------------------------------+
	// 				                   /                        |                         \
	// 					        +-----+                     +-----+                  +---------------+
	// 					        |  3  |                     | 11  |                  |    16 , 18    |
	// 					        +-----+                     +-----+                  +---------------+
	// 				           /       \                   /     |     \                /                 \
	// 		              +---+       +-------+      +----+   +----+  +---------+   +---------+         +---------+
	// 		              | 2 |       | 6 , 7 |      | 10 |   | 12 |  | 14 , 15 |   | 17 , 18 |         | 20 , 21 |
	// 		              +---+       +-------+      +----+   +----+  +---------+   +---------+         +---------+
	// 	*/

	// 	// actualKeys = []int64{}
	// 	// err = anIndex.BFS(func(aPage *Page) {
	// 	// 	node := aPage.IndexNode.(*IndexNode[int64])
	// 	// 	actualKeys = append(actualKeys, node.Keys()...)
	// 	// })
	// 	// require.NoError(t, err)

	// 	// expectedKeys := []int64{2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}

	// 	// assert.ElementsMatch(t, expectedKeys, actualKeys)

	// 	require.NoError(t, anIndex.print())

	// 	assert.True(t, false)

	// })

	// // Check that the second leaf node is now a free page
	// assert.Nil(t, leaf2Page.IndexNode)
	// assert.NotNil(t, leaf2Page.FreePage)
	// assert.Equal(t, 0, int(leaf2Page.FreePage.NextFreePage))
	// assert.Equal(t, int(leaf2Page.Index), int(aPager.dbHeader.FirstFreePage))
	// assert.Equal(t, 1, int(aPager.dbHeader.FreePageCount))
}
