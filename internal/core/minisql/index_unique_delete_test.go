package minisql

import (
	"context"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUniqueIndex_Delete(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	aColumn := Column{Name: "test_column", Kind: Int8, Size: 8}
	idxPager := aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size))
	anIndex := NewUniqueIndex[int64](zap.NewNop(), "test_index", aColumn, idxPager, 0)
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
	assertIndexNode(t, rootNode, true, false, 0, []int64{9, 16}, []uint32{5, 6, 10})
	// Internal nodes
	assertIndexNode(t, internal1, false, false, 0, []int64{2, 5}, []uint32{1, 9, 4})
	assertIndexNode(t, internal2, false, false, 0, []int64{11, 13}, []uint32{2, 7, 11})
	assertIndexNode(t, internal3, false, false, 0, []int64{19}, []uint32{3, 8})
	// Leaf nodes
	assertIndexNode(t, leaf1, false, true, 5, []int64{1}, nil)
	assertIndexNode(t, leaf2, false, true, 5, []int64{3, 4}, nil)
	assertIndexNode(t, leaf3, false, true, 5, []int64{6, 7, 8}, nil)
	assertIndexNode(t, leaf4, false, true, 6, []int64{10}, nil)
	assertIndexNode(t, leaf5, false, true, 6, []int64{12}, nil)
	assertIndexNode(t, leaf6, false, true, 6, []int64{14, 15}, nil)
	assertIndexNode(t, leaf7, false, true, 10, []int64{17, 18}, nil)
	assertIndexNode(t, leaf8, false, true, 10, []int64{20, 21}, nil)

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
		assertIndexNode(t, rootNode, true, false, 0, []int64{9, 16}, []uint32{5, 6, 10})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3, 5}, []uint32{1, 9, 4})
		assertIndexNode(t, internal2, false, false, 0, []int64{11, 13}, []uint32{2, 7, 11})
		assertIndexNode(t, internal3, false, false, 0, []int64{19}, []uint32{3, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{4}, nil)
		assertIndexNode(t, leaf3, false, true, 5, []int64{6, 7, 8}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{10}, nil)
		assertIndexNode(t, leaf5, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf6, false, true, 6, []int64{14, 15}, nil)
		assertIndexNode(t, leaf7, false, true, 10, []int64{17, 18}, nil)
		assertIndexNode(t, leaf8, false, true, 10, []int64{20, 21}, nil)

		// No page should be recycled yet
		assertFreePages(t, idxPager.(*indexPager[int64]), nil)
	})

	t.Run("Delete another key, no pages recyclet yet", func(t *testing.T) {
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
		assertIndexNode(t, rootNode, true, false, 0, []int64{9, 16}, []uint32{5, 6, 10})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3, 7}, []uint32{1, 9, 4})
		assertIndexNode(t, internal2, false, false, 0, []int64{11, 13}, []uint32{2, 7, 11})
		assertIndexNode(t, internal3, false, false, 0, []int64{19}, []uint32{3, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6}, nil)
		assertIndexNode(t, leaf3, false, true, 5, []int64{8}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{10}, nil)
		assertIndexNode(t, leaf5, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf6, false, true, 6, []int64{14, 15}, nil)
		assertIndexNode(t, leaf7, false, true, 10, []int64{17, 18}, nil)
		assertIndexNode(t, leaf8, false, true, 10, []int64{20, 21}, nil)

		// No page should be recycled yet
		assertFreePages(t, idxPager.(*indexPager[int64]), nil)
	})

	t.Run("Delete another key, first recycled page", func(t *testing.T) {
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

		var (
			rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
			internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
			internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
			internal3 = aPager.pages[10].IndexNode.(*IndexNode[int64])
			// leaves of first internal node
			leaf1 = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2 = aPager.pages[9].IndexNode.(*IndexNode[int64])
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[7].IndexNode.(*IndexNode[int64])
			leaf5 = aPager.pages[11].IndexNode.(*IndexNode[int64])
			// leaves of third node
			leaf6 = aPager.pages[3].IndexNode.(*IndexNode[int64])
			leaf7 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{9, 16}, []uint32{5, 6, 10})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{11, 13}, []uint32{2, 7, 11})
		assertIndexNode(t, internal3, false, false, 0, []int64{19}, []uint32{3, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{10}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf5, false, true, 6, []int64{14, 15}, nil)
		assertIndexNode(t, leaf6, false, true, 10, []int64{17, 18}, nil)
		assertIndexNode(t, leaf7, false, true, 10, []int64{20, 21}, nil)

		// Assert new recycled page
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{4})
	})

	t.Run("Delete another key, no page recycled", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 19)
		require.NoError(t, err)

		/*
										+------------------------------------------------+
										|        9              ,             13         |
										+------------------------------------------------+
					                   /                        |                         \
						        +-----+                      +----+                       +---------------+
						        |  3  |                      | 11 |                       |    16 , 18    |
						        +-----+                      +----+                       +---------------+
					           /       \                   /       \                     /        |        \
			              +---+       +-------+      +----+        +----+      +---------+      +----+      +---------+
			              | 2 |       | 6 , 7 |      | 10 |        | 12 |      | 14 , 15 |      | 17 |      | 20 , 21 |
			              +---+       +-------+      +----+        +----+      +---------+      +----+      +---------+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21}

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
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[7].IndexNode.(*IndexNode[int64])
			// leaves of third node
			leaf5 = aPager.pages[11].IndexNode.(*IndexNode[int64])
			leaf6 = aPager.pages[3].IndexNode.(*IndexNode[int64])
			leaf7 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{9, 13}, []uint32{5, 6, 10})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{11}, []uint32{2, 7})
		assertIndexNode(t, internal3, false, false, 0, []int64{16, 18}, []uint32{11, 3, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{10}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf5, false, true, 10, []int64{14, 15}, nil)
		assertIndexNode(t, leaf6, false, true, 10, []int64{17}, nil)
		assertIndexNode(t, leaf7, false, true, 10, []int64{20, 21}, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{4})
	})

	t.Run("Delete another key, no page recycled", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 20)
		require.NoError(t, err)
		/*
										+------------------------------------------------+
										|        9              ,             13         |
										+------------------------------------------------+
					                   /                        |                         \
						        +-----+                      +----+                       +---------------+
						        |  3  |                      | 11 |                       |    16 , 18    |
						        +-----+                      +----+                       +---------------+
					           /       \                   /       \                     /        |        \
			              +---+       +-------+      +----+        +----+      +---------+      +----+      +-----+
			              | 2 |       | 6 , 7 |      | 10 |        | 12 |      | 14 , 15 |      | 17 |      |  21 |
			              +---+       +-------+      +----+        +----+      +---------+      +----+      +-----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 21}

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
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[7].IndexNode.(*IndexNode[int64])
			// leaves of third node
			leaf5 = aPager.pages[11].IndexNode.(*IndexNode[int64])
			leaf6 = aPager.pages[3].IndexNode.(*IndexNode[int64])
			leaf7 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{9, 13}, []uint32{5, 6, 10})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{11}, []uint32{2, 7})
		assertIndexNode(t, internal3, false, false, 0, []int64{16, 18}, []uint32{11, 3, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{10}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf5, false, true, 10, []int64{14, 15}, nil)
		assertIndexNode(t, leaf6, false, true, 10, []int64{17}, nil)
		assertIndexNode(t, leaf7, false, true, 10, []int64{21}, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{4})
	})

	t.Run("Delete another key, no page recycled", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 16)
		require.NoError(t, err)
		/*
										+------------------------------------------------+
										|        9              ,             13         |
										+------------------------------------------------+
					                   /                        |                         \
						        +-----+                      +----+                       +---------------+
						        |  3  |                      | 11 |                       |    15 , 18    |
						        +-----+                      +----+                       +---------------+
					           /       \                   /       \                     /        |        \
			              +---+       +-------+      +----+        +----+           +----+      +----+      +-----+
			              | 2 |       | 6 , 7 |      | 10 |        | 12 |           | 14 |      | 17 |      |  21 |
			              +---+       +-------+      +----+        +----+           +----+      +----+      +-----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 15, 17, 18, 21}

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
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[7].IndexNode.(*IndexNode[int64])
			// leaves of third node
			leaf5 = aPager.pages[11].IndexNode.(*IndexNode[int64])
			leaf6 = aPager.pages[3].IndexNode.(*IndexNode[int64])
			leaf7 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{9, 13}, []uint32{5, 6, 10})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{11}, []uint32{2, 7})
		assertIndexNode(t, internal3, false, false, 0, []int64{15, 18}, []uint32{11, 3, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{10}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf5, false, true, 10, []int64{14}, nil)
		assertIndexNode(t, leaf6, false, true, 10, []int64{17}, nil)
		assertIndexNode(t, leaf7, false, true, 10, []int64{21}, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{4})
	})

	t.Run("Delete another key, page recycled", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 15)
		require.NoError(t, err)
		/*
										+------------------------------------------------+
										|        9              ,             13         |
										+------------------------------------------------+
					                   /                        |                         \
						        +-----+                      +----+                       +----+
						        |  3  |                      | 11 |                       | 18 |
						        +-----+                      +----+                       +----+
					           /       \                   /       \                     /      \
			              +---+       +-------+      +----+        +----+      +---------+       +----+
			              | 2 |       | 6 , 7 |      | 10 |        | 12 |      | 14 , 17 |       | 21 |
			              +---+       +-------+      +----+        +----+      +---------+       +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 17, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
			internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
			internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
			internal3 = aPager.pages[10].IndexNode.(*IndexNode[int64])
			// leaves of first internal node
			leaf1 = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2 = aPager.pages[9].IndexNode.(*IndexNode[int64])
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[7].IndexNode.(*IndexNode[int64])
			// leaves of third node
			leaf5 = aPager.pages[11].IndexNode.(*IndexNode[int64])
			leaf6 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{9, 13}, []uint32{5, 6, 10})
		// Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{11}, []uint32{2, 7})
		assertIndexNode(t, internal3, false, false, 0, []int64{18}, []uint32{11, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{10}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf5, false, true, 10, []int64{14, 17}, nil)
		assertIndexNode(t, leaf6, false, true, 10, []int64{21}, nil)

		// Assert new recycled page
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{3, 4})
	})

	t.Run("Delete another key, page recycled", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 13)
		require.NoError(t, err)
		/*
										+------------------+
										|        9         |
										+------------------+
					                   /                    \
						        +-----+                      +---------------------+
						        |  3  |                      |  11  ,   14   , 18  |
						        +-----+                      +---------------------+
					           /       \                    /       |        |      \
			              +---+       +-------+       +----+     +----+    +----+    +----+
			              | 2 |       | 6 , 7 |       | 10 |     | 12 |    | 17 |    | 21 |
			              +---+       +-------+       +----+     +----+    +----+    +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 9, 10, 11, 12, 14, 17, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
			internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
			internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
			// leaves of first internal node
			leaf1 = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2 = aPager.pages[9].IndexNode.(*IndexNode[int64])
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[7].IndexNode.(*IndexNode[int64])
			leaf5 = aPager.pages[11].IndexNode.(*IndexNode[int64])
			leaf6 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{9}, []uint32{5, 6})
		//Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{11, 14, 18}, []uint32{2, 7, 11, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{10}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf5, false, true, 6, []int64{17}, nil)
		assertIndexNode(t, leaf6, false, true, 6, []int64{21}, nil)

		// Assert new recycled page
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{10, 3, 4})
	})

	t.Run("Delete another key, page recycled", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 10)
		require.NoError(t, err)
		/*
										+------------------+
										|        9         |
										+------------------+
					                   /                    \
						        +-----+                      +------------+
						        |  3  |                      |  14  , 18  |
						        +-----+                      +------------+
					           /       \                    /       |        \
			              +---+       +-------+   +---------+    +----+      +----+
			              | 2 |       | 6 , 7 |   | 11,  12 |    | 17 |      | 21 |
			              +---+       +-------+   +---------+    +----+      +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 9, 11, 12, 14, 17, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
			internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
			internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
			// leaves of first internal node
			leaf1 = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2 = aPager.pages[9].IndexNode.(*IndexNode[int64])
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[11].IndexNode.(*IndexNode[int64])
			leaf5 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{9}, []uint32{5, 6})
		//Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{14, 18}, []uint32{2, 11, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{11, 12}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{17}, nil)
		assertIndexNode(t, leaf5, false, true, 6, []int64{21}, nil)

		// Assert new recycled page
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{7, 10, 3, 4})
	})

	t.Run("Delete another key, no page recycled", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 9)
		require.NoError(t, err)
		/*
										+------------------+
										|        11        |
										+------------------+
					                   /                    \
						        +-----+                      +------------+
						        |  3  |                      |  14  , 18  |
						        +-----+                      +------------+
					           /       \                    /       |        \
			              +---+       +-------+       +----+      +----+      +----+
			              | 2 |       | 6 , 7 |       | 12 |      | 17 |      | 21 |
			              +---+       +-------+       +----+      +----+      +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 11, 12, 14, 17, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
			internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
			internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
			// leaves of first internal node
			leaf1 = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2 = aPager.pages[9].IndexNode.(*IndexNode[int64])
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[11].IndexNode.(*IndexNode[int64])
			leaf5 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{11}, []uint32{5, 6})
		//Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{14, 18}, []uint32{2, 11, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{12}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{17}, nil)
		assertIndexNode(t, leaf5, false, true, 6, []int64{21}, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{7, 10, 3, 4})
	})

	t.Run("Delete another key, page recycled", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 11)
		require.NoError(t, err)
		/*
										+------------------+
										|        12        |
										+------------------+
					                   /                    \
						        +-----+                      +------+
						        |  3  |                      |  18  |
						        +-----+                      +------+
					           /       \                    /        \
			              +---+       +-------+       +---------+     +----+
			              | 2 |       | 6 , 7 |       | 14 , 17 |     | 21 |
			              +---+       +-------+       +---------+     +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 3, 6, 7, 12, 14, 17, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode  = aPager.pages[0].IndexNode.(*IndexNode[int64])
			internal1 = aPager.pages[5].IndexNode.(*IndexNode[int64])
			internal2 = aPager.pages[6].IndexNode.(*IndexNode[int64])
			// leaves of first internal node
			leaf1 = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2 = aPager.pages[9].IndexNode.(*IndexNode[int64])
			// leaves of second internal node
			leaf3 = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4 = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{12}, []uint32{5, 6})
		//Internal nodes
		assertIndexNode(t, internal1, false, false, 0, []int64{3}, []uint32{1, 9})
		assertIndexNode(t, internal2, false, false, 0, []int64{18}, []uint32{2, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 5, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 5, []int64{6, 7}, nil)
		assertIndexNode(t, leaf3, false, true, 6, []int64{14, 17}, nil)
		assertIndexNode(t, leaf4, false, true, 6, []int64{21}, nil)

		// Assert new recycled page
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{11, 7, 10, 3, 4})
	})

	t.Run("Delete another key, 2 pages recycled, only root and leaves left", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 3)
		require.NoError(t, err)
		/*
										+-------------------------+
										|   6   ,   12   ,   18   |
										+-------------------------+
					                   /        |        |         \
			                       +---+      +---+  +---------+    +----+
			                       | 2 |      | 7 |  | 14 , 17 |    | 21 |
			                       +---+      +---+  +---------+    +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 6, 7, 12, 14, 17, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
			leaf1    = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2    = aPager.pages[9].IndexNode.(*IndexNode[int64])
			leaf3    = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4    = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{6, 12, 18}, []uint32{1, 9, 2, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 0, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 0, []int64{7}, nil)
		assertIndexNode(t, leaf3, false, true, 0, []int64{14, 17}, nil)
		assertIndexNode(t, leaf4, false, true, 0, []int64{21}, nil)

		// Assert 2 new recycled pages
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 14)
		require.NoError(t, err)
		/*
										+-------------------------+
										|   6   ,   12   ,   18   |
										+-------------------------+
					                   /        |        |         \
			                       +---+      +---+    +----+      +----+
			                       | 2 |      | 7 |    | 17 |      | 21 |
			                       +---+      +---+    +----+      +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 6, 7, 12, 17, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
			leaf1    = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2    = aPager.pages[9].IndexNode.(*IndexNode[int64])
			leaf3    = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf4    = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{6, 12, 18}, []uint32{1, 9, 2, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 0, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 0, []int64{7}, nil)
		assertIndexNode(t, leaf3, false, true, 0, []int64{17}, nil)
		assertIndexNode(t, leaf4, false, true, 0, []int64{21}, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 6)
		require.NoError(t, err)
		/*
										+-----------------+
										|   12   ,   18   |
										+-----------------+
					                   /         |         \
			                   +-------+       +----+       +----+
			                   | 2 , 7 |       | 17 |       | 21 |
			                   +-------+       +----+       +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 7, 12, 17, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
			leaf1    = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2    = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf3    = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{12, 18}, []uint32{1, 2, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 0, []int64{2, 7}, nil)
		assertIndexNode(t, leaf2, false, true, 0, []int64{17}, nil)
		assertIndexNode(t, leaf3, false, true, 0, []int64{21}, nil)

		// Assert new recycled page
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 17)
		require.NoError(t, err)
		/*
										+----------------+
										|   7   ,   18   |
										+----------------+
					                   /         |        \
			                      +---+        +----+       +----+
			                      | 2 |        | 12 |       | 21 |
			                      +---+        +----+       +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 7, 12, 18, 21}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
			leaf1    = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2    = aPager.pages[2].IndexNode.(*IndexNode[int64])
			leaf3    = aPager.pages[8].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{7, 18}, []uint32{1, 2, 8})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 0, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 0, []int64{12}, nil)
		assertIndexNode(t, leaf3, false, true, 0, []int64{21}, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 21)
		require.NoError(t, err)
		/*
										+-------+
										|   7   |
										+-------+
					                   /         \
			                      +---+           +---------+
			                      | 2 |           | 12 , 18 |
			                      +---+           +---------+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{2, 7, 12, 18}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
			leaf1    = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2    = aPager.pages[2].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{7}, []uint32{1, 2})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 0, []int64{2}, nil)
		assertIndexNode(t, leaf2, false, true, 0, []int64{12, 18}, nil)

		// Assert new recycled page
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{8, 9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 2)
		require.NoError(t, err)
		/*
										+-------+
										|   12  |
										+-------+
					                   /         \
			                      +---+           +----+
			                      | 7 |           | 18 |
			                      +---+           +----+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{7, 12, 18}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
			leaf1    = aPager.pages[1].IndexNode.(*IndexNode[int64])
			leaf2    = aPager.pages[2].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, false, 0, []int64{12}, []uint32{1, 2})
		// Leaf nodes
		assertIndexNode(t, leaf1, false, true, 0, []int64{7}, nil)
		assertIndexNode(t, leaf2, false, true, 0, []int64{18}, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{8, 9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key, only root leaf left", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 18)
		require.NoError(t, err)
		/*
			+----------+
			|  7 , 12  |
			+----------+
		*/

		actualKeys = []int64{}
		err = anIndex.BFS(func(aPage *Page) {
			node := aPage.IndexNode.(*IndexNode[int64])
			actualKeys = append(actualKeys, node.Keys()...)
		})
		require.NoError(t, err)

		expectedKeys := []int64{7, 12}

		assert.ElementsMatch(t, expectedKeys, actualKeys)

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, true, 0, []int64{7, 12}, nil)

		// Assert 2 new recycled pages
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{1, 2, 8, 9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete remaining keys, empty root leaf left", func(t *testing.T) {
		err = anIndex.Delete(context.Background(), 12)
		require.NoError(t, err)
		err = anIndex.Delete(context.Background(), 7)
		require.NoError(t, err)
		/*
			+----------+
			|  7 , 12  |
			+----------+
		*/

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, true, 0, nil, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager.(*indexPager[int64]), []uint32{1, 2, 8, 9, 5, 6, 11, 7, 10, 3, 4})
	})
}

func TestUniqueIndex_Delete_Random_Shuffle(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	aColumn := Column{Name: "test_column", Kind: Int8, Size: 8}
	idxPager := aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size))
	anIndex := NewUniqueIndex[int64](zap.NewNop(), "test_index", aColumn, idxPager, 0)
	anIndex.maximumKeys = 3

	// Insert 100 keys in random order
	keys := make([]int64, 0, 100)
	for i := int64(1); i <= 100; i++ {
		keys = append(keys, i)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, key := range keys {
		err := anIndex.Insert(context.Background(), key, uint64(key+100))
		require.NoError(t, err)
	}

	// Verify all keys are present
	actualKeys := []int64{}
	err = anIndex.BFS(func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[int64])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, keys, actualKeys)

	// Delete all keys in random order
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for _, key := range keys {
		err := anIndex.Delete(context.Background(), key)
		require.NoError(t, err)
	}

	// Verify index is empty
	actualKeys = []int64{}
	err = anIndex.BFS(func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[int64])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)
	assert.Empty(t, actualKeys)
}

func assertFreePages(t *testing.T, aPager *indexPager[int64], expectedFreePages []uint32) {
	t.Helper()

	assert.Equal(t, len(expectedFreePages), int(aPager.dbHeader.FreePageCount))

	actualFreePages := []uint32{}
	currentFreePageID := aPager.dbHeader.FirstFreePage

	for currentFreePageID != 0 {
		actualFreePages = append(actualFreePages, currentFreePageID)
		currentFreePage := aPager.pages[currentFreePageID].FreePage
		currentFreePageID = currentFreePage.NextFreePage
	}

	if expectedFreePages == nil {
		expectedFreePages = []uint32{}
	}
	assert.Equal(t, expectedFreePages, actualFreePages)
}
