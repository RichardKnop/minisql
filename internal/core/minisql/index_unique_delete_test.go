package minisql

import (
	"context"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUniqueIndex_Delete(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)

	var (
		ctx       = context.Background()
		keys      = []int64{16, 9, 5, 18, 11, 1, 14, 7, 10, 6, 20, 19, 8, 2, 13, 12, 17, 3, 4, 21, 15}
		aColumn   = Column{Name: "test_column", Kind: Int8, Size: 8}
		txManager = NewTransactionManager()
		idxPager  = NewTransactionalPager(
			aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size)),
			txManager,
		)
		anIndex = NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, idxPager, 0)
	)
	anIndex.maximumKeys = 3

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			err := anIndex.Insert(ctx, key, uint64(key+100))
			if err != nil {
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
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 1)
		}, aPager)
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
		assertFreePages(t, idxPager, nil)
	})

	t.Run("Delete another key, no pages recyclet yet", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			if err := anIndex.Delete(ctx, 4); err != nil {
				return nil
			}

			return anIndex.Delete(ctx, 5)
		}, aPager)
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
		assertFreePages(t, idxPager, nil)
	})

	t.Run("Delete another key, first recycled page", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 8)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{4})
	})

	t.Run("Delete another key, no page recycled", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 19)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{4})
	})

	t.Run("Delete another key, no page recycled", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 20)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{4})
	})

	t.Run("Delete another key, no page recycled", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 16)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{4})
	})

	t.Run("Delete another key, page recycled", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 15)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{3, 4})
	})

	t.Run("Delete another key, page recycled", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 13)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{10, 3, 4})
	})

	t.Run("Delete another key, page recycled", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 10)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{7, 10, 3, 4})
	})

	t.Run("Delete another key, no page recycled", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 9)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{7, 10, 3, 4})
	})

	t.Run("Delete another key, page recycled", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 11)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{11, 7, 10, 3, 4})
	})

	t.Run("Delete another key, 2 pages recycled, only root and leaves left", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 3)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 14)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 6)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 17)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 21)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{8, 9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 2)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{8, 9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete another key, only root leaf left", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return anIndex.Delete(ctx, 18)
		}, aPager)
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
		assertFreePages(t, idxPager, []uint32{1, 2, 8, 9, 5, 6, 11, 7, 10, 3, 4})
	})

	t.Run("Delete remaining keys, empty root leaf left", func(t *testing.T) {
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			if err := anIndex.Delete(ctx, 12); err != nil {
				return err
			}

			return anIndex.Delete(ctx, 7)
		}, aPager)
		require.NoError(t, err)

		// require.NoError(t, anIndex.print())

		var (
			rootNode = aPager.pages[0].IndexNode.(*IndexNode[int64])
		)

		// Root node
		assertIndexNode(t, rootNode, true, true, 0, nil, nil)

		// No new pages should be recycled
		assertFreePages(t, idxPager, []uint32{1, 2, 8, 9, 5, 6, 11, 7, 10, 3, 4})
	})
}

func TestUniqueIndex_Delete_Random_Shuffle(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)

	var (
		ctx       = context.Background()
		aColumn   = Column{Name: "test_column", Kind: Int8, Size: 8}
		txManager = NewTransactionManager()
		idxPager  = NewTransactionalPager(
			aPager.ForIndex(aColumn.Kind, uint64(aColumn.Size)),
			txManager,
		)
		anIndex = NewUniqueIndex[int64](testLogger, txManager, "test_index", aColumn, idxPager, 0)
	)
	anIndex.maximumKeys = 3

	// Insert 100 keys in random order
	keys := make([]int64, 0, 100)
	for i := int64(1); i <= 100; i++ {
		keys = append(keys, i)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			err := anIndex.Insert(ctx, key, uint64(key+100))
			if err != nil {
				return err
			}
		}
		return nil
	}, aPager)
	require.NoError(t, err)

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
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, key := range keys {
			if err := anIndex.Delete(ctx, key); err != nil {
				return err
			}
		}
		return nil
	}, aPager)
	require.NoError(t, err)

	// Verify index is empty
	actualKeys = []int64{}
	err = anIndex.BFS(func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[int64])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)
	assert.Empty(t, actualKeys)
}

func assertFreePages(t *testing.T, aPager Pager, expectedFreePages []uint32) {
	t.Helper()

	dbHeader := aPager.GetHeader(context.Background())

	assert.Equal(t, len(expectedFreePages), int(dbHeader.FreePageCount))

	actualFreePages := []uint32{}
	currentFreePageID := dbHeader.FirstFreePage

	for currentFreePageID != 0 {
		actualFreePages = append(actualFreePages, currentFreePageID)
		currentFreePage, err := aPager.GetPage(context.Background(), currentFreePageID)
		require.NoError(t, err)
		currentFreePageID = currentFreePage.FreePage.NextFreePage
	}

	if expectedFreePages == nil {
		expectedFreePages = []uint32{}
	}
	assert.Equal(t, expectedFreePages, actualFreePages)
}
