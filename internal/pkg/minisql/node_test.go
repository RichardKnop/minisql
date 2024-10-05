package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInternalNode_FindChildByKey(t *testing.T) {
	t.Parallel()

	// Root node has only 1 key so there will only be 2 children
	aRootPage, internalPages, _ := newTestBtree()

	testCases := []struct {
		Name     string
		Page     *Page
		Key      uint64
		ChildIdx uint32
	}{
		// Root page
		{
			Name:     "1 is in the left child subtree of root page",
			Page:     aRootPage,
			Key:      1,
			ChildIdx: 0,
		},
		{
			Name:     "2 is in the left child subtree of root page",
			Page:     aRootPage,
			Key:      2,
			ChildIdx: 0,
		},
		{
			Name:     "5 is in the left child subtree of root page",
			Page:     aRootPage,
			Key:      5,
			ChildIdx: 0,
		},
		{
			Name:     "12 is in the right child subtree of root page",
			Page:     aRootPage,
			Key:      12,
			ChildIdx: 1,
		},
		{
			Name:     "18 is in the right child subtree of root page",
			Page:     aRootPage,
			Key:      18,
			ChildIdx: 1,
		},
		{
			Name:     "21 is in the right child subtree of root page",
			Page:     aRootPage,
			Key:      21,
			ChildIdx: 1,
		},
		// Internal page 1
		{
			Name:     "1 is in the left child subtree of left internal page",
			Page:     internalPages[0],
			Key:      1,
			ChildIdx: 0,
		},
		{
			Name:     "2 is in the left child subtree of left internal page",
			Page:     internalPages[0],
			Key:      2,
			ChildIdx: 0,
		},
		{
			Name:     "5 is in the right child subtree of left internal page",
			Page:     internalPages[0],
			Key:      5,
			ChildIdx: 1,
		},
		// Internal page 2
		{
			Name:     "12 is in the left child subtree of right internal page",
			Page:     internalPages[1],
			Key:      12,
			ChildIdx: 0,
		},
		{
			Name:     "18 is in the left child subtree of right internal page",
			Page:     internalPages[1],
			Key:      18,
			ChildIdx: 0,
		},
		{
			Name:     "21 is in the right child subtree of right internal page",
			Page:     internalPages[1],
			Key:      21,
			ChildIdx: 1,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			childIdx := aTestCase.Page.InternalNode.FindChildByKey(aTestCase.Key)
			assert.Equal(t, int(aTestCase.ChildIdx), int(childIdx))
		})
	}
}

func TestInternalNode_Child(t *testing.T) {
	t.Parallel()

	aNode := &InternalNode{
		Header: InternalNodeHeader{
			KeysNum:    2,
			RightChild: 3,
		},
		ICells: [InternalNodeMaxCells]ICell{
			{
				Child: 1,
			},
			{
				Child: 2,
			},
		},
	}

	testCases := []struct {
		Name     string
		ChildIdx uint32
		NodeIdx  uint32
	}{
		{
			Name:     "Child with index 0 is the leftmost child node 1",
			ChildIdx: 0,
			NodeIdx:  1,
		},
		{
			Name:     "Child with index 1 is the middle child node 2",
			ChildIdx: 1,
			NodeIdx:  2,
		},
		{
			Name:     "Child with index 2 is the rightmost child node 3",
			ChildIdx: 2,
			NodeIdx:  3,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			nodeIdx, err := aNode.Child(aTestCase.ChildIdx)
			require.NoError(t, err)
			assert.Equal(t, int(aTestCase.NodeIdx), int(nodeIdx))
		})
	}
}
