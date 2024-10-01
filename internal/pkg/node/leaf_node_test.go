package node

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeafNode_Marshal(t *testing.T) {
	t.Parallel()

	var (
		numCells = uint32(17)
		rowSize  = uint64(230)
	)

	aNode := NewLeafNode(numCells, rowSize)
	aNode.CommonHeader = Header{
		IsInternal: false,
		IsRoot:     false,
		Parent:     3,
	}
	aNode.Header = LeafNodeHeader{
		Cells:    2,
		NextLeaf: 4,
	}
	aNode.Cells[0].Key = 1
	aNode.Cells[0].Value = bytes.Repeat([]byte{1}, 230)
	aNode.Cells[1].Key = 2
	aNode.Cells[1].Value = bytes.Repeat([]byte{1}, 230)

	buf := make([]byte, aNode.Size())
	data, err := aNode.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewLeafNode(numCells, rowSize)
	_, err = recreatedNode.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, aNode, recreatedNode)

	for idx := 0; idx < len(aNode.Cells); idx++ {
		assert.Equal(t, aNode.Cells[idx], recreatedNode.Cells[idx])
	}
}
