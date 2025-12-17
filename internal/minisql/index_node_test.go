package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexNode_Int8_Marshal(t *testing.T) {
	t.Parallel()

	var (
		aNode = NewIndexNode[int64](true)
	)

	// Populate with values that don't necessarily make sense, we are
	// just testing marshal/unmarshal of non zero values
	aNode.Header = IndexNodeHeader{
		IsRoot:     true,
		IsLeaf:     true,
		Parent:     3,
		Keys:       2,
		RightChild: 4,
	}

	aNode.Cells[0].Key = int64(125)
	aNode.Cells[0].RowIDs = []RowID{125}
	aNode.Cells[0].Child = 7
	aNode.Cells[1].Key = int64(126)
	aNode.Cells[1].RowIDs = []RowID{126}
	aNode.Cells[1].Child = 8

	buf := make([]byte, aNode.Size())
	data, err := aNode.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewIndexNode[int64](true)
	_, err = recreatedNode.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, aNode, recreatedNode)

	for idx := 0; idx < len(aNode.Cells); idx++ {
		assert.Equal(t, aNode.Cells[idx], recreatedNode.Cells[idx])
	}
}

func TestIndexNode_Varchar_Marshal(t *testing.T) {
	t.Parallel()

	var (
		aNode = NewIndexNode[string](true)
	)

	// Populate with values that don't necessarily make sense, we are
	// just testing marshal/unmarshal of non zero values
	aNode.Header = IndexNodeHeader{
		IsRoot:     true,
		IsLeaf:     true,
		Parent:     3,
		Keys:       2,
		RightChild: 4,
	}

	aNode.Cells[0].Key = "foo"
	aNode.Cells[0].RowIDs = []RowID{125}
	aNode.Cells[0].Child = 7
	aNode.Cells[1].Key = "bar qux"
	aNode.Cells[1].RowIDs = []RowID{126}
	aNode.Cells[1].Child = 8

	buf := make([]byte, aNode.Size())
	data, err := aNode.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewIndexNode[string](true)
	_, err = recreatedNode.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, aNode, recreatedNode)

	for idx := 0; idx < len(aNode.Cells); idx++ {
		assert.Equal(t, aNode.Cells[idx], recreatedNode.Cells[idx])
	}

	expectedSize := int(15 + // header
		(varcharLengthPrefixSize + 3) + 12 + // cell 1
		(varcharLengthPrefixSize + 7) + 12) // cell 2
	assert.Equal(t, expectedSize, int(recreatedNode.Size()))
}
