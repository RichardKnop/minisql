package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexNode_Int8_Unique_Marshal(t *testing.T) {
	t.Parallel()

	node := NewIndexNode[int64](true)

	// Populate with values that don't necessarily make sense, we are
	// just testing marshal/unmarshal of non zero values
	node.Header = IndexNodeHeader{
		IsRoot:     true,
		IsLeaf:     true,
		Parent:     3,
		Keys:       2,
		RightChild: 4,
	}

	node.Cells[0].Key = int64(125)
	node.Cells[0].UniqueRowID = 125
	node.Cells[0].Child = 7
	node.Cells[1].Key = int64(126)
	node.Cells[1].UniqueRowID = 126
	node.Cells[1].Child = 8
	node.freeBytes = node.MaxSpace() - node.TakenSpace()

	buf := make([]byte, node.Size())
	err := node.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewIndexNode[int64](true)
	_, err = recreatedNode.Unmarshal(nil, buf)
	require.NoError(t, err)

	assert.Equal(t, node, recreatedNode)

	for idx := 0; idx < len(node.Cells); idx++ {
		assert.Equal(t, node.Cells[idx], recreatedNode.Cells[idx])
	}
}

func TestIndexNode_Int8_Marshal(t *testing.T) {
	t.Parallel()

	node := NewIndexNode[int64](false)

	// Populate with values that don't necessarily make sense, we are
	// just testing marshal/unmarshal of non zero values
	node.Header = IndexNodeHeader{
		IsRoot:     true,
		IsLeaf:     true,
		Parent:     3,
		Keys:       2,
		RightChild: 4,
	}

	node.Cells[0].Key = int64(125)
	node.Cells[0].RowIDs = []RowID{125, 126}
	node.Cells[0].InlineRowIDs = 2
	node.Cells[0].Child = 7
	node.Cells[1].Key = int64(127)
	node.Cells[1].RowIDs = []RowID{127, 128, 129}
	node.Cells[1].InlineRowIDs = 3
	node.Cells[1].Child = 8
	node.freeBytes = node.MaxSpace() - node.TakenSpace()

	buf := make([]byte, node.Size())
	err := node.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewIndexNode[int64](false)
	_, err = recreatedNode.Unmarshal(nil, buf)
	require.NoError(t, err)

	assert.Equal(t, node, recreatedNode)

	for idx := 0; idx < len(node.Cells); idx++ {
		assert.Equal(t, node.Cells[idx], recreatedNode.Cells[idx])
	}
}

func TestIndexNode_Varchar_Unique_Marshal(t *testing.T) {
	t.Parallel()

	node := NewIndexNode[string](true)

	// Populate with values that don't necessarily make sense, we are
	// just testing marshal/unmarshal of non zero values
	node.Header = IndexNodeHeader{
		IsRoot:     true,
		IsLeaf:     true,
		Parent:     3,
		Keys:       2,
		RightChild: 4,
	}

	node.Cells[0].Key = "foo"
	node.Cells[0].UniqueRowID = 125
	node.Cells[0].Child = 7
	node.Cells[1].Key = "bar qux"
	node.Cells[1].UniqueRowID = 126
	node.Cells[1].Child = 8
	node.freeBytes = node.MaxSpace() - node.TakenSpace()

	buf := make([]byte, node.Size())
	err := node.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewIndexNode[string](true)
	_, err = recreatedNode.Unmarshal(nil, buf)
	require.NoError(t, err)

	assert.Equal(t, node, recreatedNode)

	for idx := 0; idx < len(node.Cells); idx++ {
		assert.Equal(t, node.Cells[idx], recreatedNode.Cells[idx])
	}

	expectedSize := int(15 + // header
		(varcharLengthPrefixSize + 3) + 12 + // cell 1
		(varcharLengthPrefixSize + 7) + 12) // cell 2
	assert.Equal(t, expectedSize, int(recreatedNode.Size()))
}

func TestIndexNode_Varchar_Marshal(t *testing.T) {
	t.Parallel()

	node := NewIndexNode[string](false)

	// Populate with values that don't necessarily make sense, we are
	// just testing marshal/unmarshal of non zero values
	node.Header = IndexNodeHeader{
		IsRoot:     true,
		IsLeaf:     true,
		Parent:     3,
		Keys:       2,
		RightChild: 4,
	}

	node.Cells[0].Key = "foo"
	node.Cells[0].RowIDs = []RowID{125}
	node.Cells[0].InlineRowIDs = 1
	node.Cells[0].Child = 7
	node.Cells[1].Key = "bar qux"
	node.Cells[1].RowIDs = []RowID{126, 127}
	node.Cells[1].InlineRowIDs = 2
	node.Cells[1].Child = 8
	node.freeBytes = node.MaxSpace() - node.TakenSpace()

	buf := make([]byte, node.Size())
	err := node.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewIndexNode[string](false)
	_, err = recreatedNode.Unmarshal(nil, buf)
	require.NoError(t, err)

	assert.Equal(t, node, recreatedNode)

	for idx := 0; idx < len(node.Cells); idx++ {
		assert.Equal(t, node.Cells[idx], recreatedNode.Cells[idx])
	}

	// Non-unique cell layout: key + child_ptr(4) + rowIDs_count(4) + N×rowID(8) + overflow_ptr(4)
	expectedSize := int(15 + // header
		(varcharLengthPrefixSize + 3) + 4 + rowIDsLengthPrefixSize + 1*8 + 4 + // cell 1: "foo", 1 rowID
		(varcharLengthPrefixSize + 7) + 4 + rowIDsLengthPrefixSize + 2*8 + 4) // cell 2: "bar qux", 2 rowIDs
	assert.Equal(t, expectedSize, int(recreatedNode.Size()))
}
