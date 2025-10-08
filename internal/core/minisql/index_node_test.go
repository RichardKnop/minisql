package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexNode_Varchar_Marshal(t *testing.T) {
	t.Parallel()

	var (
		keySize = uint64(255)
		aNode   = NewIndexNode[string](keySize)
	)

	aNode.Header = IndexNodeHeader{
		IsRoot:     true,
		Parent:     3,
		Keys:       2,
		RightChild: 4,
	}

	aNode.Cells[0].Key = "foo"
	aNode.Cells[0].RowID = 125
	aNode.Cells[0].Child = 7
	aNode.Cells[1].Key = "bar"
	aNode.Cells[1].RowID = 126
	aNode.Cells[1].Child = 8

	buf := make([]byte, aNode.Size())
	data, err := aNode.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewIndexNode[string](keySize)
	_, err = recreatedNode.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, aNode, recreatedNode)

	for idx := 0; idx < len(aNode.Cells); idx++ {
		assert.Equal(t, aNode.Cells[idx], recreatedNode.Cells[idx])
	}
}
