package minisql

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/pkg/bitwise"
)

func TestLeafNode_Marshal(t *testing.T) {
	t.Parallel()

	var (
		aNode = NewLeafNode()
	)

	aNode.Header = LeafNodeHeader{
		Header: Header{
			IsInternal: false,
			IsRoot:     false,
			Parent:     3,
		},
		Cells:    2,
		NextLeaf: 4,
	}
	aNode.Cells = append(aNode.Cells, Cell{
		Key:   1,
		Value: bytes.Repeat([]byte{1}, 230),
	}, Cell{
		Key:         2,
		Value:       bytes.Repeat([]byte{1}, 230),
		NullBitmask: bitwise.Set(uint64(0), 1),
	})

	buf := make([]byte, aNode.Size())
	data, err := aNode.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewLeafNode()
	_, err = recreatedNode.Unmarshal([]Column{
		{
			Kind: Varchar,
			Size: 230,
		},
	}, data)
	require.NoError(t, err)

	assert.Equal(t, aNode, recreatedNode)

	// for idx := 0; idx < len(aNode.Cells); idx++ {
	// 	assert.Equal(t, aNode.Cells[idx], recreatedNode.Cells[idx])
	// }
}
