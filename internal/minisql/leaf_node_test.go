package minisql

import (
	"bytes"
	"testing"

	"github.com/RichardKnop/minisql/pkg/bitwise"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeafNode_Marshal(t *testing.T) {
	t.Parallel()

	node := NewLeafNode()

	node.Header = LeafNodeHeader{
		Header: Header{
			IsInternal: false,
			IsRoot:     false,
			Parent:     3,
		},
		Cells:    2,
		NextLeaf: 4,
	}
	node.Cells = append(node.Cells, Cell{
		Key:         1,
		Value:       prefixWithLength(bytes.Repeat([]byte{'a'}, 230)),
		TypeCodes:   []byte{byte(TypeCodeText)},
		ColumnCount: 1,
		isOwned:     true,
	}, Cell{
		Key:         2,
		NullBitmask: bitwise.Set(uint64(0), 0),
		TypeCodes:   []byte{byte(TypeCodeText)},
		ColumnCount: 1,
	})

	buf := make([]byte, node.Size())
	err := node.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewLeafNode()
	_, err = recreatedNode.Unmarshal(buf)
	require.NoError(t, err)

	assert.Equal(t, node, recreatedNode)

	for idx := 0; idx < len(node.Cells); idx++ {
		assert.Equal(t, node.Cells[idx], recreatedNode.Cells[idx])
	}
}

func prefixWithLength(data []byte) []byte {
	lengthPrefix := marshalUint32(make([]byte, 4), uint32(len(data)), 0)
	return append(lengthPrefix, data...)
}
