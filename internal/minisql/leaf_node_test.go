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

	var (
		aNode   = NewLeafNode()
		columns = []Column{
			{
				Kind: Varchar,
				Size: 230,
			},
		}
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
		Value: prefixWithLength(bytes.Repeat([]byte{'a'}, 230)),
	}, Cell{
		Key:         2,
		NullBitmask: bitwise.Set(uint64(0), 0),
	})

	buf := make([]byte, aNode.Size())
	data, err := aNode.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := NewLeafNode()
	_, err = recreatedNode.Unmarshal(columns, data)
	require.NoError(t, err)

	assert.Equal(t, aNode, recreatedNode)

	for idx := 0; idx < len(aNode.Cells); idx++ {
		assert.Equal(t, aNode.Cells[idx], recreatedNode.Cells[idx])
	}
}

func prefixWithLength(data []byte) []byte {
	lengthPrefix := marshalUint32(make([]byte, 4), uint32(len(data)), 0)
	return append(lengthPrefix, data...)
}
