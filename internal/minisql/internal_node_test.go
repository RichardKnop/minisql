package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInternalNode_Marshal(t *testing.T) {
	t.Parallel()

	node := &InternalNode{
		Header: InternalNodeHeader{
			Header: Header{
				IsInternal: false,
				IsRoot:     true,
				Parent:     0,
			},
			KeysNum:    1,
			RightChild: 18,
		},
		ICells: [InternalNodeMaxCells]ICell{
			{
				Key:   5,
				Child: 2,
			},
		},
	}

	buf := make([]byte, node.Size())
	err := node.Marshal(buf)
	require.NoError(t, err)

	recreatedNode := new(InternalNode)
	_, err = recreatedNode.Unmarshal(buf)
	require.NoError(t, err)

	assert.Equal(t, node, recreatedNode)
}
