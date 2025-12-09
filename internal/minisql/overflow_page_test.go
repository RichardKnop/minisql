package minisql

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTextPointer_Marshal(t *testing.T) {
	t.Parallel()

	t.Run("inline data", func(t *testing.T) {
		original := NewTextPointer(bytes.Repeat([]byte{'a'}, 100))

		buf := make([]byte, original.Size())
		data, err := original.Marshal(buf, 0)
		require.NoError(t, err)

		recreated := TextPointer{}
		err = recreated.Unmarshal(data, 0)
		require.NoError(t, err)

		assert.Equal(t, original, recreated)
	})

	t.Run("overflow data", func(t *testing.T) {
		original := NewTextPointer(bytes.Repeat([]byte{'a'}, 1000))
		original.FirstPage = 55

		buf := make([]byte, original.Size())
		data, err := original.Marshal(buf, 0)
		require.NoError(t, err)

		recreated := TextPointer{}
		err = recreated.Unmarshal(data, 0)
		require.NoError(t, err)

		expected := original
		expected.Data = nil // Data is not stored in overflow pointer
		assert.Equal(t, expected, recreated)
	})
}

func TestOverflowPage_Marshal(t *testing.T) {
	t.Parallel()

	t.Run("inline data", func(t *testing.T) {
		aNode := &OverflowPage{
			Header: OverflowPageHeader{
				NextPage: 42,
				DataSize: MaxInlineVarchar,
			},
			Data: bytes.Repeat([]byte{'a'}, MaxInlineVarchar),
		}

		buf := make([]byte, 0, 100)
		data, err := aNode.Marshal(buf)
		require.NoError(t, err)

		recreatedNode := new(OverflowPage)
		err = recreatedNode.Unmarshal(data)
		require.NoError(t, err)

		assert.Equal(t, aNode, recreatedNode)
	})

	t.Run("overflow data", func(t *testing.T) {
		aNode := &OverflowPage{
			Header: OverflowPageHeader{
				NextPage: 42,
				DataSize: 1024,
			},
			Data: bytes.Repeat([]byte{'a'}, 1024),
		}

		buf := make([]byte, 0, 100)
		data, err := aNode.Marshal(buf)
		require.NoError(t, err)

		recreatedNode := new(OverflowPage)
		err = recreatedNode.Unmarshal(data)
		require.NoError(t, err)

		assert.Equal(t, aNode, recreatedNode)
	})
}
