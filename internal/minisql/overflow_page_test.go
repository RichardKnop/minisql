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
		err := original.Marshal(buf, 0)
		require.NoError(t, err)

		recreated := TextPointer{}
		require.NoError(t, recreated.Unmarshal(buf, 0))

		assert.Equal(t, original, recreated)
	})

	t.Run("overflow data", func(t *testing.T) {
		original := NewTextPointer(bytes.Repeat([]byte{'a'}, 1000))
		original.FirstPage = 55

		buf := make([]byte, original.Size())
		err := original.Marshal(buf, 0)
		require.NoError(t, err)

		recreated := TextPointer{}
		require.NoError(t, recreated.Unmarshal(buf, 0))

		expected := original
		expected.Data = nil // Data is not stored in overflow pointer
		assert.Equal(t, expected, recreated)
	})
}

func TestOverflowPage_Marshal(t *testing.T) {
	t.Parallel()

	t.Run("inline data", func(t *testing.T) {
		node := &OverflowPage{
			Header: OverflowPageHeader{
				NextPage: 42,
				DataSize: MaxInlineVarchar,
			},
			Data: bytes.Repeat([]byte{'a'}, MaxInlineVarchar),
		}

		buf := make([]byte, PageSize)
		err := node.Marshal(buf)
		require.NoError(t, err)

		recreatedNode := new(OverflowPage)
		err = recreatedNode.Unmarshal(buf)
		require.NoError(t, err)

		assert.Equal(t, node, recreatedNode)
	})

	t.Run("overflows to next page", func(t *testing.T) {
		node := &OverflowPage{
			Header: OverflowPageHeader{
				NextPage: 42,
				DataSize: MaxOverflowPageData,
			},
			Data: bytes.Repeat([]byte{'a'}, MaxOverflowPageData),
		}

		buf := make([]byte, PageSize)
		err := node.Marshal(buf)
		require.NoError(t, err)

		recreatedNode := new(OverflowPage)
		err = recreatedNode.Unmarshal(buf)
		require.NoError(t, err)

		assert.Equal(t, node, recreatedNode)
	})
}
