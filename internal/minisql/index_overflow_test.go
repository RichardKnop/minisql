package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexOverflowPage_Marshal(t *testing.T) {
	t.Parallel()

	t.Run("inline data", func(t *testing.T) {
		aNode := &IndexOverflowPage{
			Header: IndexOverflowPageHeader{
				NextPage:  0,
				ItemCount: 5,
			},
			RowIDs: []RowID{1, 2, 3, 4, 5},
		}

		buf := make([]byte, PageSize)
		data, err := aNode.Marshal(buf)
		require.NoError(t, err)

		recreatedNode := new(IndexOverflowPage)
		err = recreatedNode.Unmarshal(data)
		require.NoError(t, err)

		assert.Equal(t, aNode, recreatedNode)
	})

	t.Run("overflows to next page", func(t *testing.T) {
		aNode := &IndexOverflowPage{
			Header: IndexOverflowPageHeader{
				NextPage:  42,
				ItemCount: MaxOverflowRowIDsPerPage,
			},
		}
		for i := range MaxOverflowRowIDsPerPage {
			aNode.RowIDs = append(aNode.RowIDs, RowID(i+1))
		}

		buf := make([]byte, PageSize)
		data, err := aNode.Marshal(buf)
		require.NoError(t, err)

		recreatedNode := new(IndexOverflowPage)
		err = recreatedNode.Unmarshal(data)
		require.NoError(t, err)

		assert.Equal(t, aNode, recreatedNode)
	})
}
