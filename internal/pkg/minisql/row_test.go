package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRow_Marshal(t *testing.T) {
	t.Parallel()

	aRow := gen.Row()

	assert.Equal(t, uint64(8+255+4), aRow.Size())

	data, err := aRow.Marshal()
	require.NoError(t, err)

	actual := NewRow(testColumns)
	err = UnmarshalRow(data, &actual)
	require.NoError(t, err)

	assert.Equal(t, aRow, actual)
}
