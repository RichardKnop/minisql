package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseHeader_Marshal(t *testing.T) {
	t.Parallel()

	dbHeader := DatabaseHeader{
		FirstFreePage: 1,
		FreePageCount: 2,
	}

	assert.Equal(t, 100, int(dbHeader.Size()))

	data, err := dbHeader.Marshal()
	require.NoError(t, err)

	actual := DatabaseHeader{}
	err = UnmarshalDatabaseHeader(data, &actual)
	require.NoError(t, err)
	assert.Equal(t, dbHeader, actual)
}
