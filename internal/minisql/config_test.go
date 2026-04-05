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
	assert.Equal(t, []byte(DatabaseHeaderMagic), data[databaseHeaderMagicOffset:databaseHeaderVersionOffset])
	assert.Equal(t, DatabaseFileFormatVersion, unmarshalUint32(data, databaseHeaderVersionOffset))
	assert.Equal(t, uint32(PageSize), unmarshalUint32(data, databaseHeaderPageSizeOffset))

	actual := DatabaseHeader{}
	err = UnmarshalDatabaseHeader(data, &actual)
	require.NoError(t, err)
	assert.Equal(t, dbHeader, actual)
}

func TestUnmarshalDatabaseHeader_RequiresFormatMagic(t *testing.T) {
	t.Parallel()

	data := make([]byte, RootPageConfigSize)
	marshalUint32(data, 123, 0)
	marshalUint32(data, 45, 4)

	var actual DatabaseHeader
	err := UnmarshalDatabaseHeader(data, &actual)
	require.Error(t, err)
	assert.Equal(t, "invalid database header magic", err.Error())
}

func TestUnmarshalDatabaseHeader_InvalidMagic(t *testing.T) {
	t.Parallel()

	data := make([]byte, RootPageConfigSize)
	copy(data, []byte("notmini!"))
	data[databaseHeaderMetadataSize] = 1

	var actual DatabaseHeader
	err := UnmarshalDatabaseHeader(data, &actual)
	require.Error(t, err)
	assert.Equal(t, "invalid database header magic", err.Error())
}
