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

func TestDatabaseHeader_MarshalTo_BufferTooSmall(t *testing.T) {
	t.Parallel()

	h := DatabaseHeader{FirstFreePage: 1, FreePageCount: 0}
	buf := make([]byte, 4) // too small
	err := h.MarshalTo(buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too small")
}

func TestUnmarshalDatabaseHeader_BufferTooSmall(t *testing.T) {
	t.Parallel()

	var h DatabaseHeader
	err := UnmarshalDatabaseHeader(make([]byte, 4), &h)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too small")
}

func TestUnmarshalDatabaseHeader_WrongVersion(t *testing.T) {
	t.Parallel()

	// Start with a valid marshalled header, then corrupt the version field.
	orig := DatabaseHeader{FirstFreePage: 2, FreePageCount: 3}
	data, err := orig.Marshal()
	require.NoError(t, err)

	marshalUint32(data, 999, databaseHeaderVersionOffset) // unsupported version

	var h DatabaseHeader
	err = UnmarshalDatabaseHeader(data, &h)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported database file format version")
}

func TestUnmarshalDatabaseHeader_WrongPageSize(t *testing.T) {
	t.Parallel()

	orig := DatabaseHeader{}
	data, err := orig.Marshal()
	require.NoError(t, err)

	marshalUint32(data, 512, databaseHeaderPageSizeOffset) // wrong page size

	var h DatabaseHeader
	err = UnmarshalDatabaseHeader(data, &h)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported database page size")
}
