package minisql

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeRow(t *testing.T) {
	t.Parallel()

	aRow := gen.Row()

	assert.Equal(t, 267, aRow.Size())

	data, err := aRow.Marshal()
	require.NoError(t, err)

	actual := Row{
		Columns: testColumns,
	}
	err = UnmarshalRow(data, &actual)
	require.NoError(t, err)

	assert.Equal(t, aRow, actual)
}

func TestSerializeInt4(t *testing.T) {
	t.Parallel()

	value := int32(25)
	buf := make([]byte, 4)

	serializeInt4(value, buf, 0)

	out := deserializeToInt4(buf, 0)

	assert.Equal(t, value, out)
}

func TestSerializeInt8(t *testing.T) {
	t.Parallel()

	value := int64(45)
	buf := make([]byte, 8)

	serializeInt8(value, buf, 0)

	out := deserializeToInt8(buf, 0)

	assert.Equal(t, value, out)
}

func TestSerializeString(t *testing.T) {
	t.Parallel()

	value := "foobar 汉字 漢字"
	const size = unsafe.Sizeof(value)
	buf := make([]byte, size)

	serializeString(value, buf, 0)

	out := deserializeToString(buf, 0, int(size))

	assert.Equal(t, value, out)
}
