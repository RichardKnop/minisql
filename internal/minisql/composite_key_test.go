package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeKey_Marshal(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Kind: Varchar, Name: "foo", Size: 100},
		{Kind: Varchar, Name: "bar", Size: 100},
	}

	ck := NewCompositeKey(columns, "hello", "world")

	buf := make([]byte, ck.Size())
	err := ck.Marshal(buf, 0)
	require.NoError(t, err)
	// 4 = length prefix for "hello", 5 = len("hello"), 4 = length prefix for "world", 5 = len("world")
	assert.Len(t, buf, 4+5+4+5)

	recreatedCK := NewCompositeKey(columns)
	ci, err := recreatedCK.Unmarshal(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 4+5+4+5, int(ci))

	assert.Equal(t, ck.Columns, recreatedCK.Columns)
	assert.Equal(t, ck.Values, recreatedCK.Values)
	assert.Equal(t, ck.Comparison, recreatedCK.Comparison)
}
