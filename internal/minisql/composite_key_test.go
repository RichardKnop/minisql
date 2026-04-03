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

func TestCompositeKey_MarshalAllTypes(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Kind: Boolean, Name: "b", Size: 1},
		{Kind: Int4, Name: "i4", Size: 4},
		{Kind: Int8, Name: "i8", Size: 8},
		{Kind: Real, Name: "r", Size: 4},
		{Kind: Double, Name: "d", Size: 8},
		{Kind: Timestamp, Name: "ts", Size: 8},
	}

	ck := NewCompositeKey(columns, true, int32(42), int64(99), float32(1.5), float64(3.14), int64(1000000))

	// expected size in bytes: boolean(1) + int32(4) + int64(8) + float32(4) + float64(8) + timestamp(8) = 33
	assert.Equal(t, uint64(1+4+8+4+8+8), ck.Size())

	buf := make([]byte, ck.Size())
	err := ck.Marshal(buf, 0)
	require.NoError(t, err)

	recreatedCK := NewCompositeKey(columns)
	_, err = recreatedCK.Unmarshal(buf, 0)
	require.NoError(t, err)

	assert.Equal(t, ck.Values, recreatedCK.Values)
	assert.Equal(t, ck.Comparison, recreatedCK.Comparison)
}

func TestCompositeKey_Size(t *testing.T) {
	t.Parallel()

	t.Run("varchar size includes length prefix", func(t *testing.T) {
		columns := []Column{{Kind: Varchar, Name: "v", Size: 100}}
		ck := NewCompositeKey(columns, "abc")
		// 4 bytes length prefix + 3 bytes data
		assert.Equal(t, uint64(4+3), ck.Size())
	})

	t.Run("mixed fixed types", func(t *testing.T) {
		columns := []Column{
			{Kind: Boolean, Name: "b"},
			{Kind: Int4, Name: "i4"},
			{Kind: Int8, Name: "i8"},
		}
		ck := NewCompositeKey(columns, false, int32(1), int64(2))
		assert.Equal(t, uint64(1+4+8), ck.Size())
	})
}

func TestCompositeKey_Prefix(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Kind: Int8, Name: "a", Size: 8},
		{Kind: Int8, Name: "b", Size: 8},
		{Kind: Int8, Name: "c", Size: 8},
	}

	ck := NewCompositeKey(columns, int64(1), int64(2), int64(3))

	t.Run("prefix of 0 columns gives empty comparison", func(t *testing.T) {
		p := ck.Prefix(0)
		assert.Len(t, p.Comparison, 0)
	})

	t.Run("prefix of 1 column gives 8-byte comparison", func(t *testing.T) {
		p := ck.Prefix(1)
		assert.Equal(t, ck.Comparison[:8], p.Comparison)
	})

	t.Run("prefix of 2 columns gives 16-byte comparison", func(t *testing.T) {
		p := ck.Prefix(2)
		assert.Equal(t, ck.Comparison[:16], p.Comparison)
	})

	t.Run("prefix beyond length is clamped", func(t *testing.T) {
		p := ck.Prefix(100)
		assert.Equal(t, ck.Comparison, p.Comparison)
	})
}

func TestCompositeKey_GenerateComparison(t *testing.T) {
	t.Parallel()

	t.Run("varchar comparison excludes length prefix", func(t *testing.T) {
		columns := []Column{{Kind: Varchar, Name: "v", Size: 100}}
		ck := NewCompositeKey(columns, "hi")
		// Comparison for varchar is just the raw bytes, no length prefix
		assert.Equal(t, []byte("hi"), ck.Comparison)
	})

	t.Run("boolean comparison byte", func(t *testing.T) {
		columns := []Column{{Kind: Boolean, Name: "b"}}
		ckTrue := NewCompositeKey(columns, true)
		ckFalse := NewCompositeKey(columns, false)
		assert.Equal(t, []byte{1}, ckTrue.Comparison)
		assert.Equal(t, []byte{0}, ckFalse.Comparison)
	})

	t.Run("two identical keys have equal comparisons", func(t *testing.T) {
		columns := []Column{
			{Kind: Int4, Name: "x"},
			{Kind: Varchar, Name: "y", Size: 50},
		}
		ck1 := NewCompositeKey(columns, int32(7), "test")
		ck2 := NewCompositeKey(columns, int32(7), "test")
		assert.Equal(t, ck1.Comparison, ck2.Comparison)
	})
}
