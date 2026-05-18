package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReturningColumns(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 100},
		{Name: "score", Kind: Int4, Size: 4},
		{Name: "email", Kind: Varchar, Size: 200},
	}

	t.Run("specific fields in order", func(t *testing.T) {
		t.Parallel()
		got := returningColumns([]Field{{Name: "id"}, {Name: "score"}}, cols)
		require.Len(t, got, 2)
		assert.Equal(t, "id", got[0].Name)
		assert.Equal(t, "score", got[1].Name)
	})

	t.Run("star expands to all columns", func(t *testing.T) {
		t.Parallel()
		got := returningColumns([]Field{{Name: "*"}}, cols)
		assert.Equal(t, cols, got)
	})

	t.Run("unknown field returns empty slice entry", func(t *testing.T) {
		t.Parallel()
		got := returningColumns([]Field{{Name: "nonexistent"}}, cols)
		assert.Empty(t, got)
	})

	t.Run("empty fields returns empty slice", func(t *testing.T) {
		t.Parallel()
		got := returningColumns(nil, cols)
		assert.Empty(t, got)
	})

	t.Run("mix of specific and star", func(t *testing.T) {
		t.Parallel()
		got := returningColumns([]Field{{Name: "id"}, {Name: "*"}}, cols)
		require.Len(t, got, 5) // "id" + all 4 columns from "*"
		assert.Equal(t, "id", got[0].Name)
		assert.Equal(t, "id", got[1].Name)
	})
}

func TestProjectReturning(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 100},
		{Name: "score", Kind: Int4, Size: 4},
	}
	vals := []OptionalValue{
		MakeInt8(int64(42)),
		MakeVarchar(NewTextPointer([]byte("Alice"))),
		MakeInt4(int32(99)),
	}
	row := NewRowWithValues(cols, vals)

	t.Run("project single field", func(t *testing.T) {
		t.Parallel()
		got, err := projectReturning(row, []Field{{Name: "id"}})
		require.NoError(t, err)
		require.Len(t, got.Values, 1)
		assert.Equal(t, int64(42), got.Values[0].AsAny())
		assert.True(t, got.Values[0].IsValid())
	})

	t.Run("project multiple fields", func(t *testing.T) {
		t.Parallel()
		got, err := projectReturning(row, []Field{{Name: "id"}, {Name: "score"}})
		require.NoError(t, err)
		require.Len(t, got.Values, 2)
		assert.Equal(t, int64(42), got.Values[0].AsAny())
		assert.Equal(t, int32(99), got.Values[1].AsAny())
	})

	t.Run("star returns all values", func(t *testing.T) {
		t.Parallel()
		got, err := projectReturning(row, []Field{{Name: "*"}})
		require.NoError(t, err)
		assert.Equal(t, row.Values, got.Values)
		assert.Equal(t, row.Columns, got.Columns)
	})

	t.Run("unknown field returns null value", func(t *testing.T) {
		t.Parallel()
		got, err := projectReturning(row, []Field{{Name: "nonexistent"}})
		require.NoError(t, err)
		require.Len(t, got.Values, 1)
		assert.False(t, got.Values[0].IsValid())
	})

	t.Run("null value in row is preserved", func(t *testing.T) {
		t.Parallel()
		nullVals := []OptionalValue{
			MakeInt8(int64(7)),
			MakeNull(), // name is NULL
			MakeInt4(int32(0)),
		}
		nullRow := NewRowWithValues(cols, nullVals)
		got, err := projectReturning(nullRow, []Field{{Name: "name"}})
		require.NoError(t, err)
		require.Len(t, got.Values, 1)
		assert.False(t, got.Values[0].IsValid())
	})
}
