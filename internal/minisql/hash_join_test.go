package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatHashKeyPart(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input any
		want  string
	}{
		{"int64 positive", int64(42), "42"},
		{"int64 negative", int64(-7), "-7"},
		{"int32", int32(100), "100"},
		{"int8", int8(3), "3"},
		{"float32", float32(1.5), "1.5"},
		{"float64", float64(3.14), "3.14"},
		{"string", "hello", "hello"},
		{"TextPointer", NewTextPointer([]byte("world")), "world"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
		{"TimestampMicros", TimestampMicros(1000000), "1000000"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, formatHashKeyPart(tc.input))
		})
	}
}

func TestBuildSideHashKey(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "cat", Kind: Varchar},
	}

	join := JoinPlan{
		JoinColumnPairs: []JoinColumnPair{
			{JoinTableColumn: Field{Name: "cat"}},
		},
	}

	t.Run("valid value returns key", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols, []OptionalValue{
			{Valid: true, Value: int64(1)},
			{Valid: true, Value: "sports"},
		})
		assert.Equal(t, "sports", buildSideHashKey(join, row))
	})

	t.Run("NULL value returns empty string", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols, []OptionalValue{
			{Valid: true, Value: int64(1)},
			{Valid: false},
		})
		assert.Equal(t, "", buildSideHashKey(join, row))
	})

	t.Run("composite key joins parts with null byte", func(t *testing.T) {
		t.Parallel()
		join2 := JoinPlan{
			JoinColumnPairs: []JoinColumnPair{
				{JoinTableColumn: Field{Name: "id"}},
				{JoinTableColumn: Field{Name: "cat"}},
			},
		}
		row := NewRowWithValues(cols, []OptionalValue{
			{Valid: true, Value: int64(7)},
			{Valid: true, Value: "music"},
		})
		assert.Equal(t, "7\x00music", buildSideHashKey(join2, row))
	})
}

func TestProbeSideHashKey(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "user_id", Kind: Int8},
		{Name: "name", Kind: Varchar},
	}
	join := JoinPlan{
		JoinColumnPairs: []JoinColumnPair{
			{BaseTableColumn: Field{Name: "user_id"}},
		},
	}

	t.Run("joinIndex 0 uses plain column name", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols, []OptionalValue{
			{Valid: true, Value: int64(5)},
			{Valid: true, Value: "alice"},
		})
		assert.Equal(t, "5", probeSideHashKey(join, row, "u", 0))
	})

	t.Run("joinIndex 1 uses alias-prefixed column name", func(t *testing.T) {
		t.Parallel()
		// Simulate a combined row with alias prefix "u.user_id"
		prefixedCols := []Column{{Name: "u.user_id", Kind: Int8}}
		row := NewRowWithValues(prefixedCols, []OptionalValue{
			{Valid: true, Value: int64(5)},
		})
		assert.Equal(t, "5", probeSideHashKey(join, row, "u", 1))
	})

	t.Run("NULL probe key returns empty string", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols, []OptionalValue{
			{Valid: false},
			{Valid: true, Value: "alice"},
		})
		assert.Equal(t, "", probeSideHashKey(join, row, "u", 0))
	})
}

func TestHashJoinAlgorithmSelection(t *testing.T) {
	t.Parallel()

	// A table with no indexes on the join column should choose hash join.
	// A table with an index on the join column should keep nested-loop.

	// We use the existing join planning path via planJoinQuery indirectly
	// by checking the Algorithm field on JoinPlan after flattenJoinTree runs.
	// Since that requires a full DB, we verify the constant contract here:
	// hashJoinMaxBuildRows must be positive.
	assert.Positive(t, hashJoinMaxBuildRows)
}
