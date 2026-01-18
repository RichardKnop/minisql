package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestTable_findBestEqualityIndexMatch_WithoutStats tests the heuristic-based
// index selection when statistics are not available (ANALYZE hasn't been run)
func TestTable_findBestEqualityIndexMatch_WithoutStats(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Kind: Int4, Size: 4, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "email"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "status"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "category"},
	}

	t.Run("primary key beats unique index", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
			WithUniqueIndex(UniqueIndex{
				IndexInfo: IndexInfo{Name: "idx_email", Columns: columns[1:2]},
			}),
		)

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("test@example.com"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		assert.Equal(t, "pk_id", match.info.Name)
		assert.True(t, match.isPrimaryKey)
	})

	t.Run("unique index beats secondary index", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithUniqueIndex(UniqueIndex{
				IndexInfo: IndexInfo{Name: "idx_email", Columns: columns[1:2]},
			}), WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_status", Columns: columns[2:3]},
			}),
		)

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("test@example.com"))},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("active"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		assert.Equal(t, "idx_email", match.info.Name)
		assert.True(t, match.isUnique)
	})

	t.Run("more matched columns beats fewer", func(t *testing.T) {
		compositeColumns := []Column{
			{Kind: Int4, Size: 4, Name: "a"},
			{Kind: Int4, Size: 4, Name: "b"},
			{Kind: Int4, Size: 4, Name: "c"},
		}

		// Two secondary indexes - one matches 2 columns, one matches 1
		aTable := NewTable(
			zap.NewNop(), nil, nil, "test", compositeColumns, 0,
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_ab", Columns: compositeColumns[0:2]},
			}), WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_a", Columns: compositeColumns[0:1]},
			}),
		)

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "a"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "b"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(2)},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		assert.Equal(t, "idx_ab", match.info.Name)
		assert.Equal(t, 2, len(match.matchedConditions))
	})
}

// TestTable_findBestEqualityIndexMatch_WithStats tests the selectivity-based
// index selection when statistics are available (after ANALYZE has been run)
func TestTable_findBestEqualityIndexMatch_WithStats(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Kind: Int4, Size: 4, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "email"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "status"},
	}

	t.Run("higher selectivity wins - secondary beats primary", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_status", Columns: columns[2:3]},
			}),
		)

		// Set up stats: email has 95% selectivity, status has only 5% selectivity
		aTable.indexStats = map[string]IndexStats{
			"pk_id": {
				NEntry:    1000,
				NDistinct: []int64{50}, // 5% selectivity - low!
			},
			"idx_status": {
				NEntry:    1000,
				NDistinct: []int64{950}, // 95% selectivity - high!
			},
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("active"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Despite being a secondary index, idx_status should win due to higher selectivity
		assert.Equal(t, "idx_status", match.info.Name)
		assert.False(t, match.isPrimaryKey)
		assert.NotNil(t, match.stats)
		assert.InDelta(t, 0.95, match.stats.Selectivity(), 0.01)
	})

	t.Run("higher selectivity wins - unique beats primary", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
			WithUniqueIndex(UniqueIndex{
				IndexInfo: IndexInfo{Name: "idx_email", Columns: columns[1:2]},
			}),
		)

		// Primary key has low selectivity (many duplicates somehow)
		// Unique index has perfect selectivity
		aTable.indexStats = map[string]IndexStats{
			"pk_id": {
				NEntry:    1000,
				NDistinct: []int64{100}, // 10% selectivity
			},
			"idx_email": {
				NEntry:    1000,
				NDistinct: []int64{1000}, // 100% selectivity (unique)
			},
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("test@example.com"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		assert.Equal(t, "idx_email", match.info.Name)
		assert.True(t, match.isUnique)
		assert.NotNil(t, match.stats)
		assert.InDelta(t, 1.0, match.stats.Selectivity(), 0.01)
	})

	t.Run("same selectivity - prefer more matched columns", func(t *testing.T) {
		compositeColumns := []Column{
			{Kind: Int4, Size: 4, Name: "a"},
			{Kind: Int4, Size: 4, Name: "b"},
			{Kind: Int4, Size: 4, Name: "c"},
		}

		aTable := NewTable(
			zap.NewNop(), nil, nil, "test", compositeColumns, 0,
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_ab", Columns: compositeColumns[0:2]},
			}),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_a", Columns: compositeColumns[0:1]},
			}),
		)

		// Both have same selectivity
		aTable.indexStats = map[string]IndexStats{
			"idx_ab": {
				NEntry:    1000,
				NDistinct: []int64{100, 500}, // 50% final selectivity
			},
			"idx_a": {
				NEntry:    1000,
				NDistinct: []int64{500}, // 50% selectivity
			},
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "a"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "b"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(2)},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Same selectivity, but idx_ab matches more columns
		assert.Equal(t, "idx_ab", match.info.Name)
		assert.Equal(t, 2, len(match.matchedConditions))
	})

	t.Run("same selectivity and columns - prefer primary key", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_status", Columns: columns[2:3]},
			}),
		)

		// Identical selectivity
		aTable.indexStats = map[string]IndexStats{
			"pk_id": {
				NEntry:    1000,
				NDistinct: []int64{500}, // 50% selectivity
			},
			"idx_status": {
				NEntry:    1000,
				NDistinct: []int64{500}, // 50% selectivity
			},
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("active"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Same selectivity, prefer primary key
		assert.Equal(t, "pk_id", match.info.Name)
		assert.True(t, match.isPrimaryKey)
	})

	t.Run("same selectivity and columns - prefer unique over secondary", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithUniqueIndex(UniqueIndex{
				IndexInfo: IndexInfo{Name: "idx_email", Columns: columns[1:2]},
			}),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_status", Columns: columns[2:3]},
			}),
		)

		// Identical selectivity
		aTable.indexStats = map[string]IndexStats{
			"idx_email": {
				NEntry:    1000,
				NDistinct: []int64{500}, // 50% selectivity
			},
			"idx_status": {
				NEntry:    1000,
				NDistinct: []int64{500}, // 50% selectivity
			},
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("test@example.com"))},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("active"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Same selectivity, prefer unique index
		assert.Equal(t, "idx_email", match.info.Name)
		assert.True(t, match.isUnique)
	})
}

// TestTable_findBestEqualityIndexMatch_MixedStats tests scenarios where some
// indexes have stats and others don't
func TestTable_findBestEqualityIndexMatch_MixedStats(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Kind: Int4, Size: 4, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "email"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "status"},
	}

	t.Run("only one index has stats - falls back to heuristic comparison", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_status", Columns: columns[2:3]},
			}),
		)

		// Only one index has stats
		aTable.indexStats = map[string]IndexStats{
			"idx_status": {
				NEntry:    1000,
				NDistinct: []int64{950}, // 95% selectivity
			},
			// pk_id has no stats
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("active"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Without both having stats, fall back to heuristic: PK wins
		assert.Equal(t, "pk_id", match.info.Name)
		assert.True(t, match.isPrimaryKey)
	})

	t.Run("no indexes have stats - use heuristic", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_status", Columns: columns[2:3]},
			}),
		)

		// No stats at all
		aTable.indexStats = map[string]IndexStats{}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("active"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Fall back to heuristic: PK wins
		assert.Equal(t, "pk_id", match.info.Name)
		assert.True(t, match.isPrimaryKey)
	})
}

// TestTable_findBestEqualityIndexMatch_EdgeCases tests edge cases and boundary conditions
func TestTable_findBestEqualityIndexMatch_EdgeCases(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Kind: Int4, Size: 4, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "email"},
	}

	t.Run("no matching index returns nil", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
		)

		// Condition on non-indexed column
		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "nonexistent"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("value"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		assert.Nil(t, match)
	})

	t.Run("zero selectivity handled gracefully", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithPrimaryKey(NewPrimaryKey("pk_id", columns[0:1], false)),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_email", Columns: columns[1:2]},
			}),
		)

		// Edge case: zero entries
		aTable.indexStats = map[string]IndexStats{
			"pk_id": {
				NEntry:    0,
				NDistinct: []int64{0}, // 0/0 = NaN, should be handled
			},
			"idx_email": {
				NEntry:    1000,
				NDistinct: []int64{500}, // 50% selectivity
			},
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("test@example.com"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Should prefer the one with valid selectivity
		assert.Equal(t, "idx_email", match.info.Name)
	})

	t.Run("perfect selectivity comparison", func(t *testing.T) {
		aTable := NewTable(
			zap.NewNop(), nil, nil, "users", columns, 0,
			WithUniqueIndex(UniqueIndex{
				IndexInfo: IndexInfo{Name: "idx_email_1", Columns: columns[1:2]},
			}),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_email_2", Columns: columns[1:2]},
			}),
		)

		// Both have 100% selectivity
		aTable.indexStats = map[string]IndexStats{
			"idx_email_1": {
				NEntry:    1000,
				NDistinct: []int64{1000}, // 100% selectivity
			},
			"idx_email_2": {
				NEntry:    1000,
				NDistinct: []int64{1000}, // 100% selectivity
			},
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
				Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("test@example.com"))},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Same selectivity, prefer unique
		assert.Equal(t, "idx_email_1", match.info.Name)
		assert.True(t, match.isUnique)
	})

	t.Run("composite index with stats", func(t *testing.T) {
		compositeColumns := []Column{
			{Kind: Int4, Size: 4, Name: "category"},
			{Kind: Int4, Size: 4, Name: "status"},
			{Kind: Int4, Size: 4, Name: "priority"},
		}

		aTable := NewTable(
			zap.NewNop(), nil, nil, "tasks", compositeColumns, 0,
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_composite", Columns: compositeColumns[0:3]},
			}),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{Name: "idx_category", Columns: compositeColumns[0:1]},
			}),
		)

		// Composite has better final selectivity
		aTable.indexStats = map[string]IndexStats{
			"idx_composite": {
				NEntry:    1000,
				NDistinct: []int64{10, 50, 800}, // Final: 80% selectivity
			},
			"idx_category": {
				NEntry:    1000,
				NDistinct: []int64{10}, // 1% selectivity
			},
		}

		conditions := Conditions{
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "category"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(1)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(2)},
			},
			{
				Operator: Eq,
				Operand1: Operand{Type: OperandField, Value: Field{Name: "priority"}},
				Operand2: Operand{Type: OperandInteger, Value: int32(3)},
			},
		}

		match := aTable.findBestEqualityIndexMatch(conditions)
		require.NotNil(t, match)
		// Composite index has better selectivity (80% vs 1%)
		assert.Equal(t, "idx_composite", match.info.Name)
		assert.Equal(t, 3, len(match.matchedConditions))
	})
}
