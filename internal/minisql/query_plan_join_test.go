package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mapTableProvider is a test TableProvider backed by a simple name→table map.
type mapTableProvider struct {
	tables map[string]*Table
}

func (m *mapTableProvider) GetTable(_ context.Context, name string) (*Table, bool) {
	t, ok := m.tables[name]
	return t, ok
}

// stubTable builds a minimal *Table with a given name and row-count estimate for
// use in collectJoinGraph / greedyJoinOrder unit tests. No pager is needed
// because collectJoinGraph only calls estimatedRowCount() and provider.GetTable.
func stubTable(name string, rows int64) *Table {
	tbl := &Table{
		Name:             name,
		UniqueIndexes:    map[string]UniqueIndex{},
		SecondaryIndexes: map[string]SecondaryIndex{},
	}
	fixedCount := rows
	tbl.getRowCount = func() int64 { return fixedCount }
	return tbl
}

func TestPushDownFilters(t *testing.T) {
	t.Parallel()

	t.Run("Single condition group with base table filters", func(t *testing.T) {
		// WHERE u.dob > '1999-01-01' AND u.verified = true
		conditions := OneOrMore{
			{
				FieldIsGreater(Field{AliasPrefix: "u", Name: "dob"}, OperandQuotedString, NewTextPointer([]byte("1999-01-01"))),
				FieldIsEqual(Field{AliasPrefix: "u", Name: "verified"}, OperandBoolean, true),
			},
		}

		joins := []Join{
			{TableName: "profiles", TableAlias: "p"},
		}

		baseFilters, joinFilters := pushDownFilters(conditions, "u", joins)

		// Both conditions should stay in base filters as a single AND group
		assert.Len(t, baseFilters, 1, "Should have 1 condition group")
		assert.Len(t, baseFilters[0], 2, "Group should have 2 conditions")
		assert.Len(t, joinFilters["p"], 0, "Join table should have no filters")
	})

	t.Run("Multiple OR-separated condition groups - base table only", func(t *testing.T) {
		// WHERE (u.dob > '1999-01-01' AND u.verified = true) OR (u.role = 'admin')
		conditions := OneOrMore{
			{
				FieldIsGreater(Field{AliasPrefix: "u", Name: "dob"}, OperandQuotedString, NewTextPointer([]byte("1999-01-01"))),
				FieldIsEqual(Field{AliasPrefix: "u", Name: "verified"}, OperandBoolean, true),
			},
			{
				FieldIsEqual(Field{AliasPrefix: "u", Name: "role"}, OperandQuotedString, NewTextPointer([]byte("admin"))),
			},
		}

		joins := []Join{
			{TableName: "profiles", TableAlias: "p"},
		}

		baseFilters, joinFilters := pushDownFilters(conditions, "u", joins)

		// Should preserve the OR structure: 2 groups
		assert.Len(t, baseFilters, 2, "Should have 2 condition groups (OR-separated)")
		assert.Len(t, baseFilters[0], 2, "First group should have 2 conditions (AND-separated)")
		assert.Len(t, baseFilters[1], 1, "Second group should have 1 condition")
		assert.Len(t, joinFilters["p"], 0, "Join table should have no filters")
	})

	t.Run("Conditions split between base and join tables", func(t *testing.T) {
		// WHERE u.verified = true AND p.status = 'active'
		conditions := OneOrMore{
			{
				FieldIsEqual(Field{AliasPrefix: "u", Name: "verified"}, OperandBoolean, true),
				FieldIsEqual(Field{AliasPrefix: "p", Name: "status"}, OperandQuotedString, NewTextPointer([]byte("active"))),
			},
		}

		joins := []Join{
			{TableName: "profiles", TableAlias: "p"},
		}

		baseFilters, joinFilters := pushDownFilters(conditions, "u", joins)

		// Conditions should be split between tables but maintain single group structure
		assert.Len(t, baseFilters, 1, "Should have 1 condition group")
		assert.Len(t, baseFilters[0], 1, "Base table group should have 1 condition")
		assert.Len(t, joinFilters["p"], 1, "Join table should have 1 condition group")
		assert.Len(t, joinFilters["p"][0], 1, "Join table group should have 1 condition")
	})

	t.Run("Multiple OR groups with conditions for different tables", func(t *testing.T) {
		// WHERE (u.verified = true AND p.status = 'active') OR (u.role = 'admin')
		conditions := OneOrMore{
			{
				FieldIsEqual(Field{AliasPrefix: "u", Name: "verified"}, OperandBoolean, true),
				FieldIsEqual(Field{AliasPrefix: "p", Name: "status"}, OperandQuotedString, NewTextPointer([]byte("active"))),
			},
			{
				FieldIsEqual(Field{AliasPrefix: "u", Name: "role"}, OperandQuotedString, NewTextPointer([]byte("admin"))),
			},
		}

		joins := []Join{
			{TableName: "profiles", TableAlias: "p"},
		}

		baseFilters, _ := pushDownFilters(conditions, "u", joins)

		// This is complex: First OR group has conditions from both tables
		// In this case, we need to be careful about how we split
		// For simplicity, conditions from different tables in same OR group should stay together
		// But the current implementation would split them incorrectly

		// Expected behavior: preserve OR structure
		// Group 1: u.verified = true AND p.status = 'active'
		// Group 2: u.role = 'admin'
		assert.Len(t, baseFilters, 2, "Should have 2 condition groups")
	})

	t.Run("No alias prefix - defaults to base table", func(t *testing.T) {
		// WHERE verified = true
		conditions := OneOrMore{
			{
				FieldIsEqual(Field{Name: "verified"}, OperandBoolean, true),
			},
		}

		joins := []Join{
			{TableName: "profiles", TableAlias: "p"},
		}

		baseFilters, joinFilters := pushDownFilters(conditions, "u", joins)

		assert.Len(t, baseFilters, 1, "Should have 1 condition group")
		assert.Len(t, baseFilters[0], 1, "Group should have 1 condition")
		assert.Len(t, joinFilters["p"], 0, "Join table should have no filters")
	})

	t.Run("Multiple join tables with conditions", func(t *testing.T) {
		// WHERE u.verified = true AND p.status = 'active' AND c.type = 'premium'
		conditions := OneOrMore{
			{
				FieldIsEqual(Field{AliasPrefix: "u", Name: "verified"}, OperandBoolean, true),
				FieldIsEqual(Field{AliasPrefix: "p", Name: "status"}, OperandQuotedString, NewTextPointer([]byte("active"))),
				FieldIsEqual(Field{AliasPrefix: "c", Name: "type"}, OperandQuotedString, NewTextPointer([]byte("premium"))),
			},
		}

		joins := []Join{
			{TableName: "profiles", TableAlias: "p"},
			{TableName: "companies", TableAlias: "c"},
		}

		baseFilters, joinFilters := pushDownFilters(conditions, "u", joins)

		assert.Len(t, baseFilters, 1, "Should have 1 condition group")
		assert.Len(t, baseFilters[0], 1, "Base table group should have 1 condition")
		assert.Len(t, joinFilters["p"], 1, "Profile table should have 1 condition group")
		assert.Len(t, joinFilters["p"][0], 1, "Profile table group should have 1 condition")
		assert.Len(t, joinFilters["c"], 1, "Company table should have 1 condition group")
		assert.Len(t, joinFilters["c"][0], 1, "Company table group should have 1 condition")
	})

	t.Run("User's example: complex OR with mixed table conditions", func(t *testing.T) {
		// SELECT * FROM users AS u
		// INNER JOIN profiles AS p ON u.id = p.user_id
		// WHERE (u.dob > '1999-01-01 00:00:00' AND verified = true) OR (role = 'admin')
		//
		// This should result in:
		// - baseFilters with 2 OR-separated groups:
		//   Group 1: [u.dob > '1999-01-01', verified = true]
		//   Group 2: [role = 'admin']
		conditions := OneOrMore{
			{
				FieldIsGreater(Field{AliasPrefix: "u", Name: "dob"}, OperandQuotedString, NewTextPointer([]byte("1999-01-01 00:00:00"))),
				FieldIsEqual(Field{Name: "verified"}, OperandBoolean, true), // No alias prefix - defaults to base
			},
			{
				FieldIsEqual(Field{Name: "role"}, OperandQuotedString, NewTextPointer([]byte("admin"))), // No alias prefix - defaults to base
			},
		}

		joins := []Join{
			{TableName: "profiles", TableAlias: "p"},
		}

		baseFilters, joinFilters := pushDownFilters(conditions, "u", joins)

		// Critical assertions: should preserve the OR structure
		assert.Len(t, baseFilters, 2, "Should have exactly 2 condition groups (OR-separated), not 3")
		assert.Len(t, baseFilters[0], 2, "First OR group should have 2 AND-separated conditions")
		assert.Len(t, baseFilters[1], 1, "Second OR group should have 1 condition")

		// Verify the actual conditions
		assert.Equal(t, "dob", baseFilters[0][0].Operand1.Value.(Field).Name)
		assert.Equal(t, "verified", baseFilters[0][1].Operand1.Value.(Field).Name)
		assert.Equal(t, "role", baseFilters[1][0].Operand1.Value.(Field).Name)

		// Join table should have no filters in this case
		assert.Len(t, joinFilters["p"], 0, "Join table should have no filters")
	})
}

func TestExtractJoinColumnPairs(t *testing.T) {
	t.Parallel()

	// Create mock tables for validation
	baseTable := &Table{
		Name: "table_a",
		Columns: []Column{
			{Name: "id", Kind: Int4, Size: 4},
			{Name: "other_id", Kind: Int4, Size: 4},
			{Name: "name", Kind: Varchar, Size: 255},
		},
		columnCache: map[string]int{
			"id":       0,
			"other_id": 1,
			"name":     2,
		},
	}

	joinTable := &Table{
		Name: "table_b",
		Columns: []Column{
			{Name: "id", Kind: Int4, Size: 4},
			{Name: "a_id", Kind: Int4, Size: 4},
			{Name: "other_id", Kind: Int4, Size: 4},
			{Name: "value", Kind: Varchar, Size: 255},
		},
		columnCache: map[string]int{
			"id":       0,
			"a_id":     1,
			"other_id": 2,
			"value":    3,
		},
	}

	t.Run("Single join condition", func(t *testing.T) {
		// ON b.a_id = a.id
		conditions := Conditions{
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "a_id"},
				OperandField,
				Field{AliasPrefix: "a", Name: "id"},
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.NoError(t, err)
		assert.Len(t, pairs, 1, "Should extract one column pair")
		assert.Equal(t, "id", pairs[0].BaseTableColumn.Name)
		assert.Equal(t, "a", pairs[0].BaseTableColumn.AliasPrefix)
		assert.Equal(t, "a_id", pairs[0].JoinTableColumn.Name)
		assert.Equal(t, "b", pairs[0].JoinTableColumn.AliasPrefix)
	})

	t.Run("Multiple join conditions (composite key)", func(t *testing.T) {
		// ON b.a_id = a.id AND b.other_id = a.other_id
		conditions := Conditions{
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "a_id"},
				OperandField,
				Field{AliasPrefix: "a", Name: "id"},
			),
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "other_id"},
				OperandField,
				Field{AliasPrefix: "a", Name: "other_id"},
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.NoError(t, err)
		assert.Len(t, pairs, 2, "Should extract two column pairs")

		// First pair
		assert.Equal(t, "id", pairs[0].BaseTableColumn.Name)
		assert.Equal(t, "a", pairs[0].BaseTableColumn.AliasPrefix)
		assert.Equal(t, "a_id", pairs[0].JoinTableColumn.Name)
		assert.Equal(t, "b", pairs[0].JoinTableColumn.AliasPrefix)

		// Second pair
		assert.Equal(t, "other_id", pairs[1].BaseTableColumn.Name)
		assert.Equal(t, "a", pairs[1].BaseTableColumn.AliasPrefix)
		assert.Equal(t, "other_id", pairs[1].JoinTableColumn.Name)
		assert.Equal(t, "b", pairs[1].JoinTableColumn.AliasPrefix)
	})

	t.Run("Reversed field order - should handle correctly", func(t *testing.T) {
		// ON a.id = b.a_id (reversed from typical order)
		conditions := Conditions{
			FieldIsEqual(
				Field{AliasPrefix: "a", Name: "id"},
				OperandField,
				Field{AliasPrefix: "b", Name: "a_id"},
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.NoError(t, err)
		assert.Len(t, pairs, 1, "Should extract one column pair")
		assert.Equal(t, "id", pairs[0].BaseTableColumn.Name)
		assert.Equal(t, "a_id", pairs[0].JoinTableColumn.Name)
	})

	t.Run("Mixed valid and invalid conditions", func(t *testing.T) {
		// ON b.a_id = a.id AND b.value > 'test' AND b.other_id = a.other_id
		// Should extract only the equality conditions with fields on both sides
		conditions := Conditions{
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "a_id"},
				OperandField,
				Field{AliasPrefix: "a", Name: "id"},
			),
			FieldIsGreater(
				Field{AliasPrefix: "b", Name: "value"},
				OperandQuotedString,
				NewTextPointer([]byte("test")),
			),
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "other_id"},
				OperandField,
				Field{AliasPrefix: "a", Name: "other_id"},
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.NoError(t, err)
		assert.Len(t, pairs, 2, "Should extract only the two valid join conditions")
		assert.Equal(t, "id", pairs[0].BaseTableColumn.Name)
		assert.Equal(t, "a_id", pairs[0].JoinTableColumn.Name)
		assert.Equal(t, "other_id", pairs[1].BaseTableColumn.Name)
		assert.Equal(t, "other_id", pairs[1].JoinTableColumn.Name)
	})

	t.Run("Error - no valid join conditions", func(t *testing.T) {
		// Only non-equality or non-field conditions
		conditions := Conditions{
			FieldIsGreater(
				Field{AliasPrefix: "b", Name: "value"},
				OperandQuotedString,
				NewTextPointer([]byte("test")),
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not extract valid join columns")
		assert.Nil(t, pairs)
	})

	t.Run("Error - base table column does not exist", func(t *testing.T) {
		// ON b.a_id = a.nonexistent_column
		conditions := Conditions{
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "a_id"},
				OperandField,
				Field{AliasPrefix: "a", Name: "nonexistent_column"},
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column nonexistent_column does not exist in base table")
		assert.Nil(t, pairs)
	})

	t.Run("Error - join table column does not exist", func(t *testing.T) {
		// ON b.nonexistent_column = a.id
		conditions := Conditions{
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "nonexistent_column"},
				OperandField,
				Field{AliasPrefix: "a", Name: "id"},
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column nonexistent_column does not exist in join table")
		assert.Nil(t, pairs)
	})

	t.Run("Ignore conditions between wrong tables", func(t *testing.T) {
		// ON b.a_id = c.id (c is not part of this join)
		// Should return error as no valid conditions found
		conditions := Conditions{
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "a_id"},
				OperandField,
				Field{AliasPrefix: "c", Name: "id"},
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not extract valid join columns")
		assert.Nil(t, pairs)
	})

	t.Run("Empty conditions", func(t *testing.T) {
		conditions := Conditions{}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not extract valid join columns")
		assert.Nil(t, pairs)
	})

	t.Run("Multiple conditions with one invalid column", func(t *testing.T) {
		// ON b.a_id = a.id AND b.invalid = a.other_id
		// Should fail validation on the second condition
		conditions := Conditions{
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "a_id"},
				OperandField,
				Field{AliasPrefix: "a", Name: "id"},
			),
			FieldIsEqual(
				Field{AliasPrefix: "b", Name: "invalid"},
				OperandField,
				Field{AliasPrefix: "a", Name: "other_id"},
			),
		}

		pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column invalid does not exist in join table")
		assert.Nil(t, pairs)
	})
}

func TestNullRowForColumns(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 50},
		{Name: "age", Kind: Int4, Size: 4},
	}

	row := nullRowForColumns(cols)

	require.Len(t, row.Values, len(cols))
	for i, v := range row.Values {
		assert.False(t, v.Valid, "column %d (%s) should be NULL", i, cols[i].Name)
	}
	assert.Len(t, row.Columns, len(cols))
}

func TestCombineRows(t *testing.T) {
	t.Parallel()

	outerCols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 50},
	}
	innerCols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "amount", Kind: Int8, Size: 8},
	}

	outerRow := NewRowWithValues(outerCols, []OptionalValue{
		{Value: int64(1), Valid: true},
		{Value: NewTextPointer([]byte("Alice")), Valid: true},
	})
	innerRow := NewRowWithValues(innerCols, []OptionalValue{
		{Value: int64(10), Valid: true},
		{Value: int64(200), Valid: true},
	})

	combined := combineRows(outerRow, innerRow, "u", "o")

	require.Len(t, combined.Columns, 4)
	assert.Equal(t, "u.id", combined.Columns[0].Name)
	assert.Equal(t, "u.name", combined.Columns[1].Name)
	assert.Equal(t, "o.id", combined.Columns[2].Name)
	assert.Equal(t, "o.amount", combined.Columns[3].Name)

	// Values are accessible by prefixed name
	v, ok := combined.GetValue("u.id")
	require.True(t, ok)
	assert.Equal(t, int64(1), v.Value)

	v, ok = combined.GetValue("o.amount")
	require.True(t, ok)
	assert.Equal(t, int64(200), v.Value)
}

func TestCombineRows_NullInner(t *testing.T) {
	t.Parallel()

	outerCols := []Column{{Name: "id", Kind: Int8, Size: 8}}
	innerCols := []Column{{Name: "user_id", Kind: Int8, Size: 8}}

	outer := NewRowWithValues(outerCols, []OptionalValue{{Value: int64(1), Valid: true}})
	nullInner := nullRowForColumns(innerCols)

	combined := combineRows(outer, nullInner, "u", "o")

	require.Len(t, combined.Columns, 2)
	v, ok := combined.GetValue("o.user_id")
	require.True(t, ok)
	assert.False(t, v.Valid, "inner column should be NULL")
}

func TestCombineRowsProgressive(t *testing.T) {
	t.Parallel()

	// Simulate a row already combined from base + first join
	existingCols := []Column{
		{Name: "u.id", Kind: Int8, Size: 8},
		{Name: "o.amount", Kind: Int8, Size: 8},
	}
	existingRow := NewRowWithValues(existingCols, []OptionalValue{
		{Value: int64(1), Valid: true},
		{Value: int64(100), Valid: true},
	})

	newCols := []Column{{Name: "status", Kind: Varchar, Size: 20}}
	newRow := NewRowWithValues(newCols, []OptionalValue{
		{Value: NewTextPointer([]byte("shipped")), Valid: true},
	})

	combined := combineRowsProgressive(existingRow, newRow, "s")

	require.Len(t, combined.Columns, 3)
	assert.Equal(t, "u.id", combined.Columns[0].Name)
	assert.Equal(t, "o.amount", combined.Columns[1].Name)
	assert.Equal(t, "s.status", combined.Columns[2].Name)

	v, ok := combined.GetValue("s.status")
	require.True(t, ok)
	assert.True(t, v.Valid)
}

func TestBuildCombinedColumns(t *testing.T) {
	t.Parallel()

	outerCols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 50},
	}
	innerCols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "amount", Kind: Int8, Size: 8},
	}

	t.Run("with aliases", func(t *testing.T) {
		cols := buildCombinedColumns(outerCols, "u", innerCols, "o")
		require.Len(t, cols, 4)
		assert.Equal(t, "u.id", cols[0].Name)
		assert.Equal(t, "u.name", cols[1].Name)
		assert.Equal(t, "o.id", cols[2].Name)
		assert.Equal(t, "o.amount", cols[3].Name)
		assert.Equal(t, Int8, cols[0].Kind)
		assert.Equal(t, Varchar, cols[1].Kind)
	})

	t.Run("outer alias empty", func(t *testing.T) {
		cols := buildCombinedColumns(outerCols, "", innerCols, "o")
		require.Len(t, cols, 4)
		// outer columns keep original names when alias is empty
		assert.Equal(t, "id", cols[0].Name)
		assert.Equal(t, "name", cols[1].Name)
		assert.Equal(t, "o.id", cols[2].Name)
		assert.Equal(t, "o.amount", cols[3].Name)
	})
}

func TestBuildCombinedColumnsProgressive(t *testing.T) {
	t.Parallel()

	existingCols := []Column{
		{Name: "u.id", Kind: Int8, Size: 8},
		{Name: "o.amount", Kind: Int8, Size: 8},
	}
	innerCols := []Column{
		{Name: "code", Kind: Varchar, Size: 10},
	}

	cols := buildCombinedColumnsProgressive(existingCols, innerCols, "s")

	require.Len(t, cols, 3)
	assert.Equal(t, "u.id", cols[0].Name)
	assert.Equal(t, "o.amount", cols[1].Name)
	assert.Equal(t, "s.code", cols[2].Name)
	assert.Equal(t, Int8, cols[0].Kind)
	assert.Equal(t, Varchar, cols[2].Kind)
}

func TestCombineRowsWithSchema(t *testing.T) {
	t.Parallel()

	outerCols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 50},
	}
	innerCols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "amount", Kind: Int8, Size: 8},
	}
	combinedCols := buildCombinedColumns(outerCols, "u", innerCols, "o")

	outerRow := NewRowWithValues(outerCols, []OptionalValue{
		{Value: int64(1), Valid: true},
		{Value: NewTextPointer([]byte("Alice")), Valid: true},
	})
	innerRow := NewRowWithValues(innerCols, []OptionalValue{
		{Value: int64(10), Valid: true},
		{Value: int64(200), Valid: true},
	})

	combined := combineRowsWithSchema(outerRow, innerRow, combinedCols)

	require.Len(t, combined.Columns, 4)
	assert.Equal(t, "u.id", combined.Columns[0].Name)
	assert.Equal(t, "o.amount", combined.Columns[3].Name)

	v, ok := combined.GetValue("u.id")
	require.True(t, ok)
	assert.Equal(t, int64(1), v.Value)

	v, ok = combined.GetValue("o.amount")
	require.True(t, ok)
	assert.Equal(t, int64(200), v.Value)
}

func TestCombineRowWithNullInner(t *testing.T) {
	t.Parallel()

	outerCols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
	}
	innerCols := []Column{
		{Name: "user_id", Kind: Int8, Size: 8},
		{Name: "total", Kind: Int8, Size: 8},
	}
	combinedCols := buildCombinedColumns(outerCols, "u", innerCols, "o")

	outer := NewRowWithValues(outerCols, []OptionalValue{
		{Value: int64(42), Valid: true},
	})

	combined := combineRowWithNullInner(outer, len(innerCols), combinedCols)

	require.Len(t, combined.Columns, 3)
	require.Len(t, combined.Values, 3)

	// Outer value is preserved.
	v, ok := combined.GetValue("u.id")
	require.True(t, ok)
	assert.Equal(t, int64(42), v.Value)
	assert.True(t, v.Valid)

	// Inner values are zero-value (NULL).
	v, ok = combined.GetValue("o.user_id")
	require.True(t, ok)
	assert.False(t, v.Valid)

	v, ok = combined.GetValue("o.total")
	require.True(t, ok)
	assert.False(t, v.Valid)
}

func TestCompileJoinConditions(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "u.id", Kind: Int8, Size: 8},
		{Name: "o.user_id", Kind: Int8, Size: 8},
	}
	row := NewRowWithValues(cols, []OptionalValue{
		{Value: int64(42), Valid: true},
		{Value: int64(42), Valid: true},
	})

	t.Run("no conditions always matches", func(t *testing.T) {
		filter := compileRowFilterForColumns(row.Columns, OneOrMore{})
		assert.Nil(t, filter)
	})

	t.Run("matching equality condition", func(t *testing.T) {
		cond := FieldIsEqual(
			Field{AliasPrefix: "u", Name: "id"},
			OperandField,
			Field{AliasPrefix: "o", Name: "user_id"},
		)
		filter := compileRowFilterForColumns(row.Columns, OneOrMore{{cond}})
		require.NotNil(t, filter)
		ok, err := filter(row)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("non-matching equality condition", func(t *testing.T) {
		mismatchCols := []Column{
			{Name: "u.id", Kind: Int8, Size: 8},
			{Name: "o.user_id", Kind: Int8, Size: 8},
		}
		mismatchRow := NewRowWithValues(mismatchCols, []OptionalValue{
			{Value: int64(1), Valid: true},
			{Value: int64(99), Valid: true},
		})
		cond := FieldIsEqual(
			Field{AliasPrefix: "u", Name: "id"},
			OperandField,
			Field{AliasPrefix: "o", Name: "user_id"},
		)
		filter := compileRowFilterForColumns(mismatchRow.Columns, OneOrMore{{cond}})
		require.NotNil(t, filter)
		ok, err := filter(mismatchRow)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestFindIndexOnColumns(t *testing.T) {
	t.Parallel()

	tbl := &Table{
		PrimaryKey: PrimaryKey{
			IndexInfo: IndexInfo{
				Name:    "pk_users",
				Columns: []Column{{Name: "id", Kind: Int8, Size: 8}},
			},
		},
		UniqueIndexes: map[string]UniqueIndex{
			"uq_email": {
				IndexInfo: IndexInfo{
					Columns: []Column{{Name: "email", Kind: Varchar, Size: 255}},
				},
			},
		},
		SecondaryIndexes: map[string]SecondaryIndex{
			"idx_created": {
				IndexInfo: IndexInfo{
					Columns: []Column{{Name: "created", Kind: Timestamp, Size: 8}},
				},
			},
		},
	}

	t.Run("finds primary key index", func(t *testing.T) {
		info := tbl.findIndexOnColumns([]string{"id"})
		require.NotNil(t, info)
		assert.Equal(t, "pk_users", info.Name)
	})

	t.Run("finds unique index", func(t *testing.T) {
		info := tbl.findIndexOnColumns([]string{"email"})
		require.NotNil(t, info)
		assert.Equal(t, "uq_email", info.Name)
	})

	t.Run("finds secondary index", func(t *testing.T) {
		info := tbl.findIndexOnColumns([]string{"created"})
		require.NotNil(t, info)
		assert.Equal(t, "idx_created", info.Name)
	})

	t.Run("returns nil for unknown column", func(t *testing.T) {
		info := tbl.findIndexOnColumns([]string{"nonexistent"})
		assert.Nil(t, info)
	})

	t.Run("returns nil for empty input", func(t *testing.T) {
		info := tbl.findIndexOnColumns(nil)
		assert.Nil(t, info)
	})
}

func TestGreedyJoinOrder_StarSchema(t *testing.T) {
	t.Parallel()

	// Three tables: A (1000 rows), B (10 rows), C (500 rows).
	// Greedy should start with B, then pick C before A.
	nodes := []joinGraphNode{
		{tableAlias: "a", rows: 1000},
		{tableAlias: "b", rows: 10},
		{tableAlias: "c", rows: 500},
	}
	// Star schema: all join to "a".
	edges := []joinGraphEdge{
		{alias1: "a", alias2: "b", joinType: Inner},
		{alias1: "a", alias2: "c", joinType: Inner},
	}

	orderedNodes, orderedEdges, ok := greedyJoinOrder(nodes, edges)
	require.True(t, ok)
	require.Len(t, orderedNodes, 3)
	require.Len(t, orderedEdges, 2)

	// B is smallest → first.
	assert.Equal(t, "b", orderedNodes[0].tableAlias)
	// Next reachable from B via edges: A (via edge a-b). C is NOT reachable from B directly
	// (edge a-c connects a and c, neither of which is done). So A must come before C.
	assert.Equal(t, "a", orderedNodes[1].tableAlias)
	// Once A is done, C becomes reachable.
	assert.Equal(t, "c", orderedNodes[2].tableAlias)
}

func TestGreedyJoinOrder_ChainSchema(t *testing.T) {
	t.Parallel()

	// Chain: A → B → C, row counts A=1000, B=5, C=200.
	// Connectivity: edge a-b and edge b-c.
	// Greedy: start B (5), then A (1000, reachable via a-b) or C (200, reachable via b-c).
	// C (200) < A (1000) → pick C next, then A.
	nodes := []joinGraphNode{
		{tableAlias: "a", rows: 1000},
		{tableAlias: "b", rows: 5},
		{tableAlias: "c", rows: 200},
	}
	edges := []joinGraphEdge{
		{alias1: "a", alias2: "b", joinType: Inner},
		{alias1: "b", alias2: "c", joinType: Inner},
	}

	orderedNodes, orderedEdges, ok := greedyJoinOrder(nodes, edges)
	require.True(t, ok)
	require.Len(t, orderedNodes, 3)
	require.Len(t, orderedEdges, 2)

	assert.Equal(t, "b", orderedNodes[0].tableAlias)
	assert.Equal(t, "c", orderedNodes[1].tableAlias)
	assert.Equal(t, "a", orderedNodes[2].tableAlias)
}

func TestGreedyJoinOrder_FallbackOnOuterJoin(t *testing.T) {
	t.Parallel()

	nodes := []joinGraphNode{
		{tableAlias: "a", rows: 100},
		{tableAlias: "b", rows: 5},
	}
	edges := []joinGraphEdge{
		{alias1: "a", alias2: "b", joinType: Left}, // non-INNER → ineligible
	}

	_, _, ok := greedyJoinOrder(nodes, edges)
	assert.False(t, ok, "should fall back when any join is not INNER")
}

func TestGreedyJoinOrder_FallbackOnUnknownRowCount(t *testing.T) {
	t.Parallel()

	nodes := []joinGraphNode{
		{tableAlias: "a", rows: -1}, // unknown
		{tableAlias: "b", rows: 50},
	}
	edges := []joinGraphEdge{
		{alias1: "a", alias2: "b", joinType: Inner},
	}

	_, _, ok := greedyJoinOrder(nodes, edges)
	assert.False(t, ok, "should fall back when any table has unknown row count")
}

func TestGreedyJoinOrder_IndexPreference(t *testing.T) {
	t.Parallel()

	// emp (10K rows, no index on join col) + dept (100 rows, index on join col).
	// Greedy should start with emp (large, no index) so that dept becomes the
	// inner (INLJ target).  The old row-count-only heuristic would have picked
	// dept (smallest) as base, putting emp on the build side — wrong for hash join.
	nodes := []joinGraphNode{
		{tableAlias: "emp", rows: 10000, indexPartners: nil},
		{tableAlias: "dept", rows: 100, indexPartners: map[string]bool{"emp": true}},
	}
	edges := []joinGraphEdge{
		{alias1: "emp", alias2: "dept", joinType: Inner},
	}

	orderedNodes, _, ok := greedyJoinOrder(nodes, edges)
	require.True(t, ok)
	require.Len(t, orderedNodes, 2)
	assert.Equal(t, "emp", orderedNodes[0].tableAlias, "large no-index table should be base (probe)")
	assert.Equal(t, "dept", orderedNodes[1].tableAlias, "indexed table should be inner (INLJ target)")
}

func TestGreedyJoinOrder_IndexPreferenceNextNode(t *testing.T) {
	t.Parallel()

	// Three tables: A (1000 rows, no index), B (500 rows, index on join col with A),
	// C (200 rows, no index).
	// A should start (no index, most rows among non-indexed nodes).
	// Then B should be picked next (has index with A) before C (no index).
	nodes := []joinGraphNode{
		{tableAlias: "a", rows: 1000},
		{tableAlias: "b", rows: 500, indexPartners: map[string]bool{"a": true}},
		{tableAlias: "c", rows: 200},
	}
	edges := []joinGraphEdge{
		{alias1: "a", alias2: "b", joinType: Inner},
		{alias1: "a", alias2: "c", joinType: Inner},
	}

	orderedNodes, _, ok := greedyJoinOrder(nodes, edges)
	require.True(t, ok)
	require.Len(t, orderedNodes, 3)
	assert.Equal(t, "a", orderedNodes[0].tableAlias)
	assert.Equal(t, "b", orderedNodes[1].tableAlias, "index-eligible node should be preferred as inner")
	assert.Equal(t, "c", orderedNodes[2].tableAlias)
}

func TestGreedyJoinOrder_UserOrderPreservedWhenAlreadyOptimal(t *testing.T) {
	t.Parallel()

	// A (10 rows) is already smallest — greedy produces same order as user.
	nodes := []joinGraphNode{
		{tableAlias: "a", rows: 10},
		{tableAlias: "b", rows: 500},
	}
	edges := []joinGraphEdge{
		{alias1: "a", alias2: "b", joinType: Inner},
	}

	orderedNodes, _, ok := greedyJoinOrder(nodes, edges)
	require.True(t, ok)
	assert.Equal(t, "a", orderedNodes[0].tableAlias)
	assert.Equal(t, "b", orderedNodes[1].tableAlias)
}

func TestCollectJoinGraph_TwoTableInner(t *testing.T) {
	t.Parallel()

	tblA := stubTable("a", 100)
	tblB := stubTable("b", 20)

	provider := &mapTableProvider{tables: map[string]*Table{"b": tblB}}
	tblA.provider = provider

	// SELECT ... FROM a AS x INNER JOIN b AS y ON x.id = y.a_id
	onCond := FieldIsEqual(
		Field{AliasPrefix: "x", Name: "id"},
		OperandField,
		Field{AliasPrefix: "y", Name: "a_id"},
	)
	stmt := Statement{
		TableAlias: "x",
		Joins: []Join{
			{
				TableName:  "b",
				TableAlias: "y",
				Type:       Inner,
				Conditions: Conditions{onCond},
			},
		},
	}

	nodes, edges, ok := tblA.collectJoinGraph(t.Context(), stmt)
	require.True(t, ok)
	require.Len(t, nodes, 2)
	require.Len(t, edges, 1)

	// First node is always the base table.
	assert.Equal(t, "x", nodes[0].tableAlias)
	assert.Equal(t, int64(100), nodes[0].rows)
	assert.Equal(t, "y", nodes[1].tableAlias)
	assert.Equal(t, int64(20), nodes[1].rows)

	assert.Equal(t, "x", edges[0].alias1)
	assert.Equal(t, "y", edges[0].alias2)
	assert.Equal(t, Inner, edges[0].joinType)
}

func TestCollectJoinGraph_ThreeTableChain(t *testing.T) {
	t.Parallel()

	tblA := stubTable("a", 1000)
	tblB := stubTable("b", 5)
	tblC := stubTable("c", 200)

	provider := &mapTableProvider{tables: map[string]*Table{
		"b": tblB,
		"c": tblC,
	}}
	tblA.provider = provider

	onAB := FieldIsEqual(Field{AliasPrefix: "a", Name: "id"}, OperandField, Field{AliasPrefix: "b", Name: "a_id"})
	onBC := FieldIsEqual(Field{AliasPrefix: "b", Name: "id"}, OperandField, Field{AliasPrefix: "c", Name: "b_id"})

	stmt := Statement{
		TableAlias: "a",
		Joins: []Join{
			{
				TableName:  "b",
				TableAlias: "b",
				Type:       Inner,
				Conditions: Conditions{onAB},
				Joins: []Join{
					{
						TableName:  "c",
						TableAlias: "c",
						Type:       Inner,
						Conditions: Conditions{onBC},
					},
				},
			},
		},
	}

	nodes, edges, ok := tblA.collectJoinGraph(t.Context(), stmt)
	require.True(t, ok)
	assert.Len(t, nodes, 3)
	assert.Len(t, edges, 2)
}

func TestCollectJoinGraph_UnknownTable_ReturnsFalse(t *testing.T) {
	t.Parallel()

	tblA := stubTable("a", 100)
	// Provider has no tables — GetTable will return false.
	tblA.provider = &mapTableProvider{tables: map[string]*Table{}}

	onCond := FieldIsEqual(
		Field{AliasPrefix: "x", Name: "id"},
		OperandField,
		Field{AliasPrefix: "y", Name: "a_id"},
	)
	stmt := Statement{
		TableAlias: "x",
		Joins: []Join{
			{
				TableName:  "missing",
				TableAlias: "y",
				Type:       Inner,
				Conditions: Conditions{onCond},
			},
		},
	}

	_, _, ok := tblA.collectJoinGraph(t.Context(), stmt)
	assert.False(t, ok)
}

func TestCollectJoinGraph_NoFromAlias_ReturnsFalse(t *testing.T) {
	t.Parallel()

	tblA := stubTable("a", 100)
	tblB := stubTable("b", 20)
	tblA.provider = &mapTableProvider{tables: map[string]*Table{"b": tblB}}

	// A join condition with no alias prefix on either side — FromTableAlias() returns "".
	onCond := FieldIsEqual(
		Field{Name: "id"},   // no AliasPrefix
		OperandField,
		Field{Name: "a_id"}, // no AliasPrefix
	)
	stmt := Statement{
		TableAlias: "x",
		Joins: []Join{
			{
				TableName:  "b",
				TableAlias: "y",
				Type:       Inner,
				Conditions: Conditions{onCond},
			},
		},
	}

	_, _, ok := tblA.collectJoinGraph(t.Context(), stmt)
	assert.False(t, ok)
}

func TestChanRowCallback(t *testing.T) {
	t.Parallel()

	t.Run("sends row to channel", func(t *testing.T) {
		ctx := context.Background()
		ch := make(chan Row, 1)
		cb := chanRowCallback(ctx, ch)

		row := NewRowWithValues([]Column{{Name: "id", Kind: Int8, Size: 8}}, []OptionalValue{{Value: int64(1), Valid: true}})
		err := cb(row)
		require.NoError(t, err)

		received := <-ch
		assert.Equal(t, row.Values, received.Values)
	})

	t.Run("returns ctx.Err when context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately
		ch := make(chan Row) // unbuffered — send would block
		cb := chanRowCallback(ctx, ch)

		row := NewRowWithValues([]Column{{Name: "id", Kind: Int8, Size: 8}}, []OptionalValue{{Value: int64(1), Valid: true}})
		err := cb(row)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestDrainRowCh(t *testing.T) {
	t.Parallel()

	ch := make(chan Row, 3)
	ch <- NewRowWithValues(nil, nil)
	ch <- NewRowWithValues(nil, nil)
	close(ch)

	// Should return without blocking after draining all values.
	drainRowCh(ch)
}

func TestDrainParallelScanCh(t *testing.T) {
	t.Parallel()

	ch := make(chan parallelScanResult, 2)
	ch <- parallelScanResult{row: NewRowWithValues(nil, nil)}
	close(ch)

	drainParallelScanCh(ch)
}

func TestPushDownFilters_UnknownAlias(t *testing.T) {
	t.Parallel()

	// WHERE x.foo = 1 where 'x' is not the base table alias and not a join alias.
	// The condition should fall through to the base group.
	conditions := OneOrMore{
		{
			FieldIsEqual(Field{AliasPrefix: "x", Name: "foo"}, OperandInteger, int64(1)),
		},
	}
	joins := []Join{
		{TableName: "profiles", TableAlias: "p"},
	}

	baseFilters, joinFilters := pushDownFilters(conditions, "u", joins)

	assert.Len(t, baseFilters, 1, "Unknown alias condition falls back to base group")
	assert.Len(t, baseFilters[0], 1)
	assert.Len(t, joinFilters["p"], 0)
}

func TestExtractJoinColumnPairs_NonFieldOperand(t *testing.T) {
	t.Parallel()

	baseTable := &Table{
		Name:        "a",
		Columns:     []Column{{Name: "id", Kind: Int8, Size: 8}},
		columnCache: map[string]int{"id": 0},
	}
	joinTable := &Table{
		Name:        "b",
		Columns:     []Column{{Name: "a_id", Kind: Int8, Size: 8}},
		columnCache: map[string]int{"a_id": 0},
	}

	// Condition with a non-field operand on the left — must be skipped.
	conditions := Conditions{
		{
			Operand1: Operand{Type: OperandInteger, Value: int64(1)},
			Operator: Eq,
			Operand2: Operand{Type: OperandField, Value: Field{AliasPrefix: "b", Name: "a_id"}},
		},
	}

	pairs, err := extractJoinColumnPairs(conditions, "a", "b", baseTable, joinTable)
	assert.Error(t, err)
	assert.Nil(t, pairs)
}

func TestFindIndexOnColumns_SkipNonBTreeAndPartial(t *testing.T) {
	t.Parallel()

	tbl := &Table{
		Name: "docs",
		PrimaryKey: PrimaryKey{
			IndexInfo: IndexInfo{
				Name:    "pk",
				Columns: []Column{{Name: "other_col", Kind: Int8, Size: 8}},
			},
		},
		UniqueIndexes: map[string]UniqueIndex{},
		SecondaryIndexes: map[string]SecondaryIndex{
			"ft_idx": {
				IndexInfo: IndexInfo{
					Name:    "ft_idx",
					Columns: []Column{{Name: "title", Kind: Varchar, Size: 255}},
					Method:  IndexMethodFullText, // non-BTree → should be skipped
				},
			},
			"partial_idx": {
				IndexInfo: IndexInfo{
					Name:        "partial_idx",
					Columns:     []Column{{Name: "title", Kind: Varchar, Size: 255}},
					Method:      IndexMethodBTree,
					WhereClause: "active = true", // partial → should be skipped
				},
			},
		},
	}

	// Searching for "title" column — neither the FTS nor the partial index should match.
	result := tbl.findIndexOnColumns([]string{"title"})
	assert.Nil(t, result, "non-BTree and partial indexes should be skipped")
}
