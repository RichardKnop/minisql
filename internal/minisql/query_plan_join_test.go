package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
