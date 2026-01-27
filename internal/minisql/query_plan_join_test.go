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
