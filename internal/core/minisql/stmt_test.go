package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatement_Validate(t *testing.T) {
	// Create a test table with both nullable and non-nullable columns
	testTable := &Table{
		Name: "test_table",
		Columns: []Column{
			{
				Kind:     Int4,
				Size:     4,
				Name:     "id",
				Nullable: false, // non-nullable
			},
			{
				Kind:     Varchar,
				Size:     255,
				Name:     "email",
				Nullable: false, // non-nullable
			},
			{
				Kind:     Int4,
				Size:     4,
				Name:     "age",
				Nullable: true, // nullable
			},
			{
				Kind:     Boolean,
				Size:     1,
				Name:     "verified",
				Nullable: true, // nullable
			},
		},
	}

	t.Run("INSERT with NULL to non-nullable column should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "test_table",
			Fields:    []string{"id", "email", "age", "verified"},
			Inserts: [][]OptionalValue{
				{
					{Valid: false}, // NULL for non-nullable id
					{Value: "test@example.com", Valid: true},
					{Value: int32(25), Valid: true},
					{Value: true, Valid: true},
				},
			},
		}

		err := stmt.Validate(testTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "id" cannot be NULL`)
	})

	t.Run("INSERT with NULL to nullable column should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "test_table",
			Fields:    []string{"id", "email", "age", "verified"},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: "test@example.com", Valid: true},
					{Valid: false}, // NULL for nullable age
					{Valid: false}, // NULL for nullable verified
				},
			},
		}

		err := stmt.Validate(testTable)
		require.NoError(t, err)
	})

	t.Run("INSERT with valid values should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "test_table",
			Fields:    []string{"id", "email", "age", "verified"},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: "test@example.com", Valid: true},
					{Value: int32(25), Valid: true},
					{Value: true, Valid: true},
				},
			},
		}

		err := stmt.Validate(testTable)
		require.NoError(t, err)
	})

	t.Run("UPDATE with NULL to non-nullable column should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: "test_table",
			Fields:    []string{"email"},
			Updates: map[string]OptionalValue{
				"email": {Valid: false}, // NULL for non-nullable email
			},
		}

		err := stmt.Validate(testTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "email" cannot be NULL`)
	})

	t.Run("UPDATE with NULL to nullable column should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: "test_table",
			Fields:    []string{"age"},
			Updates: map[string]OptionalValue{
				"age": {Valid: false}, // NULL for nullable age
			},
		}

		err := stmt.Validate(testTable)
		require.NoError(t, err)
	})

	t.Run("UPDATE with valid value should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: "test_table",
			Fields:    []string{"email", "age"},
			Updates: map[string]OptionalValue{
				"email": {Value: "new@example.com", Valid: true},
				"age":   {Value: int32(30), Valid: true},
			},
		}

		err := stmt.Validate(testTable)
		require.NoError(t, err)
	})

	t.Run("INSERT with unknown field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "test_table",
			Fields:    []string{"unknown_field"},
			Inserts: [][]OptionalValue{
				{
					{Value: "some_value", Valid: true},
				},
			},
		}

		err := stmt.Validate(testTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "unknown_field" in table "test_table"`)
	})

	t.Run("INSERT with invalid UTF-8 string should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "test_table",
			Fields:    []string{"id", "email"},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: string([]byte{0xff, 0xfe, 0xfd}), Valid: true}, // invalid UTF-8
				},
			},
		}

		err := stmt.Validate(testTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "email" expects valid UTF-8 string`)
	})
}
