package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatement_Validate(t *testing.T) {
	t.Parallel()

	// Create a test table with both nullable and non-nullable columns
	aTable := &Table{
		Name: testTableName,
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

	t.Run("CREATE without table name should fail", func(t *testing.T) {
		stmt := Statement{
			Kind: CreateTable,
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "table name is required")
	})

	t.Run("CREATE without columns name should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "at least one column is required")
	})

	t.Run("CREATE with too many columns should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   make([]Column, MaxColumns+1), // Exceed max columns
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "maximum number of columns is 64")
	})

	t.Run("CREATE with excessive row size should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Size: UsablePageSize + 1, // Exceed max row size by 1 byte
					Kind: Varchar,
					Name: "too_large",
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "row size 4067 exceeds maximum allowed 4066")
	})

	t.Run("INSERT with wrong number of columns should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns[1:], // Missing the "id" column
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

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "insert: expected 4 columns, got 3")
	})

	t.Run("INSERT with wrong number of fields should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []string{"id", "email", "age"},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: "test@example.com", Valid: true},
					{Value: int32(25), Valid: true},
					{Value: true, Valid: true},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "insert: expected 4 fields, got 3")
	})

	t.Run("INSERT with NULL to non-nullable column should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
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

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "id" cannot be NULL`)
	})

	t.Run("INSERT with NULL to nullable column should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
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

		err := stmt.Validate(aTable)
		require.NoError(t, err)
	})

	t.Run("INSERT with valid values should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
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

		err := stmt.Validate(aTable)
		require.NoError(t, err)
	})

	t.Run("INSERT with unknown field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []string{"unknown_field", "unknown_field", "unknown_field", "unknown_field"},
			Inserts: [][]OptionalValue{
				{
					{Value: "some_value", Valid: true},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "unknown_field" in table "test_table"`)
	})

	t.Run("INSERT with invalid UTF-8 string should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []string{"id", "email", "age", "verified"},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: string([]byte{0xff, 0xfe, 0xfd}), Valid: true}, // invalid UTF-8
					{Valid: false}, // NULL for nullable age
					{Valid: false}, // NULL for nullable verified
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "email" expects valid UTF-8 string`)
	})

	t.Run("UPDATE with NULL to non-nullable column should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []string{"email"},
			Updates: map[string]OptionalValue{
				"email": {Valid: false}, // NULL for non-nullable email
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "email" cannot be NULL`)
	})

	t.Run("UPDATE with NULL to nullable column should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []string{"age"},
			Updates: map[string]OptionalValue{
				"age": {Valid: false}, // NULL for nullable age
			},
		}

		err := stmt.Validate(aTable)
		require.NoError(t, err)
	})

	t.Run("UPDATE with valid value should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []string{"email", "age"},
			Updates: map[string]OptionalValue{
				"email": {Value: "new@example.com", Valid: true},
				"age":   {Value: int32(30), Valid: true},
			},
		}

		err := stmt.Validate(aTable)
		require.NoError(t, err)
	})
}

func TestStatement_CreateTableDDL(t *testing.T) {
	t.Parallel()

	t.Run("table with all data types and nullable columns", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: "users",
			Columns: []Column{
				{
					Kind:     Int8,
					Size:     8,
					Name:     "id",
					Nullable: false,
				},
				{
					Kind:     Varchar,
					Size:     255,
					Name:     "email",
					Nullable: true,
				},
				{
					Kind:     Int4,
					Size:     4,
					Name:     "age",
					Nullable: true,
				},
				{
					Kind:     Boolean,
					Size:     1,
					Name:     "verified",
					Nullable: false,
				},
				{
					Kind:     Real,
					Size:     4,
					Name:     "score",
					Nullable: true,
				},
				{
					Kind:     Double,
					Size:     8,
					Name:     "balance",
					Nullable: true,
				},
			},
		}

		expected := `create table "users" (
	id int8 not null,
	email varchar(255),
	age int4,
	verified boolean not null,
	score real,
	balance double
);`

		actual := stmt.CreateTableDDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("table with special characters in name", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: "test_table_123",
			Columns: []Column{
				{
					Kind:     Int4,
					Size:     4,
					Name:     "column_with_underscore",
					Nullable: false,
				},
			},
		}

		expected := `create table "test_table_123" (
	column_with_underscore int4 not null
);`

		actual := stmt.CreateTableDDL()
		assert.Equal(t, expected, actual)
	})
}

func TestFieldIsIn(t *testing.T) {
	t.Parallel()

	t.Run("string field with quoted string value", func(t *testing.T) {
		condition := FieldIsIn("email", OperandQuotedString, "john@example.com")

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "email",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: "john@example.com",
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("boolean field with boolean value", func(t *testing.T) {
		condition := FieldIsIn("verified", OperandBoolean, true)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "verified",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandBoolean,
				Value: true,
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("integer field with integer value", func(t *testing.T) {
		condition := FieldIsIn("id", OperandInteger, int64(25))

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "id",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(25),
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("float field with float value", func(t *testing.T) {
		condition := FieldIsIn("score", OperandFloat, 95.5)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "score",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandFloat,
				Value: 95.5,
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("field with null value", func(t *testing.T) {
		condition := FieldIsIn("description", OperandNull, nil)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "description",
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandNull,
				Value: nil,
			},
		}

		assert.Equal(t, expected, condition)
	})
}

func TestFieldIsNotIn(t *testing.T) {
	t.Parallel()

	t.Run("string field with quoted string value", func(t *testing.T) {
		condition := FieldIsNotIn("email", OperandQuotedString, "john@example.com")

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "email",
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: "john@example.com",
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("boolean field with boolean value", func(t *testing.T) {
		condition := FieldIsNotIn("verified", OperandBoolean, true)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "verified",
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandBoolean,
				Value: true,
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("integer field with integer value", func(t *testing.T) {
		condition := FieldIsNotIn("id", OperandInteger, int64(25))

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "id",
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(25),
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("float field with float value", func(t *testing.T) {
		condition := FieldIsNotIn("score", OperandFloat, 95.5)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "score",
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandFloat,
				Value: 95.5,
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("field with null value", func(t *testing.T) {
		condition := FieldIsNotIn("description", OperandNull, nil)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: "description",
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandNull,
				Value: nil,
			},
		}

		assert.Equal(t, expected, condition)
	})
}
