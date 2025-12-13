package minisql

import (
	"bytes"
	"fmt"
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
				Size:     MaxInlineVarchar,
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
			Columns:   appendUntilSize([]Column{}, UsablePageSize+1), // Exceed page size
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "potential row size exceeds maximum allowed 4065")
	})

	t.Run("CREATE with multiple primary keys should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Kind:       Int8,
					PrimaryKey: true,
					Name:       "id1",
				},
				{
					Kind:       Int8,
					PrimaryKey: true,
					Name:       "id2",
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "only one primary key column is supported")
	})

	t.Run("CREATE with TEXT primary key should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Kind:       Text,
					PrimaryKey: true,
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "primary key cannot be of type TEXT")
	})

	t.Run("CREATE with VARCHAR primary key exceeding max size should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Kind:       Varchar,
					PrimaryKey: true,
					Size:       300,
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, fmt.Sprintf("primary key of type VARCHAR exceeds max index key size %d", MaxIndexKeySize))
	})

	t.Run("CREATE with should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   testColumns,
		}

		err := stmt.Validate(aTable)
		require.NoError(t, err)
	})

	t.Run("CREATE with primary key should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   testColumnsWithPrimaryKey,
		}

		err := stmt.Validate(aTable)
		require.NoError(t, err)
	})

	t.Run("INSERT with wrong number of columns should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns[1:], // Missing the "id" column
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "age"}, {Name: "verified"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
					{Value: int32(25), Valid: true},
					{Value: true, Valid: true},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, "insert: expected 4 columns, got 3")
	})

	t.Run("INSERT with missing required field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "id"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `missing required field "email"`)
	})

	t.Run("INSERT with NULL to non-nullable column should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "age"}, {Name: "verified"}},
			Inserts: [][]OptionalValue{
				{
					{Valid: false}, // NULL for non-nullable id
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
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
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "age"}, {Name: "verified"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
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
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "age"}, {Name: "verified"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
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
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "bogus"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
					{Value: int32(25), Valid: true},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "bogus" in table "test_table"`)
	})

	t.Run("INSERT with invalid UTF-8 string should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "age"}, {Name: "verified"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer([]byte{0xff, 0xfe, 0xfd}), Valid: true}, // invalid UTF-8
					{Valid: false}, // NULL for nullable age
					{Valid: false}, // NULL for nullable verified
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "email" expects valid UTF-8 string`)
	})

	t.Run("INSERT with text exceeding maximum VARCHAR length should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer(bytes.Repeat([]byte{'a'}, 256)), Valid: true},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "email" exceeds maximum VARCHAR length of 255`)
	})

	t.Run("UPDATE with unknown field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "unknown_field"}},
			Updates: map[string]OptionalValue{
				"unknown_field": {Valid: false},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "unknown_field" in table "test_table"`)
	})

	t.Run("UPDATE with NULL to non-nullable column should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "email"}},
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
			Fields:    []Field{{Name: "age"}},
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
			Fields:    []Field{{Name: "email"}, {Name: "age"}},
			Updates: map[string]OptionalValue{
				"email": {Value: NewTextPointer([]byte("new@example.com")), Valid: true},
				"age":   {Value: int32(30), Valid: true},
			},
		}

		err := stmt.Validate(aTable)
		require.NoError(t, err)
	})

	t.Run("SELECT with no fields should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{}, // No fields specified
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `at least one field to select is required`)
	})

	t.Run("SELECT with duplicate should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "id"}},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `duplicate field "id" in select statement`)
	})

	t.Run("SELECT with unknown field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "unknown_field"}},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "unknown_field" in table "test_table"`)
	})

	t.Run("SELECT with invalid limit should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			Limit:     OptionalValue{Value: int64(-5), Valid: true},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `LIMIT must be a non-negative integer`)
	})

	t.Run("SELECT with invalid offset should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			Offset:    OptionalValue{Value: int64(-5), Valid: true},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `OFFSET must be a non-negative integer`)
	})

	t.Run("SELECT with inconsistent argument list for IN should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    fieldsFromColumns(aTable.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "id",
						},
						Operator: In,
						Operand2: Operand{
							Type:  OperandList,
							Value: []any{int64(1), "string_instead_of_int"},
						},
					},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `mixed operand types in WHERE condition list`)
	})

	t.Run("WHERE with conflicting equality conditions should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    fieldsFromColumns(aTable.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "id",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(1),
						},
					},
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "id",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: int64(2),
						},
					},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `conflicting equality conditions for field "id" in WHERE clause`)
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
					Kind:          Int8,
					Size:          8,
					Name:          "id",
					PrimaryKey:    true,
					Autoincrement: true,
				},
				{
					Kind:     Varchar,
					Size:     MaxInlineVarchar,
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
	id int8 primary key autoincrement,
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

func TestStatement_InsertForColumn(t *testing.T) {
	t.Parallel()

	stmt := Statement{
		Kind:      Insert,
		TableName: "users",
		Columns: []Column{
			{
				Kind:     Int4,
				Size:     4,
				Name:     "id",
				Nullable: false,
			},
			{
				Kind:     Varchar,
				Size:     MaxInlineVarchar,
				Name:     "email",
				Nullable: false,
			},
		},
		Fields: []Field{{Name: "id"}, {Name: "email"}},
		Inserts: [][]OptionalValue{
			{
				{Value: int32(1), Valid: true},
				{Value: "john@example.com", Valid: true},
			},
			{
				{Value: int32(2), Valid: true},
				{Value: "jane@example.com", Valid: true},
			},
		},
	}

	val, ok := stmt.InsertForColumn("email", 5)
	require.False(t, ok)
	assert.Equal(t, OptionalValue{}, val)

	val, ok = stmt.InsertForColumn("email", 0)
	require.True(t, ok)
	assert.Equal(t, OptionalValue{Value: "john@example.com", Valid: true}, val)

	val, ok = stmt.InsertForColumn("email", 1)
	require.True(t, ok)
	assert.Equal(t, OptionalValue{Value: "jane@example.com", Valid: true}, val)
}

func TestStatement_IsSelectAll(t *testing.T) {
	t.Parallel()

	stmt := Statement{
		Kind:   Select,
		Fields: []Field{{Name: "*"}},
	}
	stmt2 := Statement{
		Kind:   Select,
		Fields: []Field{{Name: "id"}, {Name: "email"}},
	}

	assert.True(t, stmt.IsSelectAll())
	assert.False(t, stmt2.IsSelectAll())
}
