package minisql

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatement_Prepare_Insert(t *testing.T) {
	t.Parallel()

	t.Run("Insert with partial fields populates missing fields with NULLs or default values", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "users",
			Columns:   testColumns[0:4],
			Fields: []Field{
				{Name: "email"},
			},
			Inserts: [][]OptionalValue{
				{
					{Value: "foo@example.com", Valid: true},
				},
				{
					{Value: "bar@example.com", Valid: true},
				},
			},
		}

		err := stmt.Prepare(Time{})
		require.NoError(t, err)

		assert.Equal(t, fieldsFromColumns(stmt.Columns...), stmt.Fields)
		assert.Equal(t, [][]OptionalValue{
			{
				{}, // id
				{Value: "foo@example.com", Valid: true},
				{},                          // age
				{Value: false, Valid: true}, // verified has default value
			},
			{
				{}, // id
				{Value: "bar@example.com", Valid: true},
				{},                          // age
				{Value: false, Valid: true}, // verified has default value
			},
		}, stmt.Inserts)
	})

	t.Run("Parse timestamps in INSERT statements", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "users",
			Columns: []Column{
				{
					Kind: Timestamp,
					Size: 8,
					Name: "created_at",
				},
			},
			Fields: []Field{
				{Name: "created_at"},
			},
			Inserts: [][]OptionalValue{
				{
					{Value: NewTextPointer([]byte("2025-12-20 03:13:27.674801")), Valid: true},
				},
			},
		}

		err := stmt.Prepare(Time{})
		require.NoError(t, err)

		assert.Equal(t, fieldsFromColumns(stmt.Columns...), stmt.Fields)
		assert.Equal(t, [][]OptionalValue{
			{
				{
					Value: Time{
						Year:         2025,
						Month:        12,
						Day:          20,
						Hour:         3,
						Minutes:      13,
						Seconds:      27,
						Microseconds: 674801,
					},
					Valid: true,
				},
			},
		}, stmt.Inserts)
	})
}

func TestStatement_Prepare_Update(t *testing.T) {
	t.Parallel()

	t.Run("Parse timestamps in UPDATE statements", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: "users",
			Columns: []Column{
				{
					Kind: Timestamp,
					Size: 8,
					Name: "created_at",
				},
			},
			Fields: []Field{
				{Name: "created_at"},
			},
			Updates: map[string]OptionalValue{
				"created_at": {Value: NewTextPointer([]byte("2025-12-20 03:13:27.674801")), Valid: true},
			},
		}

		err := stmt.Prepare(Time{})
		require.NoError(t, err)

		assert.Equal(t, fieldsFromColumns(stmt.Columns...), stmt.Fields)
		assert.Equal(t, OptionalValue{
			Value: Time{
				Year:         2025,
				Month:        12,
				Day:          20,
				Hour:         3,
				Minutes:      13,
				Seconds:      27,
				Microseconds: 674801,
			},
			Valid: true,
		}, stmt.Updates["created_at"])
	})
}

func TestStatement_Validate(t *testing.T) {
	t.Parallel()

	// Test tables to validate against
	var (
		aTable = &Table{
			Name: testTableName,
			Columns: []Column{
				{
					Kind: Int4,
					Size: 4,
					Name: "id",
				},
				{
					Kind: Varchar,
					Size: MaxInlineVarchar,
					Name: "email",
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
					Nullable: true,
				},
			},
		}

		aTableWithPK = &Table{
			Name: testTableName,
			Columns: []Column{
				{
					Kind:       Int8,
					Size:       8,
					Name:       "id",
					PrimaryKey: true,
				},
				{
					Kind: Varchar,
					Size: MaxInlineVarchar,
					Name: "email",
				},
			},
		}

		aTableWithAutoincrementPK = &Table{
			Name: testTableName,
			Columns: []Column{
				{
					Kind:          Int8,
					Size:          8,
					Name:          "id",
					PrimaryKey:    true,
					Autoincrement: true,
				},
				{
					Kind: Varchar,
					Size: MaxInlineVarchar,
					Name: "email",
				},
			},
		}

		aTableWithDefaultValue = &Table{
			Name: testTableName,
			Columns: []Column{
				{
					Kind:       Int8,
					Size:       8,
					Name:       "id",
					PrimaryKey: true,
				},
				{
					Kind:         Varchar,
					Size:         MaxInlineVarchar,
					Name:         "status",
					DefaultValue: OptionalValue{Value: "pending", Valid: true},
				},
				{
					Kind:         Timestamp,
					Size:         8,
					Name:         "created_at",
					DefaultValue: OptionalValue{Value: "0001-01-01 00:00:00", Valid: true},
				},
			},
		}
	)

	t.Run("CREATE TABLE without table name should fail", func(t *testing.T) {
		stmt := Statement{
			Kind: CreateTable,
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "table name is required")
	})

	t.Run("CREATE TABLE without columns name should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "at least one column is required")
	})

	t.Run("CREATE TABLE with too many columns should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   make([]Column, MaxColumns+1), // Exceed max columns
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "maximum number of columns is 64")
	})

	t.Run("CREATE TABLE with excessive row size should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   appendUntilSize([]Column{}, UsablePageSize+1), // Exceed page size
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "potential row size exceeds maximum allowed 4065")
	})

	t.Run("CREATE TABLE with multiple primary keys should fail", func(t *testing.T) {
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

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "only one primary key column is supported")
	})

	t.Run("CREATE TABLE with nullable primary key should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Kind:       Int8,
					Nullable:   true,
					PrimaryKey: true,
				},
			},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "primary key column cannot be nullable")
	})

	t.Run("CREATE TABLE with TEXT primary key should fail", func(t *testing.T) {
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

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "primary key cannot be of type TEXT")
	})

	t.Run("CREATE TABLE with VARCHAR primary key exceeding max size should fail", func(t *testing.T) {
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

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, fmt.Sprintf("primary key of type VARCHAR exceeds max index key size %d", MaxIndexKeySize))
	})

	t.Run("CREATE TABLE with autoincrement primary key of invalid type should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Kind:          Real,
					PrimaryKey:    true,
					Autoincrement: true,
				},
			},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "autoincrement primary key must be of type INT4 or INT8")
	})

	t.Run("CREATE TABLE with more than one index on a column should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Name:       "foo",
					Kind:       Varchar,
					Size:       MaxInlineVarchar,
					PrimaryKey: true,
					Unique:     true,
				},
			},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "column foo can only have one index")
	})

	t.Run("CREATE TABLE with TEXT unique key should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Kind:   Text,
					Unique: true,
				},
			},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "unique key cannot be of type TEXT")
	})

	t.Run("CREATE TABLE with VARCHAR unique key exceeding max size should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns: []Column{
				{
					Kind:   Varchar,
					Unique: true,
					Size:   300,
				},
			},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, fmt.Sprintf("unique key of type VARCHAR exceeds max index key size %d", MaxIndexKeySize))
	})

	t.Run("CREATE TABLE should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   testColumns,
		}

		err := stmt.Validate(nil)
		require.NoError(t, err)
	})

	t.Run("CREATE TABLE with primary key should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   testColumnsWithPrimaryKey,
		}

		err := stmt.Validate(nil)
		require.NoError(t, err)
	})

	t.Run("CREATE TABLE with default values should succeed and parse default values", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: aTableWithDefaultValue.Name,
			Columns:   aTableWithDefaultValue.Columns,
		}

		_, ok := aTableWithDefaultValue.Columns[1].DefaultValue.Value.(TextPointer)
		assert.False(t, ok)
		_, ok = aTableWithDefaultValue.Columns[2].DefaultValue.Value.(Time)
		assert.False(t, ok)

		err := stmt.Validate(nil)
		require.NoError(t, err)

		_, ok = aTableWithDefaultValue.Columns[1].DefaultValue.Value.(TextPointer)
		assert.True(t, ok, "expected default value for 'status' column to be TextPointer")
		_, ok = aTableWithDefaultValue.Columns[2].DefaultValue.Value.(Time)
		assert.True(t, ok, "expected default value for 'created_at' column to be Time")
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

	t.Run("INSERT with missing field for column with default values should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTableWithDefaultValue.Name,
			Columns:   aTableWithDefaultValue.Columns,
			Fields:    []Field{{Name: "id"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int64(1), Valid: true},
				},
			},
		}

		err := stmt.Validate(aTableWithDefaultValue)
		require.NoError(t, err)
	})

	t.Run("INSERT with NULL for primary key should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTableWithPK.Name,
			Columns:   aTableWithPK.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			Inserts: [][]OptionalValue{
				{
					{Valid: false}, // NULL for primary key
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
				},
			},
		}

		err := stmt.Validate(aTableWithPK)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "id" cannot be NULL`)
	})

	t.Run("INSERT with NULL for auto-incremented primary key should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTableWithAutoincrementPK.Name,
			Columns:   aTableWithAutoincrementPK.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			Inserts: [][]OptionalValue{
				{
					{Valid: false}, // NULL for primary key
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
				},
			},
		}

		err := stmt.Validate(aTableWithAutoincrementPK)
		require.NoError(t, err)
	})

	t.Run("INSERT with NULL for non-nullable column should fail", func(t *testing.T) {
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

	t.Run("INSERT with NULL for nullable column should succeed", func(t *testing.T) {
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

	t.Run("UPDATE with invalid UTF-8 string should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "email"}},
			Updates: map[string]OptionalValue{
				"email": {Value: NewTextPointer([]byte{0xff, 0xfe, 0xfd}), Valid: true}, // invalid UTF-8,
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

	t.Run("SELECT with unknown field in ORDER BY should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			OrderBy: []OrderBy{
				{
					Field: Field{Name: "unknown_field"},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "unknown_field" in ORDER BY clause`)
	})

	t.Run("SELECT COUNT(*) with ORDER BY should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "COUNT(*)"}},
			OrderBy: []OrderBy{
				{
					Field: Field{Name: "id"},
				},
			},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `ORDER BY cannot be used with COUNT(*)`)
	})

	t.Run("SELECT COUNT(*) with OFFSET should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "COUNT(*)"}},
			Offset:    OptionalValue{Value: int64(100), Valid: true},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `OFFSET cannot be used with COUNT(*)`)
	})

	t.Run("SELECT COUNT(*) with LIMIT should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    []Field{{Name: "COUNT(*)"}},
			Limit:     OptionalValue{Value: int64(100), Valid: true},
		}

		err := stmt.Validate(aTable)
		require.Error(t, err)
		assert.ErrorContains(t, err, `LIMIT cannot be used with COUNT(*)`)
	})

	t.Run("SELECT with invalid LIMIT should fail", func(t *testing.T) {
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

func TestStatement_ValidateColumnValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		column      Column
		insertValue OptionalValue
		err         string
	}{
		{
			"invalid BOOLEAN value",
			Column{Kind: Boolean, Name: "foo"},
			OptionalValue{Value: "not_a_bool", Valid: true},
			`field "foo" expects BOOLEAN value`,
		},
		{
			"valid BOOLEAN value",
			Column{Kind: Boolean, Name: "foo"},
			OptionalValue{Value: true, Valid: true},
			"",
		},
		{
			"invalid INT4 value",
			Column{Kind: Int4, Name: "foo"},
			OptionalValue{Value: "not_an_int", Valid: true},
			`field "foo" expects INT4 value`,
		},
		{
			"valid INT4 value",
			Column{Kind: Int4, Name: "foo"},
			OptionalValue{Value: int32(25), Valid: true},
			"",
		},
		{
			"invalid INT8 value",
			Column{Kind: Int8, Name: "foo"},
			OptionalValue{Value: int32(25), Valid: true},
			`field "foo" expects INT8 value`,
		},
		{
			"valid INT8 value",
			Column{Kind: Int8, Name: "foo"},
			OptionalValue{Value: int64(25), Valid: true},
			"",
		},
		{
			"invalid REAL value",
			Column{Kind: Real, Name: "foo"},
			OptionalValue{Value: "not_a_real", Valid: true},
			`field "foo" expects REAL value`,
		},
		{
			"valid REAL value",
			Column{Kind: Real, Name: "foo"},
			OptionalValue{Value: float32(25.5), Valid: true},
			"",
		},
		{
			"invalid DOUBLE value",
			Column{Kind: Double, Name: "foo"},
			OptionalValue{Value: float32(25.5), Valid: true},
			`field "foo" expects DOUBLE value`,
		},
		{
			"valid DOUBLE value",
			Column{Kind: Double, Name: "foo"},
			OptionalValue{Value: float64(25.5), Valid: true},
			"",
		},
		{
			"invalid TEXT value",
			Column{Kind: Text, Name: "foo"},
			OptionalValue{Value: float32(25.5), Valid: true},
			`field "foo" expects a text value`,
		},
		{
			"valid TEXT value",
			Column{Kind: Text, Name: "foo"},
			OptionalValue{Value: NewTextPointer([]byte("some text")), Valid: true},
			"",
		},
		{
			"invalid VARCHAR value",
			Column{Kind: Varchar, Name: "foo"},
			OptionalValue{Value: float32(25.5), Valid: true},
			`field "foo" expects a text value`,
		},
		{
			"valid VARCHAR value",
			Column{Kind: Varchar, Name: "foo"},
			OptionalValue{Value: NewTextPointer([]byte("some text")), Valid: true},
			"",
		},
		{
			"invalid TIMESTAMP value",
			Column{Kind: Timestamp, Name: "foo"},
			OptionalValue{Value: int32(25), Valid: true},
			`field "foo" expects time value`,
		},
		{
			"valid TIMESTAMP value",
			Column{Kind: Timestamp, Name: "foo"},
			OptionalValue{Value: MustParseTimestamp("2000-01-01 00:00:00"), Valid: true},
			"",
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.name, func(t *testing.T) {
			stmt := Statement{}
			err := stmt.validateColumnValue(aTestCase.column, aTestCase.insertValue)
			if aTestCase.err == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, aTestCase.err)
		})
	}
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
					Kind:         Boolean,
					Size:         1,
					Name:         "verified",
					Nullable:     false,
					DefaultValue: OptionalValue{Value: false, Valid: true},
				},
				{
					Kind:     Real,
					Size:     4,
					Name:     "score",
					Nullable: true,
				},
				{
					Kind:            Timestamp,
					Size:            8,
					Name:            "created_at",
					Nullable:        true,
					DefaultValueNow: true,
				},
			},
		}

		expected := `create table "users" (
	id int8 primary key autoincrement,
	email varchar(255),
	age int4,
	verified boolean not null default false,
	score real,
	created_at timestamp default now()
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
