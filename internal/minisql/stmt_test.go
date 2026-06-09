package minisql

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStatement_NumberPlaceholders(t *testing.T) {
	t.Parallel()

	stmt := Statement{
		Kind:      Update,
		TableName: "a",
		Updates: map[string]OptionalValue{
			"b": {Value: NewTextPointer([]byte("foo")), Valid: true},
			"c": {Value: Placeholder{}, Valid: true},
		},
		Conditions: OneOrMore{
			{
				FieldIsEqual(Field{Name: "a"}, OperandPlaceholder, nil),
				FieldIsEqual(Field{Name: "b"}, OperandInteger, int64(789)),
			},
		},
	}

	assert.Equal(t, 2, stmt.NumPlaceholders())
}

func TestStatement_BindArguments(t *testing.T) {
	t.Parallel()

	t.Run("Bind INSERT statement", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "a",
			Fields:    []Field{{Name: "b"}, {Name: "c"}, {Name: "d"}},
			Inserts: [][]OptionalValue{
				{
					{Value: NewTextPointer([]byte("foo")), Valid: true},
					{Value: Placeholder{}, Valid: true},
					{Value: Placeholder{}, Valid: true},
				},
			},
		}

		var err error
		stmtWithArgs, err := stmt.BindArguments(int64(123), "bar")
		require.NoError(t, err)

		assert.Equal(t, NewTextPointer([]byte("foo")), stmtWithArgs.Inserts[0][0].Value)
		assert.Equal(t, int64(123), stmtWithArgs.Inserts[0][1].Value)
		assert.Equal(t, "bar", stmtWithArgs.Inserts[0][2].Value)

		// Ensure original statement is unchanged
		assert.Equal(t, NewTextPointer([]byte("foo")), stmt.Inserts[0][0].Value)
		assert.Equal(t, Placeholder{}, stmt.Inserts[0][1].Value)
		assert.Equal(t, Placeholder{}, stmt.Inserts[0][2].Value)
	})

	t.Run("Bind SELECT with OperandList placeholders", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: "users",
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, Placeholder{}, Placeholder{}),
				},
			},
		}
		bound, err := stmt.BindArguments(int64(1), int64(2))
		require.NoError(t, err)
		list := bound.Conditions[0][0].Operand2.Value.([]any)
		assert.Equal(t, int64(1), list[0])
		assert.Equal(t, int64(2), list[1])
	})

	t.Run("Bind UPDATE statement", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: "a",
			Fields:    []Field{{Name: "b"}, {Name: "c"}, {Name: "d"}},
			Updates: map[string]OptionalValue{
				"b": {Value: NewTextPointer([]byte("foo")), Valid: true},
				"c": {Value: Placeholder{}, Valid: true},
				"d": {Value: Placeholder{}, Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "a"}, OperandPlaceholder, nil),
					FieldIsEqual(Field{Name: "b"}, OperandInteger, int64(789)),
				},
			},
		}

		var err error
		stmtWithArgs, err := stmt.BindArguments(int64(123), nil, "bar")
		require.NoError(t, err)

		assert.Equal(t, int64(123), stmtWithArgs.Updates["c"].Value)
		assert.Equal(t, OptionalValue{}, stmtWithArgs.Updates["d"])
		condition := stmtWithArgs.Conditions[0][0]
		assert.Equal(t, "bar", condition.Operand2.Value)

		// Ensure original statement is unchanged
		assert.Equal(t, Placeholder{}, stmt.Updates["c"].Value)
		assert.Equal(t, Placeholder{}, stmt.Updates["d"].Value)
		originalCondition := stmt.Conditions[0][0]
		assert.Equal(t, OperandPlaceholder, originalCondition.Operand2.Type)
	})
}

func TestStatement_Prepare_Insert(t *testing.T) {
	t.Parallel()

	now := Time{
		Year:         2025,
		Month:        12,
		Day:          20,
		Hour:         3,
		Minutes:      13,
		Seconds:      27,
		Microseconds: 674801,
	}

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

		var err error
		stmt, err = stmt.Prepare(Time{})
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

	t.Run("Unknown functions in INSERT statements cause error", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "users",
			Columns: []Column{
				{
					Kind: Timestamp,
					Size: 8,
					Name: "created",
				},
			},
			Fields: []Field{
				{Name: "created"},
			},
			Inserts: [][]OptionalValue{
				{
					{Value: Function{Name: "UNKNOWN_FUNCTION"}, Valid: true},
				},
			},
		}

		_, err := stmt.Prepare(Time{})
		require.Error(t, err)
		assert.ErrorContains(t, err, `unsupported function "UNKNOWN_FUNCTION" in INSERT`)
	})

	t.Run("Replace NOW() function in INSERT statements with Time", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "users",
			Columns: []Column{
				{
					Kind: Timestamp,
					Size: 8,
					Name: "created",
				},
			},
			Fields: []Field{
				{Name: "created"},
			},
			Inserts: [][]OptionalValue{
				{
					{Value: FunctionNow, Valid: true},
				},
			},
		}

		var err error
		stmt, err = stmt.Prepare(now)
		require.NoError(t, err)

		assert.Equal(t, fieldsFromColumns(stmt.Columns...), stmt.Fields)
		assert.Equal(t, [][]OptionalValue{
			{
				{
					Value: TimestampMicros(now.TotalMicroseconds()),
					Valid: true,
				},
			},
		}, stmt.Inserts)
	})

	t.Run("Parse NOW() function in INSERT statements", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: "users",
			Columns: []Column{
				{
					Kind: Timestamp,
					Size: 8,
					Name: "created",
				},
			},
			Fields: []Field{
				{Name: "created"},
			},
			Inserts: [][]OptionalValue{
				{
					{Value: FunctionNow, Valid: true},
				},
			},
		}

		var err error
		stmt, err = stmt.Prepare(now)
		require.NoError(t, err)

		assert.Equal(t, fieldsFromColumns(stmt.Columns...), stmt.Fields)
		assert.Equal(t, [][]OptionalValue{
			{
				{
					Value: TimestampMicros(now.TotalMicroseconds()),
					Valid: true,
				},
			},
		}, stmt.Inserts)
	})
}

func TestStatement_Prepare_Update(t *testing.T) {
	t.Parallel()

	now := Time{
		Year:         2025,
		Month:        12,
		Day:          20,
		Hour:         3,
		Minutes:      13,
		Seconds:      27,
		Microseconds: 674801,
	}

	t.Run("Parse timestamps in UPDATE statements", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: "users",
			Columns: []Column{
				{
					Kind: Timestamp,
					Size: 8,
					Name: "created",
				},
			},
			Fields: []Field{{Name: "created"}},
			Updates: map[string]OptionalValue{
				"created": {Value: NewTextPointer([]byte("2025-12-20 03:13:27.674801")), Valid: true},
			},
		}

		var err error
		stmt, err = stmt.Prepare(now)
		require.NoError(t, err)

		assert.Equal(t, OptionalValue{
			Value: TimestampMicros(now.TotalMicroseconds()),
			Valid: true,
		}, stmt.Updates["created"])
	})

	t.Run("Unknown functions in UPDATE statements cause error", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: "users",
			Columns: []Column{
				{
					Kind: Timestamp,
					Size: 8,
					Name: "created",
				},
			},
			Fields: []Field{{Name: "created"}},
			Updates: map[string]OptionalValue{
				"created": {Value: Function{Name: "UNKNOWN_FUNCTION"}, Valid: true},
			},
		}

		_, err := stmt.Prepare(now)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unsupported function "UNKNOWN_FUNCTION" in UPDATE`)
	})

	t.Run("Replace NOW() function in UPDATE statements", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: "users",
			Columns: []Column{
				{
					Kind: Timestamp,
					Size: 8,
					Name: "created",
				},
			},
			Fields: []Field{{Name: "created"}},
			Updates: map[string]OptionalValue{
				"created": {Value: FunctionNow, Valid: true},
			},
		}

		var err error
		stmt, err = stmt.Prepare(now)
		require.NoError(t, err)

		assert.Equal(t, OptionalValue{
			Value: TimestampMicros(now.TotalMicroseconds()),
			Valid: true,
		}, stmt.Updates["created"])
	})
}

func TestStatement_Prepare_CreateTable(t *testing.T) {
	t.Parallel()

	var (
		columns = []Column{
			{
				Kind: Int8,
				Size: 8,
				Name: "id",
			},
			{
				Kind:         Timestamp,
				Size:         8,
				Name:         "created",
				DefaultValue: OptionalValue{Value: NewTextPointer([]byte("0001-01-01 00:00:00")), Valid: true},
			},
		}
		stmt = Statement{
			Kind:       CreateTable,
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("pkey__tablename", columns[0:1], false),
		}
	)

	_, ok := stmt.Columns[1].DefaultValue.Value.(TimestampMicros)
	assert.False(t, ok)

	var err error
	stmt, err = stmt.Prepare(Time{})
	require.NoError(t, err)

	_, ok = stmt.Columns[1].DefaultValue.Value.(TimestampMicros)
	assert.True(t, ok, "expected default value for 'created' column to be TimestampMicros")
	assert.Equal(t, MustParseTimestampMicros("0001-01-01 00:00:00"), stmt.Columns[1].DefaultValue.Value)
}

func TestStatement_Validate(t *testing.T) {
	t.Parallel()

	// Test tables to validate against
	var (
		columns = []Column{
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
		}
		table = NewTable(zap.NewNop(), nil, nil, testTableName, columns, 0, nil)

		aTableWithPK = NewTable(zap.NewNop(), nil, nil, testTableName, columns[0:2], 0, nil, WithPrimaryKey(
			NewPrimaryKey("foo", columns[0:1], false),
		))

		aTableWithAutoincrementPK = NewTable(zap.NewNop(), nil, nil, testTableName, columns[0:2], 0, nil, WithPrimaryKey(
			NewPrimaryKey("foo", columns[0:1], true),
		))

		defaultValueColumns = []Column{
			{
				Kind: Int8,
				Size: 8,
				Name: "id",
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
				Name:         "created",
				DefaultValue: OptionalValue{Value: "0001-01-01 00:00:00", Valid: true},
			},
		}
		aTableWithDefaultValue = NewTable(zap.NewNop(), nil, nil, testTableName, defaultValueColumns, 0, nil, WithPrimaryKey(
			NewPrimaryKey("pk", defaultValueColumns[0:1], true),
		))
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
		assert.ErrorContains(t, err, "potential row size exceeds maximum allowed 4061")
	})

	t.Run("CREATE TABLE with nullable primary key should fail", func(t *testing.T) {
		columns := []Column{
			{
				Name:     "id",
				Kind:     Int8,
				Nullable: true,
			},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  testTableName,
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("foo", columns[0:1], false),
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "primary key column cannot be nullable")
	})

	t.Run("CREATE TABLE with TEXT primary key should fail", func(t *testing.T) {
		columns := []Column{
			{
				Name: "id",
				Kind: Text,
			},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  testTableName,
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("foo", columns[0:1], false),
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "primary key cannot be of type TEXT")
	})

	t.Run("CREATE TABLE with primary key exceeding max size should fail", func(t *testing.T) {
		columns := []Column{
			{
				Name: "id",
				Kind: Varchar,
				Size: 300,
			},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  testTableName,
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("foo", columns[0:1], false),
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, fmt.Sprintf("primary key size exceeds max index key size %d", MaxIndexKeySize))
	})

	t.Run("CREATE TABLE with autoincrement primary key of invalid type should fail", func(t *testing.T) {
		columns := []Column{
			{
				Name: "id",
				Kind: Real,
			},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  testTableName,
			Columns:    columns[0:1],
			PrimaryKey: NewPrimaryKey("foo", columns[0:1], true),
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "autoincrement primary key must be of type INT4 or INT8")
	})

	t.Run("CREATE TABLE with more than one index on a column should fail", func(t *testing.T) {
		columns := []Column{
			{
				Name: "id",
				Kind: Varchar,
				Size: MaxIndexKeySize,
			},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  testTableName,
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("pk__users", columns[0:1], false),
			UniqueIndexes: []UniqueIndex{
				{
					IndexInfo: IndexInfo{
						Columns: columns[0:1],
					},
				},
			},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "id can only have one index")
	})

	t.Run("CREATE TABLE with TEXT unique key should fail", func(t *testing.T) {
		columns := []Column{
			{
				Name: "id",
				Kind: Text,
			},
		}
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   columns,
			UniqueIndexes: []UniqueIndex{
				{
					IndexInfo: IndexInfo{
						Columns: columns[0:1],
					},
				},
			},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "unique index key cannot be of type TEXT")
	})

	t.Run("CREATE TABLE with VARCHAR unique key exceeding max size should fail", func(t *testing.T) {
		columns := []Column{
			{
				Name: "id",
				Kind: Varchar,
				Size: 300,
			},
		}
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   columns,
			UniqueIndexes: []UniqueIndex{
				{
					IndexInfo: IndexInfo{
						Columns: columns[0:1],
					},
				},
			},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, fmt.Sprintf("unique index key size exceeds max index key size %d", MaxIndexKeySize))
	})

	t.Run("CREATE TABLE with duplicate column names should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   []Column{{Name: "id", Kind: Int8, Size: 8}, {Name: "id", Kind: Int8, Size: 8}},
		}

		err := stmt.Validate(nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "duplicate column name")
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
			Kind:       CreateTable,
			TableName:  testTableName,
			Columns:    columns[0:2],
			PrimaryKey: NewPrimaryKey("foo", columns[0:1], false),
		}

		err := stmt.Validate(nil)
		require.NoError(t, err)
	})

	t.Run("INSERT with wrong number of columns should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns[1:], // Missing the "id" column
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

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, "insert: expected 4 columns, got 3")
	})

	t.Run("INSERT with missing required field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "id"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
				},
			},
		}

		err := stmt.Validate(table)
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
			TableName: table.Name,
			Columns:   table.Columns,
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

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "id" cannot be NULL`)
	})

	t.Run("INSERT with unbound placeholder should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "age"}, {Name: "verified"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
					{Value: int32(25), Valid: true},
					{Value: Placeholder{}, Valid: true},
				},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unbound placeholder in value for field "verified"`)
	})

	t.Run("INSERT with NULL for nullable column should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns,
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

		err := stmt.Validate(table)
		require.NoError(t, err)
	})

	t.Run("INSERT with valid values should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns,
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

		err := stmt.Validate(table)
		require.NoError(t, err)
	})

	t.Run("INSERT with unknown field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "bogus"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer([]byte("test@example.com")), Valid: true},
					{Value: int32(25), Valid: true},
				},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "bogus" in table "test_table"`)
	})

	t.Run("INSERT with invalid UTF-8 string should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns,
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

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `expects valid UTF-8 string for "email"`)
	})

	t.Run("INSERT with text exceeding maximum VARCHAR length should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			Inserts: [][]OptionalValue{
				{
					{Value: int32(1), Valid: true},
					{Value: NewTextPointer(bytes.Repeat([]byte{'a'}, MaxInlineVarchar+1)), Valid: true},
				},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, fmt.Sprintf(`field "email" exceeds maximum VARCHAR length of %d`, MaxInlineVarchar))
	})

	t.Run("UPDATE with unknown field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "unknown_field"}},
			Updates: map[string]OptionalValue{
				"unknown_field": {Valid: false},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "unknown_field" in table "test_table"`)
	})

	t.Run("UPDATE with invalid UTF-8 string should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "email"}},
			Updates: map[string]OptionalValue{
				"email": {Value: NewTextPointer([]byte{0xff, 0xfe, 0xfd}), Valid: true}, // invalid UTF-8,
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `expects valid UTF-8 string for "email"`)
	})

	t.Run("UPDATE with NULL to non-nullable column should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "email"}},
			Updates: map[string]OptionalValue{
				"email": {}, // NULL for non-nullable email
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "email" cannot be NULL`)
	})

	t.Run("UPDATE with unbound placeholder should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "age"}},
			Updates: map[string]OptionalValue{
				"age": {Value: Placeholder{}, Valid: true}, // ? (unbound placeholder)
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unbound placeholder in value for field "age"`)
	})

	t.Run("UPDATE with NULL to nullable column should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "age"}},
			Updates: map[string]OptionalValue{
				"age": {Valid: false}, // NULL for nullable age
			},
		}

		err := stmt.Validate(table)
		require.NoError(t, err)
	})

	t.Run("UPDATE with valid value should succeed", func(t *testing.T) {
		stmt := Statement{
			Kind:      Update,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "email"}, {Name: "age"}},
			Updates: map[string]OptionalValue{
				"email": {Value: NewTextPointer([]byte("new@example.com")), Valid: true},
				"age":   {Value: int32(30), Valid: true},
			},
		}

		err := stmt.Validate(table)
		require.NoError(t, err)
	})

	t.Run("SELECT with no fields should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{}, // No fields specified
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `at least one field to select is required`)
	})

	t.Run("SELECT with duplicate should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}, {Name: "id"}},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `duplicate field "id" in select statement`)
	})

	t.Run("SELECT with unknown field should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "unknown_field"}},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "unknown_field" in table "test_table"`)
	})

	t.Run("SELECT with unknown field in ORDER BY should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			OrderBy: []OrderBy{
				{
					Field: Field{Name: "unknown_field"},
				},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unknown field "unknown_field" in ORDER BY clause`)
	})

	t.Run("SELECT COUNT(*) with ORDER BY should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "COUNT(*)"}},
			OrderBy: []OrderBy{
				{
					Field: Field{Name: "id"},
				},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `ORDER BY cannot be used with COUNT(*)`)
	})

	t.Run("SELECT COUNT(*) with OFFSET should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "COUNT(*)"}},
			Offset:    OptionalValue{Value: int64(100), Valid: true},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `OFFSET cannot be used with COUNT(*)`)
	})

	t.Run("SELECT COUNT(*) with LIMIT should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "COUNT(*)"}},
			Limit:     OptionalValue{Value: int64(100), Valid: true},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `LIMIT cannot be used with COUNT(*)`)
	})

	t.Run("SELECT with invalid LIMIT should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			Limit:     OptionalValue{Value: int64(-5), Valid: true},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `LIMIT must be a non-negative integer`)
	})

	t.Run("SELECT with duplicate table name in JOIN should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			TableName:  table.Name,
			TableAlias: "t1",
			Joins: []Join{
				{
					TableName:  table.Name, // Duplicate table name
					TableAlias: "t2",
					Conditions: Conditions{
						{
							Operand1: Operand{
								Type:  OperandField,
								Value: Field{AliasPrefix: "t1", Name: "id"},
							},
							Operator: Eq,
							Operand2: Operand{
								Type:  OperandField,
								Value: Field{AliasPrefix: "t2", Name: "other_id"},
							},
						},
					},
				},
			},
			Columns: table.Columns,
			Fields:  []Field{{Name: "id"}, {Name: "email"}},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `duplicate table name "test_table" in JOINs`)
	})

	t.Run("SELECT with duplicate table alias in JOIN should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			TableName:  table.Name,
			TableAlias: "t1",
			Joins: []Join{
				{
					TableName:  "other_table",
					TableAlias: "t1", // Duplicate alias
					Conditions: Conditions{
						{
							Operand1: Operand{
								Type:  OperandField,
								Value: Field{AliasPrefix: "t1", Name: "id"},
							},
							Operator: Eq,
							Operand2: Operand{
								Type:  OperandField,
								Value: Field{AliasPrefix: "t2", Name: "other_id"},
							},
						},
					},
				},
			},
			Columns: table.Columns,
			Fields:  []Field{{Name: "id"}, {Name: "email"}},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `duplicate table alias "t1" in JOINs`)
	})

	t.Run("SELECT with invalid offset should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    []Field{{Name: "id"}, {Name: "email"}},
			Offset:    OptionalValue{Value: int64(-5), Valid: true},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `OFFSET must be a non-negative integer`)
	})

	t.Run("SELECT with non field left operand should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandInteger,
							Value: int64(1),
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
						},
					},
				},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `operand1 in WHERE condition must be a field`)
	})

	t.Run("SELECT with unbound placeholder should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
						},
						Operator: Eq,
						Operand2: Operand{
							Type: OperandPlaceholder,
						},
					},
				},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unbound placeholder in WHERE clause`)
	})

	t.Run("SELECT with unbound placeholder in list condition should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
						},
						Operator: NotIn,
						Operand2: Operand{
							Type: OperandList,
							Value: []any{
								int64(1),
								Placeholder{},
							},
						},
					},
				},
			},
		}

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `unbound placeholder in WHERE clause`)
	})

	t.Run("SELECT with inconsistent argument list for IN should fail", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		err := stmt.Validate(table)
		require.Error(t, err)
		assert.ErrorContains(t, err, `mixed operand types in WHERE condition list`)
	})

	t.Run("SELECT with same-field equality conditions in one AND group is valid (unsatisfiable but not an error)", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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
							Value: Field{Name: "id"},
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

		// Contradictory equality conditions in one AND group are unsatisfiable but
		// not a validation error — they naturally return zero rows when evaluated.
		// DNF expansion of nested WHERE clauses can legitimately produce such groups.
		err := stmt.Validate(table)
		require.NoError(t, err)
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
			`expects BOOLEAN value for "foo"`,
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
			`expects INT4 value for "foo"`,
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
			`expects INT8 value for "foo"`,
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
			`expects REAL value for "foo"`,
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
			`expects DOUBLE value for "foo"`,
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
			`expects a text value for "foo"`,
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
			`expects a text value for "foo"`,
		},
		{
			"valid VARCHAR value",
			Column{Kind: Varchar, Size: 100, Name: "foo"},
			OptionalValue{Value: NewTextPointer([]byte("some text")), Valid: true},
			"",
		},
		{
			"invalid TIMESTAMP value",
			Column{Kind: Timestamp, Name: "foo"},
			OptionalValue{Value: int32(25), Valid: true},
			`expects timestamp value for "foo"`,
		},
		{
			"valid TIMESTAMP value",
			Column{Kind: Timestamp, Name: "foo"},
			OptionalValue{Value: MustParseTimestampMicros("2000-01-01 00:00:00"), Valid: true},
			"",
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.name, func(t *testing.T) {
			stmt := Statement{}
			err := stmt.validateColumnValue(&Table{}, aTestCase.column, aTestCase.insertValue)
			if aTestCase.err == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, aTestCase.err)
		})
	}
}

func TestStatement_DDL(t *testing.T) {
	t.Parallel()

	t.Run("create table with all data types options", func(t *testing.T) {
		columns := []Column{
			{
				Kind: Int4,
				Size: 4,
				Name: "a",
			},
			{
				Kind: Int8,
				Size: 8,
				Name: "b",
			},
			{
				Kind: Varchar,
				Size: 255,
				Name: "c",
			},
			{
				Kind: Text,
				Name: "d",
			},
			{
				Kind:         Boolean,
				Size:         1,
				Name:         "e",
				Nullable:     false,
				DefaultValue: OptionalValue{Value: false, Valid: true},
			},
			{
				Kind:     Real,
				Size:     4,
				Name:     "f",
				Nullable: true,
			},
			{
				Kind:     Real,
				Size:     4,
				Name:     "g",
				Nullable: true,
			},
			{
				Kind: Timestamp,
				Size: 8,
				Name: "h",
			},
			{
				Kind:            Timestamp,
				Size:            8,
				Name:            "i",
				Nullable:        true,
				DefaultValueNow: true,
			},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  "users",
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("pk_users", columns[0:1], true),
			UniqueIndexes: []UniqueIndex{
				{
					IndexInfo: IndexInfo{
						Columns: columns[2:3],
					},
				},
			},
		}

		expected := `create table "users" (a int4 primary key autoincrement, b int8 not null, c varchar(255) not null unique, d text not null, e boolean not null default false, f real, g real, h timestamp not null, i timestamp default now());`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create table with special characters in name", func(t *testing.T) {
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

		expected := `create table "test_table_123" (column_with_underscore int4 not null);`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create table with composite primary key", func(t *testing.T) {
		columns := []Column{
			{
				Kind:     Int4,
				Size:     4,
				Name:     "bar",
				Nullable: false,
			},
			{
				Kind:     Int4,
				Size:     4,
				Name:     "baz",
				Nullable: false,
			},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  "foo",
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("pk__foo", columns[0:2], false),
		}

		expected := `create table "foo" (bar int4 not null, baz int4 not null, primary key (bar, baz));`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create table with composite unique index", func(t *testing.T) {
		columns := []Column{
			{
				Kind:     Int4,
				Size:     4,
				Name:     "bar",
				Nullable: false,
			},
			{
				Kind:     Int4,
				Size:     4,
				Name:     "baz",
				Nullable: false,
			},
		}
		stmt := Statement{
			Kind:      CreateTable,
			TableName: "foo",
			Columns:   columns,
			UniqueIndexes: []UniqueIndex{
				{
					IndexInfo: IndexInfo{
						Name:    "key__foo",
						Columns: columns[0:2],
					},
				},
			},
		}

		expected := `create table "foo" (bar int4 not null, baz int4 not null, unique (bar, baz));`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create table with multiple composite unique indexes", func(t *testing.T) {
		columns := []Column{
			{
				Kind:     Int4,
				Size:     4,
				Name:     "bar",
				Nullable: false,
			},
			{
				Kind:     Int4,
				Size:     4,
				Name:     "baz",
				Nullable: false,
			},
			{
				Kind:     Varchar,
				Size:     100,
				Name:     "lorem",
				Nullable: false,
			},
			{
				Kind:     Varchar,
				Size:     100,
				Name:     "ipsum",
				Nullable: false,
			},
		}
		stmt := Statement{
			Kind:      CreateTable,
			TableName: "foo",
			Columns:   columns,
			UniqueIndexes: []UniqueIndex{
				{
					IndexInfo: IndexInfo{
						Columns: columns[0:2],
					},
				},
				{
					IndexInfo: IndexInfo{
						Columns: columns[2:4],
					},
				},
			},
		}

		expected := `create table "foo" (bar int4 not null, baz int4 not null, lorem varchar(100) not null, ipsum varchar(100) not null, unique (bar, baz), unique (lorem, ipsum));`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create index single column", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateIndex,
			IndexName: "idx_users_on_foo",
			TableName: "users",
			Columns: []Column{
				{
					Kind:     Int4,
					Size:     4,
					Name:     "foo",
					Nullable: false,
				},
			},
		}

		expected := `create index "idx_users_on_foo" on "users" (foo);`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create index multiple columns", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateIndex,
			IndexName: "idx_users_on_foo_bar",
			TableName: "users",
			Columns: []Column{
				{
					Name: "foo",
				},
				{
					Name: "bar",
				},
			},
		}

		expected := `create index "idx_users_on_foo_bar" on "users" (foo, bar);`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create fulltext index with tokenizer", func(t *testing.T) {
		stmt := Statement{
			Kind:           CreateIndex,
			IndexMethod:    IndexMethodFullText,
			IndexName:      "idx_articles_body",
			IndexTokenizer: TextSearchTokenizerSimple,
			TableName:      "articles",
			Columns: []Column{
				{Name: "body"},
			},
		}

		expected := `create fulltext index "idx_articles_body" on "articles" (body) with (tokenizer = 'simple');`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create inverted index", func(t *testing.T) {
		stmt := Statement{
			Kind:        CreateIndex,
			IndexMethod: IndexMethodInverted,
			IndexName:   "idx_events_payload",
			TableName:   "events",
			Columns: []Column{
				{Name: "payload"},
			},
		}

		expected := `create inverted index "idx_events_payload" on "events" (payload);`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create table with single foreign key (default restrict)", func(t *testing.T) {
		columns := []Column{
			{Kind: Int8, Size: 8, Name: "id"},
			{Kind: Int8, Size: 8, Name: "user_id", Nullable: false},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  "orders",
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("pk__orders", columns[0:1], true),
			ForeignKeys: []ForeignKey{
				{
					Name:          "fk__orders__users__user_id",
					Columns:       []string{"user_id"},
					TargetTable:   "users",
					TargetColumns: []string{"id"},
					OnDelete:      FKActionRestrict,
					OnUpdate:      FKActionRestrict,
				},
			},
		}

		expected := `create table "orders" (id int8 primary key autoincrement, user_id int8 not null, constraint "fk__orders__users__user_id" foreign key ("user_id") references "users" ("id") on delete restrict on update restrict);`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create table with multi-column foreign key", func(t *testing.T) {
		columns := []Column{
			{Kind: Int8, Size: 8, Name: "id"},
			{Kind: Int8, Size: 8, Name: "order_id", Nullable: false},
			{Kind: Int8, Size: 8, Name: "product_id", Nullable: false},
			{Kind: Int8, Size: 8, Name: "shipped", Nullable: false},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  "shipment_lines",
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("pk__shipment_lines", columns[0:1], true),
			ForeignKeys: []ForeignKey{
				{
					Name:          "fk__shipment_lines__order_lines__order_id_product_id",
					Columns:       []string{"order_id", "product_id"},
					TargetTable:   "order_lines",
					TargetColumns: []string{"order_id", "product_id"},
					OnDelete:      FKActionCascade,
					OnUpdate:      FKActionRestrict,
				},
			},
		}

		expected := `create table "shipment_lines" (id int8 primary key autoincrement, order_id int8 not null, product_id int8 not null, shipped int8 not null, constraint "fk__shipment_lines__order_lines__order_id_product_id" foreign key ("order_id", "product_id") references "order_lines" ("order_id", "product_id") on delete cascade on update restrict);`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})

	t.Run("create table with multiple foreign keys", func(t *testing.T) {
		columns := []Column{
			{Kind: Int8, Size: 8, Name: "id"},
			{Kind: Int8, Size: 8, Name: "order_id", Nullable: false},
			{Kind: Int8, Size: 8, Name: "product_id", Nullable: false},
		}
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  "order_items",
			Columns:    columns,
			PrimaryKey: NewPrimaryKey("pk__order_items", columns[0:1], true),
			ForeignKeys: []ForeignKey{
				{
					Name:          "fk__order_items__orders__order_id",
					Columns:       []string{"order_id"},
					TargetTable:   "orders",
					TargetColumns: []string{"id"},
					OnDelete:      FKActionRestrict,
					OnUpdate:      FKActionRestrict,
				},
				{
					Name:          "fk__order_items__products__product_id",
					Columns:       []string{"product_id"},
					TargetTable:   "products",
					TargetColumns: []string{"id"},
					OnDelete:      FKActionNoAction,
					OnUpdate:      FKActionNoAction,
				},
			},
		}

		expected := `create table "order_items" (id int8 primary key autoincrement, order_id int8 not null, product_id int8 not null, constraint "fk__order_items__orders__order_id" foreign key ("order_id") references "orders" ("id") on delete restrict on update restrict, constraint "fk__order_items__products__product_id" foreign key ("product_id") references "products" ("id") on delete no action on update no action);`

		actual := stmt.DDL()
		assert.Equal(t, expected, actual)
	})
}

func TestStatement_InsertValuesForColumns(t *testing.T) {
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

	t.Run("invalid insert index", func(t *testing.T) {
		vals := stmt.InsertValuesForColumns(5, stmt.Columns[1])
		assert.Empty(t, vals)
	})

	t.Run("value for single column", func(t *testing.T) {
		vals := stmt.InsertValuesForColumns(0, stmt.Columns[1])
		assert.Equal(t, []OptionalValue{
			{Value: "john@example.com", Valid: true},
		}, vals)
	})

	t.Run("value for two column", func(t *testing.T) {
		vals := stmt.InsertValuesForColumns(1, stmt.Columns[0], stmt.Columns[1])
		assert.Equal(t, []OptionalValue{
			{Value: int32(2), Valid: true},
			{Value: "jane@example.com", Valid: true},
		}, vals)
	})
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

func TestOperandTypeFromAny(t *testing.T) {
	t.Parallel()

	assert.Equal(t, OperandInteger, operandTypeFromAny(int64(1)))
	assert.Equal(t, OperandInteger, operandTypeFromAny(int32(1)))
	assert.Equal(t, OperandFloat, operandTypeFromAny(float64(1.0)))
	assert.Equal(t, OperandFloat, operandTypeFromAny(float32(1.0)))
	assert.Equal(t, OperandBoolean, operandTypeFromAny(true))
	assert.Equal(t, OperandBoolean, operandTypeFromAny(false))
	assert.Equal(t, OperandQuotedString, operandTypeFromAny("hello"))
	assert.Equal(t, OperandQuotedString, operandTypeFromAny(NewTextPointer([]byte("hi"))))
	assert.Equal(t, OperandType(0), operandTypeFromAny(nil))
	assert.Equal(t, OperandType(0), operandTypeFromAny([]byte{1, 2}))
}

func TestStatement_PrepareWhere(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Kind: Int8, Name: "id", Size: 8},
		{Kind: Timestamp, Name: "created", Size: 8},
	}

	t.Run("non-timestamp condition is unchanged", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: "t",
			Columns:   cols,
			Fields:    []Field{{Name: "id"}},
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
						Operator: Eq,
						Operand2: Operand{Type: OperandInteger, Value: int64(42)},
					},
				},
			},
		}
		result, err := stmt.prepareWhere()
		require.NoError(t, err)
		assert.Equal(t, int64(42), result.Conditions[0][0].Operand2.Value)
	})

	t.Run("timestamp condition value is parsed from TextPointer", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: "t",
			Columns:   cols,
			Fields:    []Field{{Name: "created"}},
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "created"}},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: NewTextPointer([]byte("2024-01-15 10:30:00")),
						},
					},
				},
			},
		}
		result, err := stmt.prepareWhere()
		require.NoError(t, err)
		_, ok := result.Conditions[0][0].Operand2.Value.(TimestampMicros)
		assert.True(t, ok, "expected TimestampMicros value after prepareWhere")
	})

	t.Run("unknown field returns error", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: "t",
			Columns:   cols,
			Fields:    []Field{{Name: "id"}},
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "bogus"}},
						Operator: Eq,
						Operand2: Operand{Type: OperandInteger, Value: int64(1)},
					},
				},
			},
		}
		_, err := stmt.prepareWhere()
		assert.Error(t, err)
	})

	t.Run("IS NULL on timestamp is allowed without parsing", func(t *testing.T) {
		stmt := Statement{
			Kind:      Select,
			TableName: "t",
			Columns:   cols,
			Fields:    []Field{{Name: "created"}},
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "created"}},
						Operator: Eq,
						Operand2: Operand{Type: OperandNull},
					},
				},
			},
		}
		result, err := stmt.prepareWhere()
		require.NoError(t, err)
		assert.Equal(t, OperandNull, result.Conditions[0][0].Operand2.Type)
	})
}

func TestStatement_IsSelectGroupBy(t *testing.T) {
	t.Parallel()

	t.Run("true when GroupBy is populated", func(t *testing.T) {
		stmt := Statement{
			Kind:    Select,
			GroupBy: []Field{{Name: "category"}},
		}
		assert.True(t, stmt.IsSelectGroupBy())
	})

	t.Run("false when GroupBy is empty", func(t *testing.T) {
		stmt := Statement{Kind: Select}
		assert.False(t, stmt.IsSelectGroupBy())
	})
}

func TestStatement_ValidateHaving(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Kind: Int8, Name: "id", Size: 8},
		{Kind: Varchar, Name: "category", Size: MaxInlineVarchar},
		{Kind: Double, Name: "price", Size: 8},
	}
	tbl := NewTable(zap.NewNop(), nil, nil, "products", cols, 0, nil)

	havingCond := func(fieldName string) OneOrMore {
		return OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: fieldName}},
					Operator: Gt,
					Operand2: Operand{Type: OperandFloat, Value: float64(100)},
				},
			},
		}
	}

	t.Run("HAVING with aggregate function is valid", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "category"}, {Name: "SUM(price)"}},
			Aggregates: []AggregateExpr{{}, {Kind: AggregateSum, Column: "price"}},
			GroupBy:    []Field{{Name: "category"}},
			Having:     havingCond("SUM(price)"),
		}
		err := stmt.Validate(tbl)
		require.NoError(t, err)
	})

	t.Run("HAVING with COUNT(*) is valid", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "category"}, {Name: "COUNT(*)"}},
			Aggregates: []AggregateExpr{{}, {Kind: AggregateCount}},
			GroupBy:    []Field{{Name: "category"}},
			Having:     havingCond("COUNT(*)"),
		}
		err := stmt.Validate(tbl)
		require.NoError(t, err)
	})

	t.Run("HAVING referencing GROUP BY column is valid", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "category"}, {Name: "SUM(price)"}},
			Aggregates: []AggregateExpr{{}, {Kind: AggregateSum, Column: "price"}},
			GroupBy:    []Field{{Name: "category"}},
			Having:     havingCond("category"),
		}
		err := stmt.Validate(tbl)
		require.NoError(t, err)
	})

	t.Run("HAVING referencing non-GROUP BY plain column is rejected", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "category"}, {Name: "SUM(price)"}},
			Aggregates: []AggregateExpr{{}, {Kind: AggregateSum, Column: "price"}},
			GroupBy:    []Field{{Name: "category"}},
			// "price" is not in GROUP BY and not an aggregate reference
			Having: havingCond("price"),
		}
		err := stmt.Validate(tbl)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"price"`)
	})

	t.Run("HAVING with AVG is valid", func(t *testing.T) {
		stmt := Statement{
			Kind:       Select,
			Fields:     []Field{{Name: "category"}, {Name: "AVG(price)"}},
			Aggregates: []AggregateExpr{{}, {Kind: AggregateAvg, Column: "price"}},
			GroupBy:    []Field{{Name: "category"}},
			Having:     havingCond("AVG(price)"),
		}
		err := stmt.Validate(tbl)
		require.NoError(t, err)
	})
}

func TestAggregateKind_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind AggregateKind
		want string
	}{
		{AggregateCount, "COUNT"},
		{AggregateSum, "SUM"},
		{AggregateAvg, "AVG"},
		{AggregateMin, "MIN"},
		{AggregateMax, "MAX"},
		{AggregateKind(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.kind.String())
	}
}

func TestColumnKind_IsInt(t *testing.T) {
	t.Parallel()

	assert.True(t, Int4.IsInt())
	assert.True(t, Int8.IsInt())
	assert.False(t, Boolean.IsInt())
	assert.False(t, Varchar.IsInt())
	assert.False(t, Text.IsInt())
	assert.False(t, Real.IsInt())
	assert.False(t, Timestamp.IsInt())
}

func TestField_OutputName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "name", Field{Name: "name"}.OutputName())
	assert.Equal(t, "label", Field{Name: "name", Alias: "label"}.OutputName())
	// Expr with alias uses the alias.
	assert.Equal(t, "expr_alias", Field{Expr: &Expr{Literal: int64(1)}, Alias: "expr_alias"}.OutputName())
	// Expr without alias uses Expr.String().
	e := &Expr{Literal: int64(42)}
	assert.Equal(t, e.String(), Field{Expr: e}.OutputName())
}

func TestDirection_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ASC", Asc.String())
	assert.Equal(t, "DESC", Desc.String())
	assert.Equal(t, "UNKNOWN", Direction(99).String())
}

func TestStatement_IsDDL(t *testing.T) {
	t.Parallel()

	assert.True(t, Statement{Kind: CreateTable}.IsDDL())
	assert.True(t, Statement{Kind: DropTable}.IsDDL())
	assert.True(t, Statement{Kind: CreateIndex}.IsDDL())
	assert.True(t, Statement{Kind: DropIndex}.IsDDL())
	assert.False(t, Statement{Kind: Select}.IsDDL())
	assert.False(t, Statement{Kind: Insert}.IsDDL())
	assert.False(t, Statement{Kind: Update}.IsDDL())
	assert.False(t, Statement{Kind: Delete}.IsDDL())
}

func TestStatement_ValidatePragma(t *testing.T) {
	t.Parallel()

	require.NoError(t, Statement{PragmaName: "foreign_keys"}.validatePragma())
	require.Error(t, Statement{}.validatePragma())
}

func TestIterator_Close(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rows := []Row{{}, {}, {}}
	it := NewSliceIterator(rows)

	require.True(t, it.Next(ctx))
	require.NoError(t, it.Close())
	// After Close, Next must return false.
	assert.False(t, it.Next(ctx))
}

func TestStatementKind_String(t *testing.T) {
	t.Parallel()

	cases := []struct {
		kind StatementKind
		want string
	}{
		{CreateTable, "CREATE TABLE"},
		{DropTable, "DROP TABLE"},
		{CreateIndex, "CREATE INDEX"},
		{DropIndex, "DROP INDEX"},
		{Insert, "INSERT"},
		{Select, "SELECT"},
		{Update, "UPDATE"},
		{Delete, "DELETE"},
		{BeginTransaction, "BEGIN TRANSACTION"},
		{CommitTransaction, "COMMIT TRANSACTION"},
		{RollbackTransaction, "ROLLBACK TRANSACTION"},
		{Analyze, "ANALYZE"},
		{Vacuum, "VACUUM"},
		{Pragma, "PRAGMA"},
		{Explain, "EXPLAIN"},
		{StatementKind(999), "UNKNOWN"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, tc.kind.String())
	}
}
