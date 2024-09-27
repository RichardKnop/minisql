package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmpty(t *testing.T) {
	t.Parallel()

	aStatement, err := NewParser("").Parse(context.Background())
	require.Error(t, err)
	assert.Equal(t, Statement{}, aStatement)
	assert.Equal(t, errEmptyStatementKind, err)
}

func TestCreateTable(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name     string
		SQL      string
		Expected Statement
		Err      error
	}{
		{
			Name:     "Empty CREATE TABLE fails",
			SQL:      "CREATE TABLE",
			Expected: Statement{Kind: CreateTable},
			Err:      errEmptyTableName,
		},
		{
			Name: "CREATE TABLE with no opening parens fails",
			SQL:  "CREATE TABLE foo",
			Expected: Statement{
				Kind:      CreateTable,
				TableName: "foo",
			},
			Err: errCreateTableNoColumns,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := NewParser(aTestCase.SQL)
			aStatement, err := aParser.Parse(context.Background())
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aStatement)
		})
	}
}

func TestInsert(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name     string
		SQL      string
		Expected Statement
		Err      error
	}{
		{
			Name:     "Empty INSERT fails",
			SQL:      "INSERT INTO",
			Expected: Statement{Kind: Insert},
			Err:      errEmptyTableName,
		},
		{
			Name: "INSERT with no rows to insert fails",
			SQL:  "INSERT INTO 'a'",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete value section fails",
			SQL:  "INSERT INTO 'a' (",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete value section fails #2",
			SQL:  "INSERT INTO 'a' (b",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
				Fields:    []string{"b"},
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete value section fails #3",
			SQL:  "INSERT INTO 'a' (b)",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
				Fields:    []string{"b"},
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete value section fails #4",
			SQL:  "INSERT INTO 'a' (b) VALUES",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
				Fields:    []string{"b"},
			},
			Err: errNoRowsToInsert,
		},
		{
			Name: "INSERT with incomplete row fails",
			SQL:  "INSERT INTO 'a' (b) VALUES (",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
				Fields:    []string{"b"},
				Inserts:   [][]string{{}},
			},
			Err: errInsertFieldValueCountMismatch,
		},
		{
			Name: "INSERT works",
			SQL:  "INSERT INTO 'a' (b) VALUES ('1')",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
				Fields:    []string{"b"},
				Inserts:   [][]string{{"1"}},
			},
		},
		{
			Name: "INSERT * fails",
			SQL:  "INSERT INTO 'a' (*) VALUES ('1')",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
			},
			Err: errInsertNoFields,
		},
		{
			Name: "INSERT with multiple fields works",
			SQL:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' )",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}},
			},
		},
		{
			Name: "INSERT with multiple fields and multiple values works",
			SQL:  "INSERT INTO 'a' (b,c,    d) VALUES ('1','2' ,  '3' ),('4','5' ,'6' )",
			Expected: Statement{
				Kind:      Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
			},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := NewParser(aTestCase.SQL)
			aStatement, err := aParser.Parse(context.Background())
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aStatement)
		})
	}
}

func TestSelect(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name     string
		SQL      string
		Expected Statement
		Err      error
	}{
		{
			Name:     "SELECT without FROM fails",
			SQL:      "SELECT",
			Expected: Statement{Kind: Select},
			Err:      errEmptyTableName,
		},
		{
			Name:     "SELECT without fields fails",
			SQL:      "SELECT FROM 'a'",
			Expected: Statement{Kind: Select},
			Err:      errSelectWithoutFields,
		},
		{
			Name: "SELECT with comma and empty field fails",
			SQL:  "SELECT b, FROM 'a'",
			Expected: Statement{
				Kind:   Select,
				Fields: []string{"b"},
			},
			Err: errSelectWithoutFields,
		},
		{
			Name: "SELECT works",
			SQL:  "SELECT a FROM 'b'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a"},
			},
			Err: nil,
		},
		{
			Name: "SELECT works with lowercase",
			SQL:  "select a fRoM 'b'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a"},
			},
			Err: nil,
		},
		{
			Name: "SELECT many fields works",
			SQL:  "SELECT a, c, d FROM 'b'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
			},
			Err: nil,
		},
		{
			Name: "SELECT with alias works",
			SQL:  "SELECT a as z, b as y, c FROM 'b'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "b", "c"},
				Aliases: map[string]string{
					"a": "z",
					"b": "y",
				},
			},
			Err: nil,
		},

		{
			Name: "SELECT with empty WHERE fails",
			SQL:  "SELECT a, c, d FROM 'b' WHERE",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
			},
			Err: errEmptyWhereClause,
		},
		{
			Name: "SELECT with WHERE with only operand fails",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
					},
				},
			},
			Err: errWhereWithoutOperator,
		},
		{
			Name: "SELECT with WHERE with = works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a = ''",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with < works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a < '1'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Lt,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with <= works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a <= '1'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Lte,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with > works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a > '1'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Gt,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with >= works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a >= '1'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Gte,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with != works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Ne,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "SELECT with WHERE with != works (comparing field against another field)",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != b",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Ne,
						Operand2:        "b",
						Operand2IsField: true,
					},
				},
			},
		},
		{
			Name: "SELECT * works",
			SQL:  "SELECT * FROM 'b'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"*"},
			},
		},
		{
			Name: "SELECT a, * works",
			SQL:  "SELECT a, * FROM 'b'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "*"},
			},
		},
		{
			Name: "SELECT with WHERE with two conditions using AND works",
			SQL:  "SELECT a, c, d FROM 'b' WHERE a != '1' AND b = '2'",
			Expected: Statement{
				Kind:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Ne,
						Operand2:        "1",
						Operand2IsField: false,
					},
					{
						Operand1:        "b",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "2",
						Operand2IsField: false,
					},
				},
			},
			Err: nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := NewParser(aTestCase.SQL)
			aStatement, err := aParser.Parse(context.Background())
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aStatement)
		})
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name     string
		SQL      string
		Expected Statement
		Err      error
	}{
		{
			Name:     "Empty UPDATE fails",
			SQL:      "UPDATE",
			Expected: Statement{Kind: Update},
			Err:      errEmptyTableName,
		},
		{
			Name: "Incomplete UPDATE with table name fails",
			SQL:  "UPDATE 'a'",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
			},
			Err: errWhereRequiredForUpdateDelete,
		},
		{
			Name: "Incomplete UPDATE with table name and SET fails",
			SQL:  "UPDATE 'a' SET",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
			},
			Err: errWhereRequiredForUpdateDelete,
		},
		{
			Name: "Incomplete UPDATE with table name, SET with a field but no value and WHERE fails",
			SQL:  "UPDATE 'a' SET b WHERE",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
			},
			Err: errUpdateExpectedEquals,
		},
		{
			Name: "Incomplete UPDATE with table name, SET with a field and = but no value and WHERE fails",
			SQL:  "UPDATE 'a' SET b = WHERE",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
			},
			Err: errUpdateExpectedQuotedValue,
		},
		{
			Name: "Incomplete UPDATE due to no WHERE clause fails",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
				Updates: map[string]string{
					"b": "hello",
				},
			},
			Err: errEmptyWhereClause,
		},
		{
			Name: "Incomplete UPDATE due incomplete WHERE clause fails",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE a",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
				Updates: map[string]string{
					"b": "hello",
				},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
					},
				},
			},
			Err: errWhereWithoutOperator,
		},
		{
			Name: "UPDATE works",
			SQL:  "UPDATE 'a' SET b = 'hello' WHERE a = '1'",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
				Updates: map[string]string{
					"b": "hello",
				},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "UPDATE works with simple quote inside",
			SQL:  "UPDATE 'a' SET b = 'hello\\'world' WHERE a = '1'",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
				Updates: map[string]string{
					"b": "hello\\'world",
				},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "UPDATE with multiple SETs works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1'",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
				Updates: map[string]string{
					"b": "hello",
					"c": "bye",
				},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "UPDATE with multiple SETs and multiple conditions works",
			SQL:  "UPDATE 'a' SET b = 'hello', c = 'bye' WHERE a = '1' AND b = '789'",
			Expected: Statement{
				Kind:      Update,
				TableName: "a",
				Updates: map[string]string{
					"b": "hello",
					"c": "bye",
				},
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "1",
						Operand2IsField: false,
					},
					{
						Operand1:        "b",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "789",
						Operand2IsField: false,
					},
				},
			},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := NewParser(aTestCase.SQL)
			aStatement, err := aParser.Parse(context.Background())
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aStatement)
		})
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name     string
		SQL      string
		Expected Statement
		Err      error
	}{
		{
			Name:     "Empty DELETE fails",
			SQL:      "DELETE FROM",
			Expected: Statement{Kind: Delete},
			Err:      errEmptyTableName,
		},
		{
			Name: "DELETE without WHERE fails",
			SQL:  "DELETE FROM 'a'",
			Expected: Statement{
				Kind:      Delete,
				TableName: "a",
			},
			Err: errWhereRequiredForUpdateDelete,
		},
		{
			Name: "DELETE with empty WHERE fails",
			SQL:  "DELETE FROM 'a' WHERE",
			Expected: Statement{
				Kind:      Delete,
				TableName: "a",
			},
			Err: errEmptyWhereClause,
		},
		{
			Name: "DELETE with WHERE with field but no operator fails",
			SQL:  "DELETE FROM 'a' WHERE b",
			Expected: Statement{
				Kind:      Delete,
				TableName: "a",
				Conditions: []Condition{
					{
						Operand1:        "b",
						Operand1IsField: true,
					},
				},
			},
			Err: errWhereWithoutOperator,
		},
		{
			Name: "DELETE with WHERE works",
			SQL:  "DELETE FROM 'a' WHERE b = '1'",
			Expected: Statement{
				Kind:      Delete,
				TableName: "a",
				Conditions: []Condition{
					{
						Operand1:        "b",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "1",
						Operand2IsField: false,
					},
				},
			},
		},
		{
			Name: "DELETE with multiple conditions works",
			SQL:  "DELETE FROM 'a' WHERE a = '1' AND b = '789'",
			Expected: Statement{
				Kind:      Delete,
				TableName: "a",
				Conditions: []Condition{
					{
						Operand1:        "a",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "1",
						Operand2IsField: false,
					},
					{
						Operand1:        "b",
						Operand1IsField: true,
						Operator:        Eq,
						Operand2:        "789",
						Operand2IsField: false,
					},
				},
			},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := NewParser(aTestCase.SQL)
			aStatement, err := aParser.Parse(context.Background())
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aStatement)
		})
	}
}
