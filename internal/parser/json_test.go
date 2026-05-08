package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_JSONColumn(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"CREATE TABLE with json column",
			"CREATE TABLE events (id int8, payload json);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "events",
					Columns: []minisql.Column{
						{Name: "id", Kind: minisql.Int8, Size: 8, Nullable: true},
						{Name: "payload", Kind: minisql.JSON, Nullable: true},
					},
				},
			},
			nil,
		},
		{
			"CREATE TABLE json column not null",
			"CREATE TABLE logs (data json not null);",
			[]minisql.Statement{
				{
					Kind:      minisql.CreateTable,
					TableName: "logs",
					Columns: []minisql.Column{
						{Name: "data", Kind: minisql.JSON, Nullable: false},
					},
				},
			},
			nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			t.Parallel()
			got, err := New().Parse(context.Background(), aTestCase.SQL)
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, got)
		})
	}
}

func TestParse_JSONOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		wantOp  minisql.ArithOp
		wantCol string
	}{
		{
			name:    "arrow operator -> string key",
			sql:     "SELECT payload -> 'name' FROM events;",
			wantOp:  minisql.JSONArrow,
			wantCol: "payload",
		},
		{
			name:    "arrow arrow operator ->> string key",
			sql:     "SELECT payload ->> 'name' FROM events;",
			wantOp:  minisql.JSONArrowArrow,
			wantCol: "payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := New().Parse(context.Background(), tt.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.Len(t, stmts[0].Fields, 1)
			expr := stmts[0].Fields[0].Expr
			require.NotNil(t, expr)
			assert.Equal(t, tt.wantOp, expr.Op)
			require.NotNil(t, expr.Left)
			assert.Equal(t, tt.wantCol, expr.Left.Column)
		})
	}
}

func TestParse_JSONFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sql      string
		funcName string
		argCount int
	}{
		{
			name:     "JSON_EXTRACT two args",
			sql:      "SELECT JSON_EXTRACT(payload, '$.name') FROM events;",
			funcName: "JSON_EXTRACT",
			argCount: 2,
		},
		{
			name:     "JSON_VALID one arg",
			sql:      "SELECT JSON_VALID(payload) FROM events;",
			funcName: "JSON_VALID",
			argCount: 1,
		},
		{
			name:     "JSON_TYPE one arg",
			sql:      "SELECT JSON_TYPE(payload) FROM events;",
			funcName: "JSON_TYPE",
			argCount: 1,
		},
		{
			name:     "JSON_ARRAY_LENGTH one arg",
			sql:      "SELECT JSON_ARRAY_LENGTH(payload) FROM events;",
			funcName: "JSON_ARRAY_LENGTH",
			argCount: 1,
		},
		{
			name:     "JSON_TYPE with path",
			sql:      "SELECT JSON_TYPE(payload, '$.tags') FROM events;",
			funcName: "JSON_TYPE",
			argCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := New().Parse(context.Background(), tt.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.Len(t, stmts[0].Fields, 1)
			expr := stmts[0].Fields[0].Expr
			require.NotNil(t, expr)
			assert.Equal(t, tt.funcName, expr.FuncName)
			assert.Len(t, expr.Args, tt.argCount)
		})
	}
}

func TestParse_CastAsJSON(t *testing.T) {
	t.Parallel()

	stmts, err := New().Parse(context.Background(), "SELECT CAST(data AS JSON) FROM t;")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Len(t, stmts[0].Fields, 1)
	expr := stmts[0].Fields[0].Expr
	require.NotNil(t, expr)
	require.NotNil(t, expr.CastExpr)
	assert.Equal(t, minisql.JSON, expr.CastTargetType)
}

func TestParse_JSONArrow_Tokenization(t *testing.T) {
	t.Parallel()

	// Ensure ->> is not tokenized as -> followed by >
	stmts, err := New().Parse(context.Background(), "SELECT a ->> 'key' FROM t;")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Len(t, stmts[0].Fields, 1)
	expr := stmts[0].Fields[0].Expr
	require.NotNil(t, expr)
	assert.Equal(t, minisql.JSONArrowArrow, expr.Op)

	// Ensure -> does not steal the > from ->>
	stmts, err = New().Parse(context.Background(), "SELECT a -> 'key' FROM t;")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Len(t, stmts[0].Fields, 1)
	expr = stmts[0].Fields[0].Expr
	require.NotNil(t, expr)
	assert.Equal(t, minisql.JSONArrow, expr.Op)
}
