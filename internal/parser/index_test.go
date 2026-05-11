package parser

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_CreateIndex(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"Empty CREATE INDEX fails",
			"CREATE INDEX",
			nil,
			errEmptyIndexName,
		},
		{
			"CREATE INDEX with no opening parens fails",
			"CREATE INDEX foo",
			nil,
			errEmptyTableName,
		},
		{
			"CREATE INDEX with no column fails",
			"CREATE INDEX foo ON bar",
			nil,
			errCreateIndexNoColumns,
		},
		{
			"CREATE UNIQUE INDEX not supported",
			"CREATE UNIQUE INDEX foo ON bar (qux);",
			nil,
			errInvalidStatementKind,
		},
		{
			"CREATE INDEX with single column works",
			"CREATE INDEX foo ON bar (qux);",
			[]minisql.Statement{
				{
					Kind:        minisql.CreateIndex,
					IndexMethod: minisql.IndexMethodBTree,
					IndexName:   "foo",
					TableName:   "bar",
					Columns: []minisql.Column{
						{
							Name: "qux",
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE INDEX with IF NOT EXISTS works",
			"CREATE INDEX IF NOT EXISTS foo ON bar (qux);",
			[]minisql.Statement{
				{
					Kind:        minisql.CreateIndex,
					IndexMethod: minisql.IndexMethodBTree,
					IndexName:   "foo",
					TableName:   "bar",
					Columns: []minisql.Column{
						{
							Name: "qux",
						},
					},
					IfNotExists: true,
				},
			},
			nil,
		},
		{
			"CREATE INDEX with multiple columns works",
			"CREATE INDEX foo ON bar (qux, baz);",
			[]minisql.Statement{
				{
					Kind:        minisql.CreateIndex,
					IndexMethod: minisql.IndexMethodBTree,
					IndexName:   "foo",
					TableName:   "bar",
					Columns: []minisql.Column{
						{
							Name: "qux",
						},
						{
							Name: "baz",
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE INDEX with partial WHERE works",
			"CREATE INDEX idx_active ON orders (amount) WHERE status = 'active';",
			[]minisql.Statement{
				{
					Kind:             minisql.CreateIndex,
					IndexMethod:      minisql.IndexMethodBTree,
					IndexName:        "idx_active",
					TableName:        "orders",
					IndexWhereClause: "status = 'active'",
					Columns: []minisql.Column{
						{Name: "amount"},
					},
					Conditions: minisql.OneOrMore{
						{
							{
								Operand1: minisql.Operand{Type: minisql.OperandField, Value: minisql.Field{Name: "status"}},
								Operand2: minisql.Operand{Type: minisql.OperandQuotedString, Value: minisql.NewTextPointer([]byte("active"))},
								Operator: minisql.Eq,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"CREATE INDEX without WHERE is still full index",
			"CREATE INDEX idx_all ON orders (amount);",
			[]minisql.Statement{
				{
					Kind:        minisql.CreateIndex,
					IndexMethod: minisql.IndexMethodBTree,
					IndexName:   "idx_all",
					TableName:   "orders",
					Columns:     []minisql.Column{{Name: "amount"}},
				},
			},
			nil,
		},
		{
			"CREATE FULLTEXT INDEX works",
			"CREATE FULLTEXT INDEX idx_body ON articles (body);",
			[]minisql.Statement{
				{
					Kind:           minisql.CreateIndex,
					IndexMethod:    minisql.IndexMethodFullText,
					IndexTokenizer: minisql.TextSearchTokenizerSimple,
					IndexName:      "idx_body",
					TableName:      "articles",
					Columns:        []minisql.Column{{Name: "body"}},
				},
			},
			nil,
		},
		{
			"CREATE FULLTEXT INDEX with tokenizer option works",
			"CREATE FULLTEXT INDEX idx_body ON articles (body) WITH (tokenizer = 'simple');",
			[]minisql.Statement{
				{
					Kind:           minisql.CreateIndex,
					IndexMethod:    minisql.IndexMethodFullText,
					IndexTokenizer: minisql.TextSearchTokenizerSimple,
					IndexName:      "idx_body",
					TableName:      "articles",
					Columns:        []minisql.Column{{Name: "body"}},
				},
			},
			nil,
		},
		{
			"CREATE INVERTED INDEX works",
			"CREATE INVERTED INDEX idx_payload ON events (payload);",
			[]minisql.Statement{
				{
					Kind:        minisql.CreateIndex,
					IndexMethod: minisql.IndexMethodInverted,
					IndexName:   "idx_payload",
					TableName:   "events",
					Columns:     []minisql.Column{{Name: "payload"}},
				},
			},
			nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aStatement, err := New().Parse(context.Background(), aTestCase.SQL)
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

func TestParse_CreateExpressionIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sql           string
		expressionSQL string
		assertExpr    func(*testing.T, *minisql.Expr)
		whereClause   string
		conditions    minisql.OneOrMore
	}{
		{
			name:          "function expression",
			sql:           "CREATE INDEX idx_lower ON users (LOWER(email));",
			expressionSQL: "LOWER(email)",
			assertExpr: func(t *testing.T, expr *minisql.Expr) {
				t.Helper()
				assert.Equal(t, "LOWER", expr.FuncName)
				require.Len(t, expr.Args, 1)
				assert.Equal(t, "email", expr.Args[0].Column)
			},
		},
		{
			name:          "JSON text path expression",
			sql:           "CREATE INDEX idx_json ON events (payload ->> 'type');",
			expressionSQL: "payload ->> 'type'",
			assertExpr: func(t *testing.T, expr *minisql.Expr) {
				t.Helper()
				assert.Equal(t, minisql.JSONArrowArrow, expr.Op)
				require.NotNil(t, expr.Left)
				require.NotNil(t, expr.Right)
				assert.Equal(t, "payload", expr.Left.Column)
				assert.Equal(t, minisql.NewTextPointer([]byte("type")), expr.Right.Literal)
			},
		},
		{
			name:          "nested JSON path expression",
			sql:           "CREATE INDEX idx_nested_json ON events (payload -> 'meta' ->> 'id');",
			expressionSQL: "payload -> 'meta' ->> 'id'",
			assertExpr: func(t *testing.T, expr *minisql.Expr) {
				t.Helper()
				assert.Equal(t, minisql.JSONArrowArrow, expr.Op)
				require.NotNil(t, expr.Left)
				require.NotNil(t, expr.Right)
				assert.Equal(t, minisql.NewTextPointer([]byte("id")), expr.Right.Literal)
				assert.Equal(t, minisql.JSONArrow, expr.Left.Op)
				require.NotNil(t, expr.Left.Left)
				require.NotNil(t, expr.Left.Right)
				assert.Equal(t, "payload", expr.Left.Left.Column)
				assert.Equal(t, minisql.NewTextPointer([]byte("meta")), expr.Left.Right.Literal)
			},
		},
		{
			name:          "cast JSON path expression",
			sql:           "CREATE INDEX idx_json_cast ON events (CAST(payload ->> 'price' AS double));",
			expressionSQL: "CAST(payload ->> 'price' AS double)",
			assertExpr: func(t *testing.T, expr *minisql.Expr) {
				t.Helper()
				assert.Equal(t, minisql.Double, expr.CastTargetType)
				require.NotNil(t, expr.CastExpr)
				assert.Equal(t, minisql.JSONArrowArrow, expr.CastExpr.Op)
				require.NotNil(t, expr.CastExpr.Left)
				require.NotNil(t, expr.CastExpr.Right)
				assert.Equal(t, "payload", expr.CastExpr.Left.Column)
				assert.Equal(t, minisql.NewTextPointer([]byte("price")), expr.CastExpr.Right.Literal)
			},
		},
		{
			name:          "JSON expression with partial WHERE",
			sql:           "CREATE INDEX idx_expr_partial ON events (payload ->> 'type') WHERE active = true;",
			expressionSQL: "payload ->> 'type'",
			whereClause:   "active = true",
			conditions: minisql.OneOrMore{
				{
					{
						Operand1: minisql.Operand{Type: minisql.OperandField, Value: minisql.Field{Name: "active"}},
						Operand2: minisql.Operand{Type: minisql.OperandBoolean, Value: true},
						Operator: minisql.Eq,
					},
				},
			},
			assertExpr: func(t *testing.T, expr *minisql.Expr) {
				t.Helper()
				assert.Equal(t, minisql.JSONArrowArrow, expr.Op)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts, err := New().Parse(context.Background(), tt.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			stmt := stmts[0]
			assert.Equal(t, minisql.CreateIndex, stmt.Kind)
			assert.Equal(t, minisql.IndexMethodBTree, stmt.IndexMethod)
			assert.Equal(t, tt.expressionSQL, stmt.IndexExpressionSQL)
			require.NotNil(t, stmt.IndexExpression)
			assert.Equal(t, []minisql.Column{{Name: "__expr__"}}, stmt.Columns)
			assert.Equal(t, tt.whereClause, stmt.IndexWhereClause)
			assert.Equal(t, tt.conditions, stmt.Conditions)
			tt.assertExpr(t, stmt.IndexExpression)
		})
	}
}

func TestParse_DropIndex(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			Name:     "Empty DROP INDEX fails",
			SQL:      "DROP INDEX",
			Expected: nil,
			Err:      errEmptyIndexName,
		},
		{
			Name: "DROP INDEX works",
			SQL:  "DROP INDEX foo;",
			Expected: []minisql.Statement{
				{
					Kind:      minisql.DropIndex,
					IndexName: "foo",
				},
			},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aStatement, err := New().Parse(context.Background(), aTestCase.SQL)
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
