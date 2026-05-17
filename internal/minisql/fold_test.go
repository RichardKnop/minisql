package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsConstExpr verifies the constant-expression detector.
func TestIsConstExpr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		expr  *Expr
		want  bool
	}{
		{
			name: "nil",
			expr: nil,
			want: true,
		},
		{
			name: "integer literal",
			expr: &Expr{Literal: int64(42)},
			want: true,
		},
		{
			name: "string literal",
			expr: &Expr{Literal: NewTextPointer([]byte("hello"))},
			want: true,
		},
		{
			name: "null literal",
			expr: &Expr{IsNull: true},
			want: true,
		},
		{
			name: "column reference",
			expr: &Expr{Column: "status"},
			want: false,
		},
		{
			name: "NOW() — no column ref",
			expr: &Expr{FuncName: "NOW"},
			want: true,
		},
		{
			name: "UPPER with literal arg",
			expr: &Expr{
				FuncName: "UPPER",
				Args:     []*Expr{{Literal: NewTextPointer([]byte("hello"))}},
			},
			want: true,
		},
		{
			name: "UPPER with column arg",
			expr: &Expr{
				FuncName: "UPPER",
				Args:     []*Expr{{Column: "status"}},
			},
			want: false,
		},
		{
			name: "arithmetic with literals",
			expr: &Expr{
				Left:  &Expr{Literal: int64(1)},
				Right: &Expr{Literal: int64(2)},
				Op:    ArithAdd,
			},
			want: true,
		},
		{
			name: "arithmetic with column",
			expr: &Expr{
				Left:  &Expr{Column: "price"},
				Right: &Expr{Literal: int64(2)},
				Op:    ArithMul,
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isConstExpr(tc.expr))
		})
	}
}

// TestAnyToOperand verifies the Go-value → Operand conversion.
func TestAnyToOperand(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input any
		want  Operand
	}{
		{
			name:  "nil → null",
			input: nil,
			want:  Operand{Type: OperandNull},
		},
		{
			name:  "int64",
			input: int64(7),
			want:  Operand{Type: OperandInteger, Value: int64(7)},
		},
		{
			name:  "int32 promoted to int64",
			input: int32(3),
			want:  Operand{Type: OperandInteger, Value: int64(3)},
		},
		{
			name:  "float64",
			input: float64(3.14),
			want:  Operand{Type: OperandFloat, Value: float64(3.14)},
		},
		{
			name:  "float32 promoted to float64",
			input: float32(1.5),
			want:  Operand{Type: OperandFloat, Value: float64(float32(1.5))},
		},
		{
			name:  "bool",
			input: true,
			want:  Operand{Type: OperandBoolean, Value: true},
		},
		{
			name:  "TextPointer",
			input: NewTextPointer([]byte("HELLO")),
			want:  Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("HELLO"))},
		},
		{
			name:  "TimestampMicros",
			input: TimestampMicros(1000),
			want:  Operand{Type: OperandQuotedString, Value: TimestampMicros(1000)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, anyToOperand(tc.input))
		})
	}
}

// TestFoldConditions_ConstExprRHS verifies that a constant OperandExpr on the
// RHS of a field condition is folded to a concrete operand.
func TestFoldConditions_ConstExprRHS(t *testing.T) {
	t.Parallel()

	upperExpr := &Expr{
		FuncName: "UPPER",
		Args:     []*Expr{{Literal: NewTextPointer([]byte("active"))}},
	}
	cond := Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
		Operator: Eq,
		Operand2: Operand{Type: OperandExpr, Value: upperExpr},
	}
	conds := OneOrMore{{cond}}

	result, alwaysFalse, err := FoldConditions(conds)
	require.NoError(t, err)
	assert.False(t, alwaysFalse)
	require.Len(t, result, 1)
	require.Len(t, result[0], 1)

	got := result[0][0]
	assert.Equal(t, OperandField, got.Operand1.Type, "operand1 should remain a field")
	assert.Equal(t, OperandQuotedString, got.Operand2.Type, "const expr should fold to QuotedString")
	assert.Equal(t, NewTextPointer([]byte("ACTIVE")), got.Operand2.Value)
}

// TestFoldConditions_ConstExprLHS verifies folding on the left side.
func TestFoldConditions_ConstExprLHS(t *testing.T) {
	t.Parallel()

	addExpr := &Expr{
		Left:  &Expr{Literal: int64(1)},
		Right: &Expr{Literal: int64(2)},
		Op:    ArithAdd,
	}
	cond := Condition{
		Operand1: Operand{Type: OperandExpr, Value: addExpr},
		Operator: Eq,
		Operand2: Operand{Type: OperandInteger, Value: int64(3)},
	}
	conds := OneOrMore{{cond}}

	result, alwaysFalse, err := FoldConditions(conds)
	require.NoError(t, err)
	assert.False(t, alwaysFalse)
	// 1+2 = 3 is always true → condition is a tautology → AND group empty → alwaysTrue → no filter
	assert.Empty(t, result, "tautology should fold to no conditions")
}

// TestFoldConditions_AlwaysFalse verifies that a demonstrably false condition
// causes its AND group to be pruned; if all groups are pruned, alwaysFalse=true.
func TestFoldConditions_AlwaysFalse(t *testing.T) {
	t.Parallel()

	// 1 = 2 is always false
	cond := Condition{
		Operand1: Operand{Type: OperandInteger, Value: int64(1)},
		Operator: Eq,
		Operand2: Operand{Type: OperandInteger, Value: int64(2)},
	}
	conds := OneOrMore{{cond}}

	_, alwaysFalse, err := FoldConditions(conds)
	require.NoError(t, err)
	assert.True(t, alwaysFalse)
}

// TestFoldConditions_AlwaysTrue verifies that a tautology condition causes the
// whole WHERE to be dropped (result is nil/empty, alwaysFalse=false).
func TestFoldConditions_AlwaysTrue(t *testing.T) {
	t.Parallel()

	// 5 > 3 is always true
	cond := Condition{
		Operand1: Operand{Type: OperandInteger, Value: int64(5)},
		Operator: Gt,
		Operand2: Operand{Type: OperandInteger, Value: int64(3)},
	}
	conds := OneOrMore{{cond}}

	result, alwaysFalse, err := FoldConditions(conds)
	require.NoError(t, err)
	assert.False(t, alwaysFalse)
	assert.Empty(t, result, "always-true WHERE should reduce to no conditions")
}

// TestFoldConditions_ColumnExprNotFolded verifies that an OperandExpr containing
// a column reference is NOT folded (it requires a row value at scan time).
func TestFoldConditions_ColumnExprNotFolded(t *testing.T) {
	t.Parallel()

	colExpr := &Expr{Column: "price"}
	cond := Condition{
		Operand1: Operand{Type: OperandExpr, Value: colExpr},
		Operator: Gt,
		Operand2: Operand{Type: OperandInteger, Value: int64(100)},
	}
	conds := OneOrMore{{cond}}

	result, alwaysFalse, err := FoldConditions(conds)
	require.NoError(t, err)
	assert.False(t, alwaysFalse)
	require.Len(t, result, 1)
	require.Len(t, result[0], 1)
	// Column ref expression must stay as OperandExpr
	assert.Equal(t, OperandExpr, result[0][0].Operand1.Type)
}

// TestEvalConstCond exercises all branches of evalConstCond directly.
func TestEvalConstCond(t *testing.T) {
	t.Parallel()

	t.Run("NULL = NULL", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandNull},
			Operator: Eq,
			Operand2: Operand{Type: OperandNull},
		}
		result, canEval := evalConstCond(cond)
		assert.True(t, canEval)
		assert.True(t, result)
	})

	t.Run("NULL = non-null", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandNull},
			Operator: Eq,
			Operand2: Operand{Type: OperandInteger, Value: int64(1)},
		}
		result, canEval := evalConstCond(cond)
		assert.True(t, canEval)
		assert.False(t, result)
	})

	t.Run("NULL != non-null", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandNull},
			Operator: Ne,
			Operand2: Operand{Type: OperandInteger, Value: int64(1)},
		}
		result, canEval := evalConstCond(cond)
		assert.True(t, canEval)
		assert.True(t, result)
	})

	t.Run("NULL > non-null (unsupported for null)", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandNull},
			Operator: Gt,
			Operand2: Operand{Type: OperandInteger, Value: int64(1)},
		}
		result, canEval := evalConstCond(cond)
		assert.False(t, canEval)
		assert.False(t, result)
	})

	t.Run("non-null = NULL", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandInteger, Value: int64(5)},
			Operator: Eq,
			Operand2: Operand{Type: OperandNull},
		}
		result, canEval := evalConstCond(cond)
		assert.True(t, canEval)
		assert.False(t, result)
	})

	t.Run("non-null != NULL", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandInteger, Value: int64(5)},
			Operator: Ne,
			Operand2: Operand{Type: OperandNull},
		}
		result, canEval := evalConstCond(cond)
		assert.True(t, canEval)
		assert.True(t, result)
	})

	t.Run("non-null > NULL (unsupported for null)", func(t *testing.T) {
		cond := Condition{
			Operand1: Operand{Type: OperandInteger, Value: int64(5)},
			Operator: Gt,
			Operand2: Operand{Type: OperandNull},
		}
		result, canEval := evalConstCond(cond)
		assert.False(t, canEval)
		assert.False(t, result)
	})

	t.Run("unsupported operand type returns canEval=false", func(t *testing.T) {
		// struct{}{} is not a recognised scalar type → compareScalarToOperand errors.
		cond := Condition{
			Operand1: Operand{Type: OperandInteger, Value: struct{}{}},
			Operator: Eq,
			Operand2: Operand{Type: OperandInteger, Value: int64(1)},
		}
		result, canEval := evalConstCond(cond)
		assert.False(t, canEval)
		assert.False(t, result)
	})
}

// TestIsConstExpr_CaseClauses exercises the CASE-clause branches of isConstExpr.
func TestIsConstExpr_CaseClauses(t *testing.T) {
	t.Parallel()

	t.Run("searched CASE with non-nil Cond is non-const", func(t *testing.T) {
		expr := &Expr{
			CaseClauses: []CaseWhen{
				{Cond: &ConditionNode{}, When: &Expr{Literal: int64(1)}, Then: &Expr{Literal: int64(2)}},
			},
		}
		assert.False(t, isConstExpr(expr))
	})

	t.Run("simple CASE with non-const When is non-const", func(t *testing.T) {
		expr := &Expr{
			CaseClauses: []CaseWhen{
				{When: &Expr{Column: "status"}, Then: &Expr{Literal: int64(1)}},
			},
		}
		assert.False(t, isConstExpr(expr))
	})

	t.Run("simple CASE with all-const clauses is const", func(t *testing.T) {
		expr := &Expr{
			CaseClauses: []CaseWhen{
				{When: &Expr{Literal: int64(1)}, Then: &Expr{Literal: int64(2)}},
			},
		}
		assert.True(t, isConstExpr(expr))
	})
}

// TestAnyToOperand_Default verifies that an unrecognised type returns OperandExpr.
func TestAnyToOperand_Default(t *testing.T) {
	t.Parallel()

	type myType struct{}
	got := anyToOperand(myType{})
	assert.Equal(t, OperandExpr, got.Type)
}

// TestFoldConditions_MixedGroups verifies OR group semantics: if one AND group is
// always false it is pruned, but other groups survive.
func TestFoldConditions_MixedGroups(t *testing.T) {
	t.Parallel()

	// Group 0: 1 = 2 (always false) → pruned
	falseCond := Condition{
		Operand1: Operand{Type: OperandInteger, Value: int64(1)},
		Operator: Eq,
		Operand2: Operand{Type: OperandInteger, Value: int64(2)},
	}
	// Group 1: status = UPPER('active') — field condition, stays
	upperExpr := &Expr{
		FuncName: "UPPER",
		Args:     []*Expr{{Literal: NewTextPointer([]byte("active"))}},
	}
	realCond := Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
		Operator: Eq,
		Operand2: Operand{Type: OperandExpr, Value: upperExpr},
	}
	conds := OneOrMore{{falseCond}, {realCond}}

	result, alwaysFalse, err := FoldConditions(conds)
	require.NoError(t, err)
	assert.False(t, alwaysFalse, "one OR branch survives")
	require.Len(t, result, 1, "the always-false group should be pruned")
	assert.Equal(t, OperandQuotedString, result[0][0].Operand2.Type, "expr should be folded")
}
