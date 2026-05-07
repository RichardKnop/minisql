package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func leaf(cond Condition) *ConditionNode {
	return &ConditionNode{Leaf: &cond}
}

func and(l, r *ConditionNode) *ConditionNode {
	return &ConditionNode{Left: l, Op: LogicOpAnd, Right: r}
}

func or(l, r *ConditionNode) *ConditionNode {
	return &ConditionNode{Left: l, Op: LogicOpOr, Right: r}
}

func TestConditionNode_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		node *ConditionNode
		want string
	}{
		{
			name: "nil node returns empty string",
			node: nil,
			want: "",
		},
		{
			name: "leaf with field = integer",
			node: leaf(FieldIsEqual(Field{Name: "age"}, OperandInteger, int64(25))),
			want: "age = 25",
		},
		{
			name: "leaf with IS NULL",
			node: leaf(FieldIsNull(Field{Name: "email"})),
			want: "email IS NULL",
		},
		{
			name: "leaf with IS NOT NULL",
			node: leaf(FieldIsNotNull(Field{Name: "email"})),
			want: "email IS NOT NULL",
		},
		{
			name: "leaf with field != field",
			node: leaf(Condition{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "a"}},
				Operator: Ne,
				Operand2: Operand{Type: OperandField, Value: Field{Name: "b"}},
			}),
			want: "a != b",
		},
		{
			name: "leaf with non-field operands (literal string)",
			node: leaf(Condition{
				Operand1: Operand{Type: OperandQuotedString, Value: "hello"},
				Operator: Eq,
				Operand2: Operand{Type: OperandQuotedString, Value: "world"},
			}),
			want: "hello = world",
		},
		{
			name: "AND branch",
			node: and(
				leaf(FieldIsEqual(Field{Name: "a"}, OperandInteger, int64(1))),
				leaf(FieldIsEqual(Field{Name: "b"}, OperandInteger, int64(2))),
			),
			want: "a = 1 AND b = 2",
		},
		{
			name: "OR branch",
			node: or(
				leaf(FieldIsEqual(Field{Name: "x"}, OperandInteger, int64(10))),
				leaf(FieldIsEqual(Field{Name: "y"}, OperandInteger, int64(20))),
			),
			want: "(x = 10 OR y = 20)",
		},
		{
			name: "nested AND/OR",
			node: and(
				leaf(FieldIsEqual(Field{Name: "a"}, OperandInteger, int64(1))),
				or(
					leaf(FieldIsEqual(Field{Name: "b"}, OperandInteger, int64(2))),
					leaf(FieldIsEqual(Field{Name: "c"}, OperandInteger, int64(3))),
				),
			),
			want: "a = 1 AND (b = 2 OR c = 3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.node.String())
		})
	}
}

func TestConditionNode_ToDNF(t *testing.T) {
	t.Parallel()

	a := FieldIsEqual(Field{Name: "a"}, OperandInteger, int64(1))
	b := FieldIsEqual(Field{Name: "b"}, OperandInteger, int64(2))
	c := FieldIsEqual(Field{Name: "c"}, OperandInteger, int64(3))
	d := FieldIsEqual(Field{Name: "d"}, OperandInteger, int64(4))

	tests := []struct {
		name string
		node *ConditionNode
		want OneOrMore
	}{
		{
			name: "single leaf",
			node: leaf(a),
			want: OneOrMore{Conditions{a}},
		},
		{
			name: "two conditions ANDed",
			node: and(leaf(a), leaf(b)),
			want: OneOrMore{Conditions{a, b}},
		},
		{
			name: "two conditions ORed",
			node: or(leaf(a), leaf(b)),
			want: OneOrMore{Conditions{a}, Conditions{b}},
		},
		{
			name: "three conditions ANDed",
			node: and(and(leaf(a), leaf(b)), leaf(c)),
			want: OneOrMore{Conditions{a, b, c}},
		},
		{
			name: "a AND b OR c — standard DNF",
			node: or(and(leaf(a), leaf(b)), leaf(c)),
			want: OneOrMore{Conditions{a, b}, Conditions{c}},
		},
		{
			name: "(a OR b) AND c — distributes into DNF",
			node: and(or(leaf(a), leaf(b)), leaf(c)),
			want: OneOrMore{Conditions{a, c}, Conditions{b, c}},
		},
		{
			name: "(a OR b) AND (c OR d) — full cross-product",
			node: and(or(leaf(a), leaf(b)), or(leaf(c), leaf(d))),
			want: OneOrMore{
				Conditions{a, c},
				Conditions{a, d},
				Conditions{b, c},
				Conditions{b, d},
			},
		},
		{
			name: "a AND (b OR c) — distributes",
			node: and(leaf(a), or(leaf(b), leaf(c))),
			want: OneOrMore{Conditions{a, b}, Conditions{a, c}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.node.ToDNF())
		})
	}
}

func TestConditionNode_Columns(t *testing.T) {
	t.Parallel()

	// nil receiver returns nil.
	var nilNode *ConditionNode
	assert.Nil(t, nilNode.Columns())

	// Leaf with two field operands returns both column names.
	leaf := &ConditionNode{Leaf: &Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "a"}},
		Operator: Eq,
		Operand2: Operand{Type: OperandField, Value: Field{Name: "b"}},
	}}
	assert.Equal(t, []string{"a", "b"}, leaf.Columns())

	// Non-leaf OR node merges columns from both children.
	left := &ConditionNode{Leaf: &Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "x"}},
		Operator: Eq,
		Operand2: Operand{Type: OperandInteger, Value: int64(1)},
	}}
	right := &ConditionNode{Leaf: &Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "y"}},
		Operator: Eq,
		Operand2: Operand{Type: OperandInteger, Value: int64(2)},
	}}
	parent := &ConditionNode{Op: LogicOpOr, Left: left, Right: right}
	assert.Equal(t, []string{"x", "y"}, parent.Columns())
}
