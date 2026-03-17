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
