package minisql

import (
	"fmt"
)

// LogicOp represents a boolean logic operator (AND or OR) used in a ConditionNode.
type LogicOp int

const (
	// LogicOpAnd represents the SQL AND logical operator.
	LogicOpAnd LogicOp = iota + 1
	// LogicOpOr represents the SQL OR logical operator.
	LogicOpOr
)

// ConditionNode is a node in a boolean expression tree for WHERE clauses.
// Leaf nodes (Leaf != nil) hold a single Condition.
// Branch nodes (Left != nil) join two sub-expressions with a logical operator.
type ConditionNode struct {
	Leaf  *Condition
	Left  *ConditionNode
	Right *ConditionNode
	Op    LogicOp
}

// IsLeaf returns true if this is a leaf node.
func (n *ConditionNode) IsLeaf() bool {
	return n.Leaf != nil
}

// String returns a human-readable representation of the condition tree,
// used as a fallback column name for CASE expressions without AS aliases.
func (n *ConditionNode) String() string {
	if n == nil {
		return ""
	}
	if n.IsLeaf() {
		l := n.Leaf
		var op1 string
		if l.Operand1.Type == OperandField {
			if f, ok := l.Operand1.Value.(Field); ok {
				op1 = f.Name
			}
		} else {
			op1 = fmt.Sprintf("%v", l.Operand1.Value)
		}
		if l.Operand2.Type == OperandNull {
			if l.Operator == Eq {
				return op1 + " IS NULL"
			}
			return op1 + " IS NOT NULL"
		}
		var op2 string
		if l.Operand2.Type == OperandField {
			if f, ok := l.Operand2.Value.(Field); ok {
				op2 = f.Name
			}
		} else {
			op2 = fmt.Sprintf("%v", l.Operand2.Value)
		}
		return op1 + " " + l.Operator.String() + " " + op2
	}
	switch n.Op {
	case LogicOpAnd:
		return n.Left.String() + " AND " + n.Right.String()
	case LogicOpOr:
		return "(" + n.Left.String() + " OR " + n.Right.String() + ")"
	}
	return ""
}

// Columns returns all column names referenced in the condition tree.
func (n *ConditionNode) Columns() []string {
	if n == nil {
		return nil
	}
	if n.IsLeaf() {
		var cols []string
		if n.Leaf.Operand1.Type == OperandField {
			if f, ok := n.Leaf.Operand1.Value.(Field); ok {
				cols = append(cols, f.Name)
			}
		}
		if n.Leaf.Operand2.Type == OperandField {
			if f, ok := n.Leaf.Operand2.Value.(Field); ok {
				cols = append(cols, f.Name)
			}
		}
		return cols
	}
	return append(n.Left.Columns(), n.Right.Columns()...)
}

// ToDNF converts the condition tree to Disjunctive Normal Form.
// The result is a OneOrMore where each Conditions group is a conjunction
// (conditions ANDed together) and the outer slice is a disjunction (groups ORed).
// This allows arbitrary WHERE nesting to be evaluated by existing row filtering code.
func (n *ConditionNode) ToDNF() OneOrMore {
	if n.IsLeaf() {
		return OneOrMore{Conditions{*n.Leaf}}
	}
	leftDNF := n.Left.ToDNF()
	rightDNF := n.Right.ToDNF()
	switch n.Op {
	case LogicOpOr:
		result := make(OneOrMore, 0, len(leftDNF)+len(rightDNF))
		return append(append(result, leftDNF...), rightDNF...)
	case LogicOpAnd:
		// Cross-product: merge each pair of left and right groups into one conjunction.
		result := make(OneOrMore, 0, len(leftDNF)*len(rightDNF))
		for _, l := range leftDNF {
			for _, r := range rightDNF {
				merged := make(Conditions, len(l)+len(r))
				copy(merged, l)
				copy(merged[len(l):], r)
				result = append(result, merged)
			}
		}
		return result
	}
	return nil
}
