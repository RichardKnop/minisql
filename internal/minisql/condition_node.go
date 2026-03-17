package minisql

// LogicOp represents a boolean logic operator (AND or OR) used in a ConditionNode.
type LogicOp int

const (
	LogicOpAnd LogicOp = iota + 1
	LogicOpOr
)

// ConditionNode is a node in a boolean expression tree for WHERE clauses.
// Leaf nodes (Leaf != nil) hold a single Condition.
// Branch nodes (Left != nil) join two sub-expressions with a logical operator.
type ConditionNode struct {
	Leaf  *Condition     // non-nil for leaf nodes
	Left  *ConditionNode // non-nil for branch nodes
	Op    LogicOp        // logical operator for branch nodes
	Right *ConditionNode // non-nil for branch nodes
}

// IsLeaf returns true if this is a leaf node.
func (n *ConditionNode) IsLeaf() bool {
	return n.Leaf != nil
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
