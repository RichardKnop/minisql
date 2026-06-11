package minisql

// isConstExpr returns true when e contains no column references and can be
// evaluated at plan time against an empty row.
func isConstExpr(e *Expr) bool {
	if e == nil {
		return true
	}
	if e.Column != "" {
		return false
	}
	for _, arg := range e.Args {
		if !isConstExpr(arg) {
			return false
		}
	}
	for _, clause := range e.CaseClauses {
		// Searched CASE: WHEN <condition> — conservatively treat as non-const
		// because ConditionNode may reference columns.
		if clause.Cond != nil {
			return false
		}
		if !isConstExpr(clause.When) || !isConstExpr(clause.Then) {
			return false
		}
	}
	return isConstExpr(e.Left) && isConstExpr(e.Right) &&
		isConstExpr(e.CastExpr) && isConstExpr(e.CaseInput) && isConstExpr(e.CaseElse)
}

// anyToOperand converts a concrete Go value returned by Expr.Eval into the
// matching Operand type so that the condition evaluator can use it directly.
func anyToOperand(v any) Operand {
	switch val := v.(type) {
	case nil:
		return Operand{Type: OperandNull}
	case int64:
		return Operand{Type: OperandInteger, Value: val}
	case int32:
		return Operand{Type: OperandInteger, Value: int64(val)}
	case float64:
		return Operand{Type: OperandFloat, Value: val}
	case float32:
		return Operand{Type: OperandFloat, Value: float64(val)}
	case bool:
		return Operand{Type: OperandBoolean, Value: val}
	case TextPointer:
		return Operand{Type: OperandQuotedString, Value: val}
	case TimestampMicros:
		return Operand{Type: OperandQuotedString, Value: val}
	default:
		return Operand{Type: OperandExpr, Value: v}
	}
}

// conditionsCanSkipFolding returns true when every condition has at least one
// field operand and neither operand is OperandExpr, meaning there is no
// constant sub-expression to fold.  Called as a fast-path to avoid iterating
// through all conditions when folding is guaranteed to be a no-op.
func conditionsCanSkipFolding(conds OneOrMore) bool {
	for _, group := range conds {
		for _, cond := range group {
			if cond.Operand1.Type == OperandExpr || cond.Operand2.Type == OperandExpr {
				return false
			}
			if !cond.Operand1.IsField() && !cond.Operand2.IsField() {
				return false
			}
		}
	}
	return true
}

// FoldConditions evaluates constant sub-expressions in conds, replacing
// OperandExpr operands with concrete literals where possible.
//
// Three outcomes:
//   - (result, false, nil): result is the folded conditions (may be shorter).
//     If result is nil/empty the WHERE is always true (no filter needed).
//   - (nil, true, nil):  every AND group was pruned; the WHERE can never match.
//   - (_, _, err):       an expression evaluation error occurred.
func FoldConditions(conds OneOrMore) (result OneOrMore, alwaysFalse bool, err error) {
	if conditionsCanSkipFolding(conds) {
		return conds, false, nil
	}
	for _, group := range conds {
		folded, groupFalse, groupTrue, ferr := foldAndGroup(group)
		if ferr != nil {
			return nil, false, ferr
		}
		if groupFalse {
			continue // this OR branch can never fire — prune it
		}
		if groupTrue {
			// One OR branch is always satisfied → whole WHERE is always true.
			return nil, false, nil
		}
		result = append(result, folded)
	}
	if len(result) == 0 && len(conds) > 0 {
		return nil, true, nil
	}
	return result, false, nil
}

func foldAndGroup(group Conditions) (folded Conditions, alwaysFalse, alwaysTrue bool, err error) {
	for _, cond := range group {
		fc, condFalse, condTrue, ferr := foldCondition(cond)
		if ferr != nil {
			return nil, false, false, ferr
		}
		if condFalse {
			return nil, true, false, nil // AND short-circuits to false
		}
		if condTrue {
			continue // tautology — drop from AND group
		}
		folded = append(folded, fc)
	}
	if len(folded) == 0 {
		return nil, false, true, nil // all conditions were tautologies
	}
	return folded, false, false, nil
}

// foldCondition folds any constant OperandExpr operands in cond and, when
// neither operand references a row column, evaluates the whole condition.
// condFalse=true means the condition can never be true (prune the AND group).
// condTrue=true means the condition is always true (drop it from the group).
func foldCondition(cond Condition) (fc Condition, condFalse, condTrue bool, err error) {
	if cond.Operand1.Type == OperandExpr {
		if expr, ok := cond.Operand1.Value.(*Expr); ok && isConstExpr(expr) {
			val, evalErr := expr.Eval(Row{})
			if evalErr != nil {
				return cond, false, false, evalErr
			}
			cond.Operand1 = anyToOperand(val)
		}
	}
	if cond.Operand2.Type == OperandExpr {
		if expr, ok := cond.Operand2.Value.(*Expr); ok && isConstExpr(expr) {
			val, evalErr := expr.Eval(Row{})
			if evalErr != nil {
				return cond, false, false, evalErr
			}
			cond.Operand2 = anyToOperand(val)
		}
	}
	// If neither side references a field after folding, evaluate now.
	if !cond.Operand1.IsField() && !cond.Operand2.IsField() &&
		cond.Operand1.Type != OperandExpr && cond.Operand2.Type != OperandExpr {
		result, canEval := evalConstCond(cond)
		if canEval {
			return cond, !result, result, nil
		}
	}
	return cond, false, false, nil
}

// evalConstCond evaluates a condition where neither operand references a row
// column. Returns (result, canEval): canEval is false when the comparison
// cannot be performed (e.g. unsupported type), in which case result is meaningless.
func evalConstCond(cond Condition) (result, canEval bool) {
	if cond.Operand1.Type == OperandNull {
		switch cond.Operator {
		case Eq:
			return cond.Operand2.Type == OperandNull, true
		case Ne:
			return cond.Operand2.Type != OperandNull, true
		default:
			return false, false
		}
	}
	if cond.Operand2.Type == OperandNull {
		switch cond.Operator {
		case Eq:
			return false, true
		case Ne:
			return true, true
		default:
			return false, false
		}
	}
	ok, err := compareScalarToOperand(cond.Operand1.Value, cond.Operand2, cond.Operator)
	if err != nil {
		return false, false
	}
	return ok, true
}
