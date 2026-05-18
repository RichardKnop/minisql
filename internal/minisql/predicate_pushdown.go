package minisql

// pushIntoInner attempts to push outer WHERE conditions into an inner subquery
// statement. It returns a (possibly modified) copy of the inner statement and
// the conditions that could not be pushed and must remain in the outer WHERE.
//
// A condition group is eligible for pushdown when:
//   - Every condition in the group references only columns produced by the inner
//     SELECT (checked via outerAlias and the set of inner output column names).
//   - The inner query has no GROUP BY / aggregates / HAVING (pushing before
//     aggregation changes which rows are aggregated).
//   - The inner query has no LIMIT/OFFSET (LIMIT picks a specific top-N; a
//     pushed filter would change which rows appear in that top-N).
//   - The inner query has no UNION (each branch has an independent schema).
//
// Eligible groups are stripped of the outer alias prefix and merged into the
// inner statement's Conditions using AND semantics. Ineligible groups remain in
// the returned OneOrMore for the outer query to apply.
func pushIntoInner(outerConds OneOrMore, inner Statement, outerAlias string) (Statement, OneOrMore) {
	if len(outerConds) == 0 || !innerIsPushdownEligible(inner) {
		return inner, outerConds
	}

	innerCols := innerOutputColumns(inner)

	var pushable, remaining OneOrMore
	for _, group := range outerConds {
		if groupReferencesOnlyInner(group, outerAlias, innerCols) {
			pushable = append(pushable, group)
		} else {
			remaining = append(remaining, group)
		}
	}

	if len(pushable) == 0 {
		return inner, outerConds
	}

	// Strip the outer alias prefix so pushed conditions use plain column names
	// that match the inner table's column schema.
	remapped := stripConditionsAlias(pushable, outerAlias)

	inner.Conditions = mergeConditionsAND(inner.Conditions, remapped)
	return inner, remaining
}

// innerIsPushdownEligible reports whether predicates can safely be pushed into
// the inner statement without changing query semantics.
func innerIsPushdownEligible(inner Statement) bool {
	if len(inner.GroupBy) > 0 || len(inner.Aggregates) > 0 || len(inner.Having) > 0 {
		return false
	}
	if inner.Limit.IsValid() || inner.Offset.IsValid() {
		return false
	}
	if len(inner.Unions) > 0 {
		return false
	}
	return true
}

// innerOutputColumns returns the set of column names produced by the inner
// SELECT. Returns nil when the inner selects all columns (SELECT * or no
// explicit field list), meaning any column reference in the outer WHERE is
// eligible for pushdown.
func innerOutputColumns(inner Statement) map[string]bool {
	if len(inner.Fields) == 0 {
		return nil // implicit SELECT * — all columns eligible
	}
	cols := make(map[string]bool, len(inner.Fields))
	for _, f := range inner.Fields {
		if f.Name == "*" {
			return nil // explicit wildcard — all columns eligible
		}
		cols[f.OutputName()] = true
	}
	return cols
}

// groupReferencesOnlyInner reports whether every condition in the group
// references only columns produced by the inner SELECT. outerAlias is the
// alias used in the outer WHERE to qualify the derived-table columns;
// innerCols is nil when the inner is SELECT * (all columns eligible).
func groupReferencesOnlyInner(group Conditions, outerAlias string, innerCols map[string]bool) bool {
	for _, cond := range group {
		if !condReferencesOnlyInner(cond, outerAlias, innerCols) {
			return false
		}
	}
	return true
}

func condReferencesOnlyInner(cond Condition, outerAlias string, innerCols map[string]bool) bool {
	return operandReferencesOnlyInner(cond.Operand1, outerAlias, innerCols) &&
		operandReferencesOnlyInner(cond.Operand2, outerAlias, innerCols)
}

func operandReferencesOnlyInner(op Operand, outerAlias string, innerCols map[string]bool) bool {
	if op.Type != OperandField {
		// Literals, NULLs, subqueries, expressions — not column references.
		return true
	}
	f := op.Value.(Field)
	// A non-empty alias prefix that isn't the outer alias means the operand
	// references a different table (e.g., a JOIN partner) — cannot push.
	if f.AliasPrefix != "" && f.AliasPrefix != outerAlias {
		return false
	}
	// nil innerCols means SELECT * — all column names are eligible.
	if innerCols == nil {
		return true
	}
	return innerCols[f.Name]
}

// mergeConditionsAND returns the logical AND of two DNF condition sets.
//
// In DNF: (E₁ OR E₂ OR …) AND (P₁ OR P₂ OR …) = all pairs (Eᵢ ++ Pⱼ).
// When either set is empty the other is returned unchanged.
func mergeConditionsAND(existing, pushed OneOrMore) OneOrMore {
	if len(existing) == 0 {
		return pushed
	}
	if len(pushed) == 0 {
		return existing
	}
	result := make(OneOrMore, 0, len(existing)*len(pushed))
	for _, eg := range existing {
		for _, pg := range pushed {
			combined := make(Conditions, len(eg)+len(pg))
			copy(combined, eg)
			copy(combined[len(eg):], pg)
			result = append(result, combined)
		}
	}
	return result
}
