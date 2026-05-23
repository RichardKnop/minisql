package minisql

import (
	"fmt"
)

// semiJoinEligible reports whether sub can be converted to a semi-join.
// Returns the single inner column name and true when eligible.
// A subquery is eligible when it is a plain SELECT with no joins, no UNION,
// no CTEs, no aggregates, no DISTINCT, no GROUP BY/HAVING, no LIMIT/OFFSET,
// and selects exactly one plain column reference.
func semiJoinEligible(sub Statement) (innerCol string, ok bool) {
	if sub.Kind != Select {
		return "", false
	}
	if sub.FromSubquery != nil || len(sub.CTEs) > 0 || len(sub.Joins) > 0 || len(sub.Unions) > 0 {
		return "", false
	}
	if sub.Distinct || sub.GroupBy != nil || sub.Having != nil {
		return "", false
	}
	if sub.Limit.Valid || sub.Offset.Valid {
		return "", false
	}
	if len(sub.Fields) != 1 {
		return "", false
	}
	f := sub.Fields[0]
	if f.Name == "*" || f.Expr != nil {
		return "", false
	}
	return f.Name, true
}

// outerTableNames collects all table names referenced in stmt (base table + all
// join tables at every depth).  Used to detect conflicts before adding a
// synthetic semi-join that refers to the same table.
func outerTableNames(stmt Statement) map[string]struct{} {
	names := map[string]struct{}{stmt.TableName: {}}
	var walk func(joins []Join)
	walk = func(joins []Join) {
		for _, j := range joins {
			names[j.TableName] = struct{}{}
			walk(j.Joins)
		}
	}
	walk(stmt.Joins)
	return names
}

// prefixConditionAlias rewrites bare field references (AliasPrefix == "") in
// cond to carry alias as the AliasPrefix.  This allows pushDownFilters to route
// inner-subquery WHERE conditions to the correct semi-join scan.
func prefixConditionAlias(cond Condition, alias string) Condition {
	if cond.Operand1.Type == OperandField {
		if f, ok := cond.Operand1.Value.(Field); ok && f.AliasPrefix == "" {
			f.AliasPrefix = alias
			cond.Operand1.Value = f
		}
	}
	if cond.Operand2.Type == OperandField {
		if f, ok := cond.Operand2.Value.(Field); ok && f.AliasPrefix == "" {
			f.AliasPrefix = alias
			cond.Operand2.Value = f
		}
	}
	return cond
}

// hasINSubqueryConditions reports whether conds contains any IN/NOT IN condition
// whose right-hand side is a subquery. Used as a fast-path guard to avoid
// allocating the outerTableNames map when there is nothing to lift.
func hasINSubqueryConditions(conds OneOrMore) bool {
	for _, group := range conds {
		for _, cond := range group {
			if (cond.Operator == In || cond.Operator == NotIn) &&
				cond.Operand2.Type == OperandSubquery {
				return true
			}
		}
	}
	return false
}

// liftINSubqueriesToSemiJoins converts eligible IN/NOT IN (subquery) conditions
// in stmt.Conditions into Semi / AntiSemi JOIN entries, removing the original
// condition from stmt.Conditions.  Ineligible subqueries are left untouched so
// resolveSubqueries can still materialise them.
//
// When a semi-join is added and the outer statement has no table alias, the
// alias is set to stmt.TableName so that the join planner can form correct
// ON conditions.
func liftINSubqueriesToSemiJoins(stmt Statement) Statement {
	if !hasINSubqueryConditions(stmt.Conditions) {
		return stmt
	}
	outerTables := outerTableNames(stmt)
	semiCounter := 0

	// outerAlias is used to prefix the outer field in the ON condition.
	// When the outer table has no alias, the field AliasPrefix is already ""
	// and extractJoinColumnPairs matches on "" == "".
	outerAlias := stmt.TableAlias

	extraConditions := make(OneOrMore, 0)
	newConditions := make(OneOrMore, 0, len(stmt.Conditions))

	for _, group := range stmt.Conditions {
		newGroup := make(Conditions, 0, len(group))
		for _, cond := range group {
			if cond.Operand2.Type != OperandSubquery ||
				(cond.Operator != In && cond.Operator != NotIn) ||
				cond.Operand1.Type != OperandField {
				newGroup = append(newGroup, cond)
				continue
			}

			subStmt, ok := cond.Operand2.Value.(*Statement)
			if !ok {
				newGroup = append(newGroup, cond)
				continue
			}

			innerCol, eligible := semiJoinEligible(*subStmt)
			if !eligible {
				newGroup = append(newGroup, cond)
				continue
			}

			// Skip if the inner table is already referenced in the outer query;
			// validateJoinTree rejects duplicate table names.
			if _, conflict := outerTables[subStmt.TableName]; conflict {
				newGroup = append(newGroup, cond)
				continue
			}

			joinType := Semi
			if cond.Operator == NotIn {
				joinType = AntiSemi
			}

			semiAlias := fmt.Sprintf("__semi%d", semiCounter)
			semiCounter += 1

			// Build the ON condition: outerAlias.outerCol = semiAlias.innerCol
			outerField := cond.Operand1.Value.(Field)
			if outerField.AliasPrefix == "" {
				outerField.AliasPrefix = outerAlias
			}
			onCond := Condition{
				Operand1: Operand{Type: OperandField, Value: outerField},
				Operator: Eq,
				Operand2: Operand{Type: OperandField, Value: Field{Name: innerCol, AliasPrefix: semiAlias}},
			}

			// Re-prefix inner WHERE conditions with semiAlias so that
			// pushDownFilters routes them to the semi-join's scan.
			for _, innerGroup := range subStmt.Conditions {
				rewritten := make(Conditions, len(innerGroup))
				for k, innerCond := range innerGroup {
					rewritten[k] = prefixConditionAlias(innerCond, semiAlias)
				}
				extraConditions = append(extraConditions, rewritten)
			}

			// Mark the inner table as used so a second IN clause on the same
			// table falls back to materialisation.
			outerTables[subStmt.TableName] = struct{}{}

			stmt.Joins = append(stmt.Joins, Join{
				Type:       joinType,
				TableName:  subStmt.TableName,
				TableAlias: semiAlias,
				Conditions: Conditions{onCond},
			})
			// Drop the original IN condition — the semi-join replaces it.
		}

		if len(newGroup) > 0 {
			newConditions = append(newConditions, newGroup)
		}
	}

	// Append pushed-down inner conditions after the outer conditions so that
	// pushDownFilters can distribute them to the right scan plan.
	newConditions = append(newConditions, extraConditions...)
	stmt.Conditions = newConditions

	return stmt
}
