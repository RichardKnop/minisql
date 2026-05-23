package minisql

// cteBodyIsInlineEligible reports whether a CTE body can be safely inlined
// into the outer query without changing semantics. The body must be a plain
// filtered scan of a real table — no aggregation, DISTINCT, LIMIT, UNION,
// derived-table FROM, JOINs, table alias, or column renaming.
func cteBodyIsInlineEligible(body Statement) bool {
	if body.FromSubquery != nil || len(body.Joins) > 0 {
		return false
	}
	if len(body.GroupBy) > 0 || len(body.Aggregates) > 0 || len(body.Having) > 0 {
		return false
	}
	if body.Distinct {
		return false
	}
	if body.Limit.Valid || body.Offset.Valid {
		return false
	}
	if len(body.Unions) > 0 {
		return false
	}
	// A table alias in the body introduces a second level of alias rewriting
	// that is not worth the complexity for this optimisation.
	if body.TableAlias != "" {
		return false
	}
	// Column renaming (SELECT col AS alias) changes the names exposed to the
	// outer query; rewriting outer field references back to inner names is
	// out of scope for the pragmatic inlining pass.
	for _, f := range body.Fields {
		if f.Name != "*" && f.Alias != "" {
			return false
		}
	}
	return true
}

// cteBodyIsInlineEligibleIgnoreAliases is like cteBodyIsInlineEligible but
// skips the column-alias restriction. Use when the caller separately checks
// that no alias is actually referenced by the outer query.
func cteBodyIsInlineEligibleIgnoreAliases(body Statement) bool {
	if body.FromSubquery != nil || len(body.Joins) > 0 {
		return false
	}
	if len(body.GroupBy) > 0 || len(body.Aggregates) > 0 || len(body.Having) > 0 {
		return false
	}
	if body.Distinct {
		return false
	}
	if body.Limit.Valid || body.Offset.Valid {
		return false
	}
	if len(body.Unions) > 0 {
		return false
	}
	if body.TableAlias != "" {
		return false
	}
	return true
}

// cteBodyAliasesConflictWithOuter returns true if any column alias defined in
// body.Fields is referenced by the outer query's fields, conditions, GROUP BY,
// HAVING, or ORDER BY. If no alias is referenced, inlining is safe even though
// the CTE body uses column renaming: the outer simply never names those aliases.
func cteBodyAliasesConflictWithOuter(body, outer Statement) bool {
	aliasSet := make(map[string]struct{})
	for _, f := range body.Fields {
		if f.Name != "*" && f.Alias != "" {
			aliasSet[f.Alias] = struct{}{}
		}
	}
	if len(aliasSet) == 0 {
		return false
	}

	for _, f := range outer.Fields {
		if _, ok := aliasSet[f.Name]; ok {
			return true
		}
	}
	for _, group := range outer.Conditions {
		for _, cond := range group {
			if operandRefsAlias(cond.Operand1, aliasSet) || operandRefsAlias(cond.Operand2, aliasSet) {
				return true
			}
		}
	}
	for _, f := range outer.GroupBy {
		if _, ok := aliasSet[f.Name]; ok {
			return true
		}
	}
	for _, group := range outer.Having {
		for _, cond := range group {
			if operandRefsAlias(cond.Operand1, aliasSet) || operandRefsAlias(cond.Operand2, aliasSet) {
				return true
			}
		}
	}
	for _, ob := range outer.OrderBy {
		if _, ok := aliasSet[ob.Field.Name]; ok {
			return true
		}
	}
	return false
}

func operandRefsAlias(op Operand, aliases map[string]struct{}) bool {
	if op.Type != OperandField {
		return false
	}
	f, _ := op.Value.(Field)
	_, ok := aliases[f.Name]
	return ok
}

// cteRefCount returns the number of times cteName is referenced as a FROM or
// JOIN source in the main statement and in all other CTE bodies.
// Subquery references inside WHERE conditions are NOT counted here — use
// cteAppearsInConditionSubqueries for those.
func cteRefCount(cteName string, mainStmt Statement, allCTEs []CTE) int {
	count := 0
	if mainStmt.TableName == cteName {
		count += 1
	}
	count += cteJoinRefCount(cteName, mainStmt.Joins)
	for _, other := range allCTEs {
		if other.Name == cteName {
			continue
		}
		if other.Body.TableName == cteName {
			count += 1
		}
		count += cteJoinRefCount(cteName, other.Body.Joins)
	}
	return count
}

func cteJoinRefCount(cteName string, joins []Join) int {
	count := 0
	for _, j := range joins {
		if j.TableName == cteName {
			count += 1
		}
		count += cteJoinRefCount(cteName, j.Joins)
	}
	return count
}

// cteAppearsInConditionSubqueries reports whether cteName is used as the FROM
// table of a direct subquery embedded in conds. This guards against inlining
// a CTE that appears both as the main FROM source and inside a WHERE subquery,
// because inlining removes the CTE from the registry that subquery resolution
// depends on.
func cteAppearsInConditionSubqueries(cteName string, conds OneOrMore) bool {
	for _, group := range conds {
		for _, cond := range group {
			if operandSubqueryReferencesCTE(cond.Operand1, cteName) ||
				operandSubqueryReferencesCTE(cond.Operand2, cteName) {
				return true
			}
		}
	}
	return false
}

func operandSubqueryReferencesCTE(op Operand, cteName string) bool {
	if op.Type != OperandSubquery {
		return false
	}
	sub, ok := op.Value.(*Statement)
	return ok && sub.TableName == cteName
}

// cteIsUsed reports whether cteName is referenced anywhere that requires
// it to be materialised: as the main FROM source, as a JOIN target, inside a
// WHERE subquery, or inside another CTE body.
func cteIsUsed(cteName string, mainStmt Statement, allCTEs []CTE) bool {
	if mainStmt.TableName == cteName {
		return true
	}
	if cteJoinRefCount(cteName, mainStmt.Joins) > 0 {
		return true
	}
	if cteAppearsInConditionSubqueries(cteName, mainStmt.Conditions) {
		return true
	}
	for _, other := range allCTEs {
		if other.Name == cteName {
			continue
		}
		if other.Body.TableName == cteName {
			return true
		}
		if cteJoinRefCount(cteName, other.Body.Joins) > 0 {
			return true
		}
		if cteAppearsInConditionSubqueries(cteName, other.Body.Conditions) {
			return true
		}
	}
	return false
}

// pruneUnusedCTEs removes CTEs from stmt.CTEs that are never referenced by
// the main query or by other CTEs. Unreferenced CTEs produce no results
// visible to the query and materialising them wastes resources.
func pruneUnusedCTEs(stmt Statement) Statement {
	if len(stmt.CTEs) == 0 {
		return stmt
	}
	kept := make([]CTE, 0, len(stmt.CTEs))
	for _, cte := range stmt.CTEs {
		if cteIsUsed(cte.Name, stmt, stmt.CTEs) {
			kept = append(kept, cte)
		}
	}
	stmt.CTEs = kept
	return stmt
}

// cteIsInlineable reports whether cte can be merged directly into mainStmt
// instead of being materialised into a virtual table.
func cteIsInlineable(cte CTE, mainStmt Statement, allCTEs []CTE) bool {
	// Only inline when the CTE is the main FROM source, not a JOIN target.
	if mainStmt.TableName != cte.Name {
		return false
	}
	if !cteBodyIsInlineEligible(*cte.Body) {
		// Second chance: the body may have column aliases but the outer query
		// never references any of them (e.g. SELECT COUNT(*) FROM cte or
		// SELECT id FROM cte where id = ?).  Inlining is safe because the
		// merged query never names the aliased columns.
		if !cteBodyIsInlineEligibleIgnoreAliases(*cte.Body) {
			return false
		}
		if cteBodyAliasesConflictWithOuter(*cte.Body, mainStmt) {
			return false
		}
	}
	// Exactly one FROM/JOIN reference ensures the body isn't executed twice.
	if cteRefCount(cte.Name, mainStmt, allCTEs) != 1 {
		return false
	}
	// WHERE subquery references require the CTE to be in the registry,
	// which it won't be after inlining.
	if cteAppearsInConditionSubqueries(cte.Name, mainStmt.Conditions) {
		return false
	}
	return true
}

// inlineCTE merges cte.Body into mainStmt, returning a statement that scans
// the real underlying table directly. The caller must remove the inlined CTE
// from mainStmt.CTEs before executing the returned statement.
func inlineCTE(mainStmt Statement, cte CTE, outerAlias string) Statement {
	body := *cte.Body

	// Strip the outer CTE alias from all field references in the main statement
	// so that plain column names resolve against the real table.
	mainStmt = stripDerivedTableAliasPrefix(mainStmt, outerAlias)

	// When the outer SELECT is * but the CTE body projects a specific column
	// list, propagate the body's fields to avoid leaking extra columns from
	// the underlying table that the CTE did not expose.
	if mainStmt.IsSelectAll() && !body.IsSelectAll() && len(body.Fields) > 0 {
		mainStmt.Fields = body.Fields
	}

	// Merge the CTE body's WHERE conditions with the outer conditions using
	// AND semantics (DNF Cartesian product via mergeConditionsAND).
	mainStmt.Conditions = mergeConditionsAND(body.Conditions, mainStmt.Conditions)

	// Replace the CTE reference with the real underlying table.
	mainStmt.TableName = body.TableName
	mainStmt.TableAlias = ""

	return mainStmt
}
