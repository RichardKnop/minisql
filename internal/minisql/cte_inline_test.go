package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeSimpleBody builds a minimal CTE body statement for testing.
func makeSimpleBody(tableName string, conditions OneOrMore) Statement {
	return Statement{Kind: Select, TableName: tableName, Conditions: conditions}
}

// TestCTEBodyIsInlineEligible covers all eligibility guard conditions.
func TestCTEBodyIsInlineEligible(t *testing.T) {
	t.Parallel()

	t.Run("bare_table_scan", func(t *testing.T) {
		t.Parallel()
		assert.True(t, cteBodyIsInlineEligible(makeSimpleBody("users", nil)))
	})

	t.Run("with_where_clause", func(t *testing.T) {
		t.Parallel()
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
			Operator: Gt,
			Operand2: Operand{Type: OperandInteger, Value: int64(80)},
		}
		body := makeSimpleBody("users", OneOrMore{{cond}})
		assert.True(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_group_by", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("orders", nil)
		body.GroupBy = []Field{{Name: "user_id"}}
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_aggregate", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("orders", nil)
		body.Aggregates = []AggregateExpr{{Kind: AggregateCount}}
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_having", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("orders", nil)
		body.Having = OneOrMore{{}}
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_distinct", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.Distinct = true
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_limit", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.Limit = OptionalValue{Valid: true, Value: int64(10)}
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_offset", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.Offset = OptionalValue{Valid: true, Value: int64(5)}
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_union", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.Unions = []UnionClause{{}}
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_table_alias", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.TableAlias = "u"
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_from_subquery", func(t *testing.T) {
		t.Parallel()
		inner := makeSimpleBody("orders", nil)
		body := makeSimpleBody("", nil)
		body.FromSubquery = &inner
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("has_join", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.Joins = []Join{{TableName: "orders"}}
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("column_rename_alias", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.Fields = []Field{{Name: "user_id", Alias: "id"}}
		assert.False(t, cteBodyIsInlineEligible(body))
	})

	t.Run("select_star_no_alias", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.Fields = []Field{{Name: "*"}}
		assert.True(t, cteBodyIsInlineEligible(body))
	})

	t.Run("explicit_columns_no_rename", func(t *testing.T) {
		t.Parallel()
		body := makeSimpleBody("users", nil)
		body.Fields = []Field{{Name: "id"}, {Name: "name"}}
		assert.True(t, cteBodyIsInlineEligible(body))
	})
}

// TestCTERefCount verifies FROM and JOIN reference counting.
func TestCTERefCount(t *testing.T) {
	t.Parallel()

	allCTEs := []CTE{
		{Name: "t", Body: &Statement{TableName: "users"}},
	}

	t.Run("main_from_counts_one", func(t *testing.T) {
		t.Parallel()
		main := Statement{TableName: "t"}
		assert.Equal(t, 1, cteRefCount("t", main, allCTEs))
	})

	t.Run("join_ref_counts", func(t *testing.T) {
		t.Parallel()
		main := Statement{
			TableName: "orders",
			Joins:     []Join{{TableName: "t"}},
		}
		assert.Equal(t, 1, cteRefCount("t", main, allCTEs))
	})

	t.Run("from_and_join_counts_two", func(t *testing.T) {
		t.Parallel()
		main := Statement{
			TableName: "t",
			Joins:     []Join{{TableName: "t"}},
		}
		assert.Equal(t, 2, cteRefCount("t", main, allCTEs))
	})

	t.Run("other_cte_body_ref_counts", func(t *testing.T) {
		t.Parallel()
		ctes := []CTE{
			{Name: "a", Body: &Statement{TableName: "users"}},
			{Name: "b", Body: &Statement{TableName: "a"}}, // b references a
		}
		main := Statement{TableName: "a"}
		// FROM ref (1) + b.Body.TableName == "a" (1) = 2
		assert.Equal(t, 2, cteRefCount("a", main, ctes))
	})

	t.Run("not_referenced", func(t *testing.T) {
		t.Parallel()
		main := Statement{TableName: "orders"}
		assert.Equal(t, 0, cteRefCount("t", main, allCTEs))
	})
}

// TestCTEAppearsInConditionSubqueries verifies subquery detection in conditions.
func TestCTEAppearsInConditionSubqueries(t *testing.T) {
	t.Parallel()

	subStmt := &Statement{TableName: "cte1"}
	cond := Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
		Operator: In,
		Operand2: Operand{Type: OperandSubquery, Value: subStmt},
	}

	t.Run("present", func(t *testing.T) {
		t.Parallel()
		assert.True(t, cteAppearsInConditionSubqueries("cte1", OneOrMore{{cond}}))
	})

	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		assert.False(t, cteAppearsInConditionSubqueries("other", OneOrMore{{cond}}))
	})

	t.Run("empty_conditions", func(t *testing.T) {
		t.Parallel()
		assert.False(t, cteAppearsInConditionSubqueries("cte1", nil))
	})

	t.Run("non_subquery_operand", func(t *testing.T) {
		t.Parallel()
		plainCond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandInteger, Value: int64(1)},
		}
		assert.False(t, cteAppearsInConditionSubqueries("cte1", OneOrMore{{plainCond}}))
	})
}

// TestCTEBodyAliasesConflictWithOuter verifies the alias-conflict check used
// by the extended inlining pass.
func TestCTEBodyAliasesConflictWithOuter(t *testing.T) {
	t.Parallel()

	body := Statement{
		Fields: []Field{
			{Name: "id"},
			{Name: "name", Alias: "display_name"},
		},
	}

	t.Run("outer_count_star_no_conflict", func(t *testing.T) {
		t.Parallel()
		outer := Statement{Fields: []Field{{Name: "COUNT(*)"}}}
		assert.False(t, cteBodyAliasesConflictWithOuter(body, outer))
	})

	t.Run("outer_refs_alias_in_field", func(t *testing.T) {
		t.Parallel()
		outer := Statement{Fields: []Field{{Name: "display_name"}}}
		assert.True(t, cteBodyAliasesConflictWithOuter(body, outer))
	})

	t.Run("outer_refs_alias_in_condition", func(t *testing.T) {
		t.Parallel()
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "display_name"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandQuotedString, Value: "Alice"},
		}
		outer := Statement{Conditions: OneOrMore{{cond}}}
		assert.True(t, cteBodyAliasesConflictWithOuter(body, outer))
	})

	t.Run("outer_refs_alias_in_order_by", func(t *testing.T) {
		t.Parallel()
		outer := Statement{
			Fields:  []Field{{Name: "id"}},
			OrderBy: []OrderBy{{Field: Field{Name: "display_name"}}},
		}
		assert.True(t, cteBodyAliasesConflictWithOuter(body, outer))
	})

	t.Run("outer_refs_non_aliased_column_no_conflict", func(t *testing.T) {
		t.Parallel()
		outer := Statement{Fields: []Field{{Name: "id"}}}
		assert.False(t, cteBodyAliasesConflictWithOuter(body, outer))
	})

	t.Run("no_aliases_in_body_never_conflicts", func(t *testing.T) {
		t.Parallel()
		noAlias := Statement{Fields: []Field{{Name: "id"}, {Name: "name"}}}
		outer := Statement{Fields: []Field{{Name: "display_name"}}}
		assert.False(t, cteBodyAliasesConflictWithOuter(noAlias, outer))
	})
}

// TestCTEIsInlineableAliasExtension verifies the second-chance inlining path
// for CTE bodies with column aliases that the outer query doesn't reference.
func TestCTEIsInlineableAliasExtension(t *testing.T) {
	t.Parallel()

	makeAliasBody := func() *Statement {
		return &Statement{
			Kind:      Select,
			TableName: "bench_rows",
			Fields:    []Field{{Name: "id"}, {Name: "name", Alias: "display_name"}},
			Conditions: OneOrMore{{Condition{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
				Operator: Gte,
				Operand2: Operand{Type: OperandInteger, Value: int64(80)},
			}}},
		}
	}

	t.Run("count_star_outer_can_inline", func(t *testing.T) {
		t.Parallel()
		cte := CTE{Name: "seniors", Body: makeAliasBody()}
		outer := Statement{
			Kind:      Select,
			TableName: "seniors",
			Fields:    []Field{{Name: "COUNT(*)"}},
		}
		assert.True(t, cteIsInlineable(cte, outer, []CTE{cte}))
	})

	t.Run("outer_refs_alias_cannot_inline", func(t *testing.T) {
		t.Parallel()
		cte := CTE{Name: "seniors", Body: makeAliasBody()}
		outer := Statement{
			Kind:      Select,
			TableName: "seniors",
			Fields:    []Field{{Name: "display_name"}},
		}
		assert.False(t, cteIsInlineable(cte, outer, []CTE{cte}))
	})

	t.Run("outer_id_only_can_inline", func(t *testing.T) {
		t.Parallel()
		cte := CTE{Name: "seniors", Body: makeAliasBody()}
		outer := Statement{
			Kind:      Select,
			TableName: "seniors",
			Fields:    []Field{{Name: "id"}},
		}
		assert.True(t, cteIsInlineable(cte, outer, []CTE{cte}))
	})

	t.Run("group_by_still_blocks_inlining", func(t *testing.T) {
		t.Parallel()
		groupBody := &Statement{
			Kind:      Select,
			TableName: "bench_rows",
			Fields:    []Field{{Name: "age"}, {Name: "name", Alias: "display_name"}},
			GroupBy:   []Field{{Name: "age"}},
		}
		cte := CTE{Name: "summary", Body: groupBody}
		outer := Statement{
			Kind:      Select,
			TableName: "summary",
			Fields:    []Field{{Name: "COUNT(*)"}},
		}
		assert.False(t, cteIsInlineable(cte, outer, []CTE{cte}))
	})
}

// TestCTEIsUsed verifies the usage detector used for pruning.
func TestCTEIsUsed(t *testing.T) {
	t.Parallel()

	t.Run("used_as_main_from", func(t *testing.T) {
		t.Parallel()
		main := Statement{TableName: "t"}
		assert.True(t, cteIsUsed("t", main, nil))
	})

	t.Run("used_in_join", func(t *testing.T) {
		t.Parallel()
		main := Statement{TableName: "users", Joins: []Join{{TableName: "t"}}}
		assert.True(t, cteIsUsed("t", main, nil))
	})

	t.Run("used_in_where_subquery", func(t *testing.T) {
		t.Parallel()
		subStmt := &Statement{TableName: "t"}
		cond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
			Operator: In,
			Operand2: Operand{Type: OperandSubquery, Value: subStmt},
		}
		main := Statement{TableName: "orders", Conditions: OneOrMore{{cond}}}
		assert.True(t, cteIsUsed("t", main, nil))
	})

	t.Run("unused", func(t *testing.T) {
		t.Parallel()
		main := Statement{TableName: "users"}
		assert.False(t, cteIsUsed("orphan", main, nil))
	})
}

// TestPruneUnusedCTEs verifies unused CTE removal.
func TestPruneUnusedCTEs(t *testing.T) {
	t.Parallel()

	t.Run("prunes_unused", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			TableName: "used",
			CTEs: []CTE{
				{Name: "used", Body: &Statement{TableName: "users"}},
				{Name: "unused", Body: &Statement{TableName: "orders"}},
			},
		}
		result := pruneUnusedCTEs(stmt)
		require.Len(t, result.CTEs, 1)
		assert.Equal(t, "used", result.CTEs[0].Name)
	})

	t.Run("keeps_all_when_all_used", func(t *testing.T) {
		t.Parallel()
		subStmt := &Statement{TableName: "stats"}
		cond := Condition{
			Operand2: Operand{Type: OperandSubquery, Value: subStmt},
		}
		stmt := Statement{
			TableName:  "data",
			Conditions: OneOrMore{{cond}},
			CTEs: []CTE{
				{Name: "data", Body: &Statement{TableName: "users"}},
				{Name: "stats", Body: &Statement{TableName: "orders"}},
			},
		}
		result := pruneUnusedCTEs(stmt)
		assert.Len(t, result.CTEs, 2)
	})

	t.Run("empty_ctes_noop", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{TableName: "users"}
		result := pruneUnusedCTEs(stmt)
		assert.Empty(t, result.CTEs)
	})
}

// TestCTEIsInlineable verifies the combined inlineability check.
func TestCTEIsInlineable(t *testing.T) {
	t.Parallel()

	simpleCTE := CTE{Name: "t", Body: &Statement{Kind: Select, TableName: "users"}}
	allCTEs := []CTE{simpleCTE}

	t.Run("eligible_simple_cte", func(t *testing.T) {
		t.Parallel()
		main := Statement{TableName: "t"}
		assert.True(t, cteIsInlineable(simpleCTE, main, allCTEs))
	})

	t.Run("not_main_from", func(t *testing.T) {
		t.Parallel()
		main := Statement{TableName: "orders", Joins: []Join{{TableName: "t"}}}
		assert.False(t, cteIsInlineable(simpleCTE, main, allCTEs))
	})

	t.Run("body_has_group_by", func(t *testing.T) {
		t.Parallel()
		cte := CTE{Name: "t", Body: &Statement{
			Kind: Select, TableName: "orders",
			GroupBy: []Field{{Name: "user_id"}},
		}}
		main := Statement{TableName: "t"}
		assert.False(t, cteIsInlineable(cte, main, []CTE{cte}))
	})

	t.Run("referenced_more_than_once", func(t *testing.T) {
		t.Parallel()
		// CTE appears in both FROM and a JOIN.
		main := Statement{
			TableName: "t",
			Joins:     []Join{{TableName: "t"}},
		}
		assert.False(t, cteIsInlineable(simpleCTE, main, allCTEs))
	})

	t.Run("referenced_in_where_subquery", func(t *testing.T) {
		t.Parallel()
		subStmt := &Statement{TableName: "t"}
		cond := Condition{
			Operand2: Operand{Type: OperandSubquery, Value: subStmt},
		}
		main := Statement{TableName: "t", Conditions: OneOrMore{{cond}}}
		assert.False(t, cteIsInlineable(simpleCTE, main, allCTEs))
	})
}

// TestInlineCTE verifies the statement merge produced by inlineCTE.
func TestInlineCTE(t *testing.T) {
	t.Parallel()

	t.Run("basic_merge", func(t *testing.T) {
		t.Parallel()
		bodyCond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
			Operator: Gt,
			Operand2: Operand{Type: OperandInteger, Value: int64(80)},
		}
		cte := CTE{Name: "t", Body: &Statement{
			Kind: Select, TableName: "users",
			Conditions: OneOrMore{{bodyCond}},
		}}
		outerCond := Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "id", AliasPrefix: "t"}},
			Operator: Lt,
			Operand2: Operand{Type: OperandInteger, Value: int64(5)},
		}
		main := Statement{
			Kind:       Select,
			TableName:  "t",
			Fields:     []Field{{Name: "name", AliasPrefix: "t"}},
			Conditions: OneOrMore{{outerCond}},
		}

		merged := inlineCTE(main, cte, "t")

		assert.Equal(t, "users", merged.TableName)
		assert.Empty(t, merged.TableAlias)
		// Alias prefix on fields and conditions must be stripped.
		assert.Empty(t, merged.Fields[0].AliasPrefix)
		assert.Equal(t, "name", merged.Fields[0].Name)
		// Conditions: body cond AND outer cond merged.
		require.Len(t, merged.Conditions, 1)
		require.Len(t, merged.Conditions[0], 2)
		assert.Equal(t, "score", merged.Conditions[0][0].Operand1.Value.(Field).Name)
		assert.Equal(t, "id", merged.Conditions[0][1].Operand1.Value.(Field).Name)
		assert.Empty(t, merged.Conditions[0][1].Operand1.Value.(Field).AliasPrefix)
	})

	t.Run("outer_select_star_body_has_fields", func(t *testing.T) {
		t.Parallel()
		cte := CTE{Name: "t", Body: &Statement{
			Kind:      Select,
			TableName: "users",
			Fields:    []Field{{Name: "id"}, {Name: "name"}},
		}}
		main := Statement{
			Kind:      Select,
			TableName: "t",
			Fields:    []Field{{Name: "*"}},
		}

		merged := inlineCTE(main, cte, "t")

		// Body's field list propagated to prevent leaking extra columns.
		require.Len(t, merged.Fields, 2)
		assert.Equal(t, "id", merged.Fields[0].Name)
		assert.Equal(t, "name", merged.Fields[1].Name)
	})

	t.Run("outer_select_star_body_also_star", func(t *testing.T) {
		t.Parallel()
		cte := CTE{Name: "t", Body: &Statement{
			Kind:      Select,
			TableName: "users",
			Fields:    []Field{{Name: "*"}},
		}}
		main := Statement{
			Kind:      Select,
			TableName: "t",
			Fields:    []Field{{Name: "*"}},
		}

		merged := inlineCTE(main, cte, "t")

		// Both are SELECT * — keep SELECT * in the merged statement.
		require.Len(t, merged.Fields, 1)
		assert.Equal(t, "*", merged.Fields[0].Name)
	})
}
