package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helpers for building test operands / conditions

func fieldOp(alias, name string) Operand {
	return Operand{Type: OperandField, Value: Field{AliasPrefix: alias, Name: name}}
}

func intOp(v int64) Operand {
	return Operand{Type: OperandInteger, Value: v}
}

func nullOp() Operand {
	return Operand{Type: OperandNull}
}

func mkCond(op1 Operand, operator Operator, op2 Operand) Condition {
	return Condition{Operand1: op1, Operator: operator, Operand2: op2}
}

// ──────────────────────────────────────────────────────────────────
// mergeConditionsAND
// ──────────────────────────────────────────────────────────────────

func TestMergeConditionsAND_BothEmpty(t *testing.T) {
	t.Parallel()
	assert.Nil(t, mergeConditionsAND(nil, nil))
}

func TestMergeConditionsAND_ExistingEmpty(t *testing.T) {
	t.Parallel()
	pushed := OneOrMore{{mkCond(fieldOp("", "a"), Eq, intOp(1))}}
	result := mergeConditionsAND(nil, pushed)
	assert.Equal(t, pushed, result)
}

func TestMergeConditionsAND_PushedEmpty(t *testing.T) {
	t.Parallel()
	existing := OneOrMore{{mkCond(fieldOp("", "a"), Eq, intOp(1))}}
	result := mergeConditionsAND(existing, nil)
	assert.Equal(t, existing, result)
}

func TestMergeConditionsAND_CartesianProduct(t *testing.T) {
	t.Parallel()
	// existing = [A] OR [B], pushed = [P] OR [Q]
	// result = [A,P] OR [A,Q] OR [B,P] OR [B,Q]
	ca := mkCond(fieldOp("", "a"), Eq, intOp(1))
	cb := mkCond(fieldOp("", "b"), Eq, intOp(2))
	cp := mkCond(fieldOp("", "p"), Gt, intOp(10))
	cq := mkCond(fieldOp("", "q"), Lt, intOp(20))

	existing := OneOrMore{{ca}, {cb}}
	pushed := OneOrMore{{cp}, {cq}}
	result := mergeConditionsAND(existing, pushed)

	require.Len(t, result, 4)
	assert.Equal(t, Conditions{ca, cp}, result[0])
	assert.Equal(t, Conditions{ca, cq}, result[1])
	assert.Equal(t, Conditions{cb, cp}, result[2])
	assert.Equal(t, Conditions{cb, cq}, result[3])
}

func TestMergeConditionsAND_SingleGroups(t *testing.T) {
	t.Parallel()
	ca := mkCond(fieldOp("", "x"), Eq, intOp(5))
	cp := mkCond(fieldOp("", "y"), Gt, intOp(0))
	result := mergeConditionsAND(OneOrMore{{ca}}, OneOrMore{{cp}})
	require.Len(t, result, 1)
	assert.Equal(t, Conditions{ca, cp}, result[0])
}

// ──────────────────────────────────────────────────────────────────
// innerOutputColumns
// ──────────────────────────────────────────────────────────────────

func TestInnerOutputColumns_NoFields(t *testing.T) {
	t.Parallel()
	assert.Nil(t, innerOutputColumns(Statement{}))
}

func TestInnerOutputColumns_WildcardField(t *testing.T) {
	t.Parallel()
	stmt := Statement{Fields: []Field{{Name: "*"}}}
	assert.Nil(t, innerOutputColumns(stmt))
}

func TestInnerOutputColumns_SpecificFields(t *testing.T) {
	t.Parallel()
	stmt := Statement{Fields: []Field{{Name: "id"}, {Name: "score"}}}
	cols := innerOutputColumns(stmt)
	require.NotNil(t, cols)
	assert.True(t, cols["id"])
	assert.True(t, cols["score"])
	assert.False(t, cols["name"])
}

func TestInnerOutputColumns_AliasedField(t *testing.T) {
	t.Parallel()
	// "score AS points" → OutputName() = "points"
	stmt := Statement{Fields: []Field{{Name: "score", Alias: "points"}}}
	cols := innerOutputColumns(stmt)
	require.NotNil(t, cols)
	assert.True(t, cols["points"])
	assert.False(t, cols["score"])
}

// ──────────────────────────────────────────────────────────────────
// innerIsPushdownEligible
// ──────────────────────────────────────────────────────────────────

func TestInnerIsPushdownEligible_Plain(t *testing.T) {
	t.Parallel()
	assert.True(t, innerIsPushdownEligible(Statement{}))
}

func TestInnerIsPushdownEligible_GroupBy(t *testing.T) {
	t.Parallel()
	assert.False(t, innerIsPushdownEligible(Statement{GroupBy: []Field{{Name: "x"}}}))
}

func TestInnerIsPushdownEligible_Aggregates(t *testing.T) {
	t.Parallel()
	assert.False(t, innerIsPushdownEligible(Statement{Aggregates: []AggregateExpr{{Column: "id"}}}))
}

func TestInnerIsPushdownEligible_Having(t *testing.T) {
	t.Parallel()
	assert.False(t, innerIsPushdownEligible(Statement{Having: OneOrMore{{mkCond(fieldOp("", "cnt"), Gt, intOp(1))}}}))
}

func TestInnerIsPushdownEligible_Limit(t *testing.T) {
	t.Parallel()
	assert.False(t, innerIsPushdownEligible(Statement{Limit: OptionalValue{Valid: true, Value: int64(10)}}))
}

func TestInnerIsPushdownEligible_Offset(t *testing.T) {
	t.Parallel()
	assert.False(t, innerIsPushdownEligible(Statement{Offset: OptionalValue{Valid: true, Value: int64(5)}}))
}

func TestInnerIsPushdownEligible_Union(t *testing.T) {
	t.Parallel()
	assert.False(t, innerIsPushdownEligible(Statement{Unions: []UnionClause{{Stmt: Statement{}}}}))
}

// ──────────────────────────────────────────────────────────────────
// operandReferencesOnlyInner
// ──────────────────────────────────────────────────────────────────

func TestOperandReferencesOnlyInner_Literal(t *testing.T) {
	t.Parallel()
	assert.True(t, operandReferencesOnlyInner(intOp(42), "t", map[string]bool{"x": true}))
}

func TestOperandReferencesOnlyInner_Null(t *testing.T) {
	t.Parallel()
	assert.True(t, operandReferencesOnlyInner(nullOp(), "t", nil))
}

func TestOperandReferencesOnlyInner_FieldMatchesAlias(t *testing.T) {
	t.Parallel()
	innerCols := map[string]bool{"score": true}
	assert.True(t, operandReferencesOnlyInner(fieldOp("t", "score"), "t", innerCols))
}

func TestOperandReferencesOnlyInner_FieldNotInInnerCols(t *testing.T) {
	t.Parallel()
	innerCols := map[string]bool{"id": true}
	assert.False(t, operandReferencesOnlyInner(fieldOp("t", "score"), "t", innerCols))
}

func TestOperandReferencesOnlyInner_FieldDifferentAlias(t *testing.T) {
	t.Parallel()
	innerCols := map[string]bool{"id": true}
	assert.False(t, operandReferencesOnlyInner(fieldOp("other", "id"), "t", innerCols))
}

func TestOperandReferencesOnlyInner_FieldNoAlias_SelectStar(t *testing.T) {
	t.Parallel()
	// innerCols nil = SELECT * → any column eligible
	assert.True(t, operandReferencesOnlyInner(fieldOp("", "anything"), "t", nil))
}

func TestOperandReferencesOnlyInner_FieldNoAlias_NotInInnerCols(t *testing.T) {
	t.Parallel()
	innerCols := map[string]bool{"id": true}
	assert.False(t, operandReferencesOnlyInner(fieldOp("", "score"), "t", innerCols))
}

func TestOperandReferencesOnlyInner_FieldNoAlias_InInnerCols(t *testing.T) {
	t.Parallel()
	innerCols := map[string]bool{"score": true}
	assert.True(t, operandReferencesOnlyInner(fieldOp("", "score"), "t", innerCols))
}

// ──────────────────────────────────────────────────────────────────
// groupReferencesOnlyInner
// ──────────────────────────────────────────────────────────────────

func TestGroupReferencesOnlyInner_Empty(t *testing.T) {
	t.Parallel()
	assert.True(t, groupReferencesOnlyInner(Conditions{}, "t", nil))
}

func TestGroupReferencesOnlyInner_AllInner(t *testing.T) {
	t.Parallel()
	innerCols := map[string]bool{"score": true, "id": true}
	group := Conditions{
		mkCond(fieldOp("t", "score"), Gt, intOp(80)),
		mkCond(fieldOp("t", "id"), Eq, intOp(1)),
	}
	assert.True(t, groupReferencesOnlyInner(group, "t", innerCols))
}

func TestGroupReferencesOnlyInner_OneOuterRef(t *testing.T) {
	t.Parallel()
	innerCols := map[string]bool{"score": true}
	group := Conditions{
		mkCond(fieldOp("t", "score"), Gt, intOp(80)),
		mkCond(fieldOp("other", "id"), Eq, intOp(1)),
	}
	assert.False(t, groupReferencesOnlyInner(group, "t", innerCols))
}

// ──────────────────────────────────────────────────────────────────
// pushIntoInner
// ──────────────────────────────────────────────────────────────────

func TestPushIntoInner_EmptyOuterConds(t *testing.T) {
	t.Parallel()
	inner := Statement{TableName: "users", Fields: []Field{{Name: "id"}}}
	got, remaining := pushIntoInner(nil, inner, "t")
	assert.Nil(t, got.Conditions)
	assert.Nil(t, remaining)
}

func TestPushIntoInner_IneligibleInner_GroupBy(t *testing.T) {
	t.Parallel()
	inner := Statement{
		TableName: "users",
		Fields:    []Field{{Name: "id"}},
		GroupBy:   []Field{{Name: "id"}},
	}
	outerConds := OneOrMore{{mkCond(fieldOp("t", "id"), Eq, intOp(1))}}
	got, remaining := pushIntoInner(outerConds, inner, "t")
	assert.Nil(t, got.Conditions)
	assert.Equal(t, outerConds, remaining)
}

func TestPushIntoInner_AllPushed(t *testing.T) {
	t.Parallel()
	inner := Statement{
		TableName: "users",
		Fields:    []Field{{Name: "id"}, {Name: "score"}},
	}
	outerConds := OneOrMore{{mkCond(fieldOp("t", "score"), Gt, intOp(80))}}
	got, remaining := pushIntoInner(outerConds, inner, "t")

	assert.Nil(t, remaining)
	require.Len(t, got.Conditions, 1)
	require.Len(t, got.Conditions[0], 1)
	f := got.Conditions[0][0].Operand1.Value.(Field)
	assert.Empty(t, f.AliasPrefix, "alias prefix should be stripped")
	assert.Equal(t, "score", f.Name)
}

func TestPushIntoInner_NoPushable(t *testing.T) {
	t.Parallel()
	inner := Statement{
		TableName: "users",
		Fields:    []Field{{Name: "id"}},
	}
	outerConds := OneOrMore{{mkCond(fieldOp("other", "id"), Eq, intOp(1))}}
	got, remaining := pushIntoInner(outerConds, inner, "t")
	assert.Nil(t, got.Conditions)
	assert.Equal(t, outerConds, remaining)
}

func TestPushIntoInner_PartialPush(t *testing.T) {
	t.Parallel()
	inner := Statement{
		TableName: "users",
		Fields:    []Field{{Name: "id"}, {Name: "score"}},
	}
	cPushable := mkCond(fieldOp("t", "score"), Gt, intOp(80))
	cOuter := mkCond(fieldOp("other", "ref"), Eq, fieldOp("t", "id"))

	outerConds := OneOrMore{{cPushable}, {cOuter}}
	got, remaining := pushIntoInner(outerConds, inner, "t")

	require.Len(t, got.Conditions, 1)
	require.Len(t, remaining, 1)
	assert.Equal(t, cOuter, remaining[0][0])
}

func TestPushIntoInner_SelectStar_AllEligible(t *testing.T) {
	t.Parallel()
	// Inner has no explicit fields (SELECT *) → all outer conditions are eligible.
	inner := Statement{TableName: "users"}
	outerConds := OneOrMore{{mkCond(fieldOp("t", "anything"), Eq, intOp(0))}}
	got, remaining := pushIntoInner(outerConds, inner, "t")
	require.Len(t, got.Conditions, 1)
	assert.Nil(t, remaining)
}

func TestPushIntoInner_MergeWithExistingInnerConditions(t *testing.T) {
	t.Parallel()
	existingCond := mkCond(fieldOp("", "active"), Eq, intOp(1))
	inner := Statement{
		TableName:  "users",
		Fields:     []Field{{Name: "id"}, {Name: "active"}, {Name: "score"}},
		Conditions: OneOrMore{{existingCond}},
	}
	outerConds := OneOrMore{{mkCond(fieldOp("t", "score"), Gt, intOp(50))}}
	got, remaining := pushIntoInner(outerConds, inner, "t")

	assert.Nil(t, remaining)
	// mergeConditionsAND: 1 existing group × 1 pushed group → 1 combined group with 2 conds.
	require.Len(t, got.Conditions, 1)
	assert.Len(t, got.Conditions[0], 2)
}

func TestPushIntoInner_LiteralOperands_Eligible(t *testing.T) {
	t.Parallel()
	inner := Statement{
		TableName: "t",
		Fields:    []Field{{Name: "val"}},
	}
	// Both operands are literals — still counts as referencing only inner.
	outerConds := OneOrMore{{mkCond(intOp(1), Eq, intOp(1))}}
	got, remaining := pushIntoInner(outerConds, inner, "sub")
	require.Len(t, got.Conditions, 1)
	assert.Nil(t, remaining)
}
