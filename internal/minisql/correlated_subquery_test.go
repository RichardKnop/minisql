package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── DB-backed tests for resolveSetSubqueries / executeScalarSetSubquery ──────

// corrSubqueryDB returns the same two-table DB as updateFromTestDB so that the
// correlated-subquery unit tests don't duplicate the setup code.
func corrSubqueryDB(t *testing.T) *Database {
	t.Helper()
	return updateFromTestDB(t)
}

func TestResolveSetSubqueries_NoSubqueries(t *testing.T) {
	// Fast path: no *Statement in Updates → context returned unchanged.
	db := corrSubqueryDB(t)
	stmt := &Statement{
		Kind:      Update,
		TableName: "employees",
		Updates: map[string]OptionalValue{
			"dept_id": MakeInt8(int64(1)),
		},
	}
	ctx := context.Background()
	newCtx, err := db.resolveSetSubqueries(ctx, stmt)
	require.NoError(t, err)
	_, ok := correlatedSetUpdatesFromContext(newCtx)
	assert.False(t, ok, "no correlatedSetUpdates should be in context when there are no subqueries")
	assert.Equal(t, int64(1), stmt.Updates["dept_id"].AsInt8(), "literal value must be unchanged")
}

func TestResolveSetSubqueries_TableNotFound(t *testing.T) {
	// Target table not in DB → function returns ctx unchanged without error.
	db := corrSubqueryDB(t)
	inner := &Statement{Kind: Select, TableName: "departments", Fields: []Field{{Name: "id"}}}
	stmt := &Statement{
		Kind:      Update,
		TableName: "no_such_table",
		Updates:   map[string]OptionalValue{"dept_id": MakeStatement(inner)},
	}
	ctx := context.Background()
	newCtx, err := db.resolveSetSubqueries(ctx, stmt)
	require.NoError(t, err)
	_, ok := correlatedSetUpdatesFromContext(newCtx)
	assert.False(t, ok)
}

func TestResolveSetSubqueries_NonCorrelated_OneRow(t *testing.T) {
	// Non-correlated subquery returning exactly one row → value substituted into stmt.Updates.
	db := corrSubqueryDB(t)
	// SELECT id FROM departments WHERE departments.id = 1  (qualified → no outer-table reference)
	// Using AliasPrefix "departments" prevents conditionsHaveOuterRef from treating the
	// unqualified "id" as an outer-table column match.
	inner := &Statement{
		Kind:      Select,
		TableName: "departments",
		Fields:    []Field{{Name: "id"}},
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id", AliasPrefix: "departments"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandInteger, Value: int64(1)},
				},
			},
		},
	}
	stmt := &Statement{
		Kind:      Update,
		TableName: "employees",
		Updates:   map[string]OptionalValue{"dept_id": MakeStatement(inner)},
	}
	ctx := context.Background()
	newCtx, err := db.resolveSetSubqueries(ctx, stmt)
	require.NoError(t, err)
	// The *Statement placeholder must be replaced with the scalar result (int64(1)).
	assert.Equal(t, int64(1), stmt.Updates["dept_id"].AsInt8())
	assert.True(t, stmt.Updates["dept_id"].IsValid())
	// Non-correlated path does not populate the context map.
	_, ok := correlatedSetUpdatesFromContext(newCtx)
	assert.False(t, ok)
}

func TestResolveSetSubqueries_NonCorrelated_NoRows(t *testing.T) {
	// Non-correlated subquery returning zero rows → NULL substituted.
	db := corrSubqueryDB(t)
	inner := &Statement{
		Kind:      Select,
		TableName: "departments",
		Fields:    []Field{{Name: "id"}},
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id", AliasPrefix: "departments"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandInteger, Value: int64(999)},
				},
			},
		},
	}
	stmt := &Statement{
		Kind:      Update,
		TableName: "employees",
		Updates:   map[string]OptionalValue{"dept_id": MakeStatement(inner)},
	}
	ctx := context.Background()
	_, err := db.resolveSetSubqueries(ctx, stmt)
	require.NoError(t, err)
	assert.True(t, stmt.Updates["dept_id"].IsNull(), "zero-row subquery should produce NULL")
}

func TestResolveSetSubqueries_NonCorrelated_MultipleRows(t *testing.T) {
	// Non-correlated subquery returning multiple rows → error.
	db := corrSubqueryDB(t)
	// SELECT id FROM departments (no WHERE → returns all 2 rows)
	inner := &Statement{
		Kind:      Select,
		TableName: "departments",
		Fields:    []Field{{Name: "id"}},
	}
	stmt := &Statement{
		Kind:      Update,
		TableName: "employees",
		Updates:   map[string]OptionalValue{"dept_id": MakeStatement(inner)},
	}
	ctx := context.Background()
	_, err := db.resolveSetSubqueries(ctx, stmt)
	require.Error(t, err, "multi-row scalar subquery must produce an error")
	assert.Contains(t, err.Error(), "more than one row")
}

func TestResolveSetSubqueries_Correlated(t *testing.T) {
	// Correlated subquery: per-row values pre-computed and stored in context.
	db := corrSubqueryDB(t)
	// inner: SELECT id FROM departments WHERE id = e.dept_id
	inner := &Statement{
		Kind:      Select,
		TableName: "departments",
		Fields:    []Field{{Name: "id"}},
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
					Operator: Eq,
					// e.dept_id — qualified reference to outer employees alias.
					Operand2: Operand{Type: OperandField, Value: Field{Name: "dept_id", AliasPrefix: "e"}},
				},
			},
		},
	}
	stmt := &Statement{
		Kind:       Update,
		TableName:  "employees",
		TableAlias: "e",
		Updates:    map[string]OptionalValue{"dept_id": MakeStatement(inner)},
	}
	ctx := context.Background()
	newCtx, err := db.resolveSetSubqueries(ctx, stmt)
	require.NoError(t, err)

	// correlatedSetUpdates must be in context.
	updates, ok := correlatedSetUpdatesFromContext(newCtx)
	require.True(t, ok)
	// One entry per employee row (2 employees in the test DB).
	assert.Len(t, updates, 2)

	// The *Statement placeholder must remain in stmt.Updates so that validateUpdate
	// can verify there is at least one field to update.
	assert.True(t, stmt.Updates["dept_id"].IsStatement(), "*Statement placeholder must not be removed from stmt.Updates")

	// Verify per-row results: Alice has dept_id=0 (no match → NULL),
	// Bob has dept_id=2 (matches dept id=2 → int64(2)).
	var nullCount, matchCount int
	for _, rowUpdates := range updates {
		v, exists := rowUpdates["dept_id"]
		if !exists {
			continue
		}
		if v.IsNull() {
			nullCount += 1
		} else {
			matchCount += 1
			assert.Equal(t, int64(2), v.AsInt8(), "matched dept id must be 2")
		}
	}
	assert.Equal(t, 1, nullCount, "Alice (dept_id=0) should have NULL result")
	assert.Equal(t, 1, matchCount, "Bob (dept_id=2) should have a matched result")
}

// ── pure helper tests ──────────────────────────────────────────────────────

func TestIsCorrelatedSetSubquery_Qualified(t *testing.T) {
	t.Parallel()

	outerCols := []Column{{Name: "id", Kind: Int8}, {Name: "dept_id", Kind: Int8}}

	inner := Statement{
		Kind:      Select,
		TableName: "depts",
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id", AliasPrefix: "depts"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandField, Value: Field{Name: "dept_id", AliasPrefix: "e"}},
				},
			},
		},
	}

	assert.True(t, isCorrelatedSetSubquery(inner, outerCols, "employees", "e"))
}

func TestIsCorrelatedSetSubquery_Unqualified(t *testing.T) {
	t.Parallel()

	outerCols := []Column{{Name: "dept_id", Kind: Int8}}

	inner := Statement{
		Kind:      Select,
		TableName: "depts",
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandField, Value: Field{Name: "dept_id"}},
				},
			},
		},
	}

	assert.True(t, isCorrelatedSetSubquery(inner, outerCols, "employees", "employees"))
}

func TestIsCorrelatedSetSubquery_NonCorrelated(t *testing.T) {
	t.Parallel()

	outerCols := []Column{{Name: "id", Kind: Int8}}

	inner := Statement{
		Kind:      Select,
		TableName: "depts",
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "active", AliasPrefix: "depts"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandBoolean, Value: true},
				},
			},
		},
	}

	assert.False(t, isCorrelatedSetSubquery(inner, outerCols, "employees", "employees"))
}

func TestIsCorrelatedSetSubquery_EmptyConditions(t *testing.T) {
	t.Parallel()
	outerCols := []Column{{Name: "id", Kind: Int8}}
	inner := Statement{Kind: Select, TableName: "depts"}
	assert.False(t, isCorrelatedSetSubquery(inner, outerCols, "employees", "employees"))
}

func TestBindOuterOperand_QualifiedRef(t *testing.T) {
	t.Parallel()

	outerCols := []Column{{Name: "dept_id", Kind: Int8}}
	outerRow := NewRowWithValues(outerCols, []OptionalValue{MakeInt8(int64(42))})

	op := Operand{Type: OperandField, Value: Field{Name: "dept_id", AliasPrefix: "e"}}
	bound := bindOuterOperand(op, outerRow, "employees", "e")

	assert.Equal(t, OperandInteger, bound.Type)
	assert.Equal(t, int64(42), bound.Value)
}

func TestBindOuterOperand_NonOuterRef(t *testing.T) {
	t.Parallel()

	outerCols := []Column{{Name: "dept_id", Kind: Int8}}
	outerRow := NewRowWithValues(outerCols, []OptionalValue{MakeInt8(int64(42))})

	// AliasPrefix is "depts" — refers to inner table, not outer.
	op := Operand{Type: OperandField, Value: Field{Name: "id", AliasPrefix: "depts"}}
	bound := bindOuterOperand(op, outerRow, "employees", "e")

	// Unchanged.
	assert.Equal(t, op, bound)
}

func TestBindOuterOperand_NullValue(t *testing.T) {
	t.Parallel()

	outerCols := []Column{{Name: "x", Kind: Int8}}
	outerRow := NewRowWithValues(outerCols, []OptionalValue{MakeNull()})

	op := Operand{Type: OperandField, Value: Field{Name: "x", AliasPrefix: "t"}}
	bound := bindOuterOperand(op, outerRow, "tbl", "t")

	assert.Equal(t, OperandNull, bound.Type)
}

func TestBindOuterRowToStatement(t *testing.T) {
	t.Parallel()

	outerCols := []Column{{Name: "dept_id", Kind: Int8}}
	outerRow := NewRowWithValues(outerCols, []OptionalValue{MakeInt8(int64(7))})

	inner := Statement{
		Kind:      Select,
		TableName: "depts",
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandField, Value: Field{Name: "dept_id", AliasPrefix: "e"}},
				},
			},
		},
	}

	bound := bindOuterRowToStatement(inner, outerRow, "employees", "e")

	// Operand2 should now be the concrete value 7, not a field ref.
	cond := bound.Conditions[0][0]
	assert.Equal(t, OperandInteger, cond.Operand2.Type)
	assert.Equal(t, int64(7), cond.Operand2.Value)

	// Original must not be mutated.
	assert.Equal(t, OperandField, inner.Conditions[0][0].Operand2.Type)
}

func TestOperandFromOptionalValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    OptionalValue
		wantType OperandType
		wantVal  any
	}{
		{"null", MakeNull(), OperandNull, nil},
		{"int64", MakeInt8(int64(5)), OperandInteger, int64(5)},
		{"int32", MakeInt4(int32(3)), OperandInteger, int64(3)},
		{"float64", MakeDouble(float64(1.5)), OperandFloat, float64(1.5)},
		{"float32", MakeReal(float32(2.5)), OperandFloat, float64(float32(2.5))},
		{"bool", MakeBool(true), OperandBoolean, true},
		{"TextPointer", MakeVarchar(NewTextPointer([]byte("hi"))), OperandQuotedString, "hi"},
		{"string", MakeVarchar(NewTextPointer([]byte("hello"))), OperandQuotedString, "hello"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := operandFromOptionalValue(tc.input)
			assert.Equal(t, tc.wantType, got.Type)
			if tc.wantVal != nil {
				assert.Equal(t, tc.wantVal, got.Value)
			}
		})
	}
}

func TestContextWithCorrelatedSetUpdates(t *testing.T) {
	t.Parallel()

	updates := correlatedSetUpdates{
		RowID(1): {"salary": MakeInt8(int64(5000))},
	}
	ctx := contextWithCorrelatedSetUpdates(context.Background(), updates)
	got, ok := correlatedSetUpdatesFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, int64(5000), got[RowID(1)]["salary"].AsInt8())
}

func TestCorrelatedSetUpdatesFromContext_Missing(t *testing.T) {
	t.Parallel()
	_, ok := correlatedSetUpdatesFromContext(context.Background())
	assert.False(t, ok)
}
