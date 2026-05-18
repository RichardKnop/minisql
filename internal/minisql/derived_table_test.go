package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewVirtualTable(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "name", Kind: Text},
	}
	rows := []Row{
		NewRowWithValues(cols, []OptionalValue{MakeInt8(int64(1)), MakeVarchar(NewTextPointer([]byte("Alice")))}),
		NewRowWithValues(cols, []OptionalValue{MakeInt8(int64(2)), MakeVarchar(NewTextPointer([]byte("Bob")))}),
	}

	vt := newVirtualTable(zap.NewNop(), "t", cols, rows)

	assert.Equal(t, "t", vt.Name)
	assert.Equal(t, cols, vt.Columns)
	assert.Equal(t, rows, vt.virtualRows)
	assert.True(t, vt.HasNoIndex())

	// Column cache should be populated.
	idx, ok := vt.columnCache["id"]
	require.True(t, ok)
	assert.Equal(t, 0, idx)
	idx, ok = vt.columnCache["name"]
	require.True(t, ok)
	assert.Equal(t, 1, idx)

	// Provider should resolve the virtual table by alias.
	_, found := vt.provider.(*singleTableProvider).GetTable(t.Context(), "t")
	assert.True(t, found)
}

func TestStripFieldsAlias(t *testing.T) {
	t.Parallel()

	fields := []Field{
		{Name: "id", AliasPrefix: "t"},
		{Name: "name", AliasPrefix: "t"},
		{Name: "score", AliasPrefix: "u"}, // different alias — must not be stripped
	}

	got := stripFieldsAlias(fields, "t")

	assert.Equal(t, "", got[0].AliasPrefix)
	assert.Equal(t, "", got[1].AliasPrefix)
	assert.Equal(t, "u", got[2].AliasPrefix, "non-matching alias must be preserved")

	// Original slice must not be mutated.
	assert.Equal(t, "t", fields[0].AliasPrefix)
}

func TestStripFieldsAlias_Empty(t *testing.T) {
	t.Parallel()
	assert.Nil(t, stripFieldsAlias(nil, "t"))
	assert.Empty(t, stripFieldsAlias([]Field{}, "t"))
}

func TestStripConditionAlias(t *testing.T) {
	t.Parallel()

	cond := Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "cnt", AliasPrefix: "t"}},
		Operator: Gt,
		Operand2: Operand{Type: OperandInteger, Value: int64(5)},
	}

	got := stripConditionAlias(cond, "t")

	assert.Equal(t, "", got.Operand1.Value.(Field).AliasPrefix)
	assert.Equal(t, OperandInteger, got.Operand2.Type) // literal unchanged

	// Field-to-field condition: both sides have alias.
	cond2 := Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "a", AliasPrefix: "t"}},
		Operator: Eq,
		Operand2: Operand{Type: OperandField, Value: Field{Name: "b", AliasPrefix: "t"}},
	}
	got2 := stripConditionAlias(cond2, "t")
	assert.Equal(t, "", got2.Operand1.Value.(Field).AliasPrefix)
	assert.Equal(t, "", got2.Operand2.Value.(Field).AliasPrefix)
}

func TestStripConditionsAlias(t *testing.T) {
	t.Parallel()

	conds := OneOrMore{
		{
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "x", AliasPrefix: "sub"}},
				Operator: Eq,
				Operand2: Operand{Type: OperandInteger, Value: int64(1)},
			},
		},
	}

	got := stripConditionsAlias(conds, "sub")

	assert.Equal(t, "", got[0][0].Operand1.Value.(Field).AliasPrefix)
	// Original must not be mutated.
	assert.Equal(t, "sub", conds[0][0].Operand1.Value.(Field).AliasPrefix)
}

func TestStripOrderByAlias(t *testing.T) {
	t.Parallel()

	t.Run("strips matching alias", func(t *testing.T) {
		t.Parallel()
		ob := []OrderBy{
			{Field: Field{Name: "score", AliasPrefix: "t"}, Direction: Desc},
			{Field: Field{Name: "name", AliasPrefix: "other"}, Direction: Asc},
		}
		got := stripOrderByAlias(ob, "t")
		assert.Equal(t, "", got[0].Field.AliasPrefix)
		assert.Equal(t, Desc, got[0].Direction)
		assert.Equal(t, "other", got[1].Field.AliasPrefix, "non-matching alias preserved")
		// Original must not be mutated.
		assert.Equal(t, "t", ob[0].Field.AliasPrefix)
	})

	t.Run("nil input", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, stripOrderByAlias(nil, "t"))
	})

	t.Run("empty input", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, stripOrderByAlias([]OrderBy{}, "t"))
	})
}

func TestStripAggregatesAlias(t *testing.T) {
	t.Parallel()

	aggs := []AggregateExpr{
		{Kind: AggregateSum, Column: "t.amount"},
		{Kind: AggregateCount, Column: ""},
		{Kind: AggregateMax, Column: "other.price"}, // different prefix — must not strip
	}

	got := stripAggregatesAlias(aggs, "t")

	assert.Equal(t, "amount", got[0].Column)
	assert.Equal(t, "", got[1].Column)
	assert.Equal(t, "other.price", got[2].Column)

	// Original must not be mutated.
	assert.Equal(t, "t.amount", aggs[0].Column)
}

func TestVirtualSequentialScan(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "score", Kind: Int8},
	}
	rows := []Row{
		NewRowWithValues(cols, []OptionalValue{MakeInt8(int64(1)), MakeInt8(int64(90))}),
		NewRowWithValues(cols, []OptionalValue{MakeInt8(int64(2)), MakeInt8(int64(70))}),
		NewRowWithValues(cols, []OptionalValue{MakeInt8(int64(3)), MakeInt8(int64(85))}),
	}

	vt := newVirtualTable(zap.NewNop(), "t", cols, rows)
	ctx := context.Background()

	t.Run("no filter returns all rows", func(t *testing.T) {
		scan := Scan{TableName: "t", Type: ScanTypeSequential}
		var got []Row
		err := vt.sequentialScan(ctx, scan, nil, func(row Row) error {
			got = append(got, row)
			return nil
		})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("filter returns matching rows", func(t *testing.T) {
		scan := Scan{
			TableName: "t",
			Type:      ScanTypeSequential,
			Filters: OneOrMore{
				{
					{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
						Operator: Gt,
						Operand2: Operand{Type: OperandInteger, Value: int64(80)},
					},
				},
			},
		}
		var got []Row
		err := vt.sequentialScan(ctx, scan, nil, func(row Row) error {
			got = append(got, row)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, int64(90), got[0].Values[1].AsAny())
		assert.Equal(t, int64(85), got[1].Values[1].AsAny())
	})

	t.Run("empty virtual table returns no rows", func(t *testing.T) {
		empty := newVirtualTable(zap.NewNop(), "t", cols, nil)
		scan := Scan{TableName: "t", Type: ScanTypeSequential}
		var got []Row
		err := empty.sequentialScan(ctx, scan, nil, func(row Row) error {
			got = append(got, row)
			return nil
		})
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestStripDerivedTableAliasPrefix(t *testing.T) {
	t.Parallel()

	stmt := Statement{
		Fields: []Field{
			{Name: "name", AliasPrefix: "d"},
			{Name: "score", AliasPrefix: "d"},
		},
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "score", AliasPrefix: "d"}},
					Operator: Gt,
					Operand2: Operand{Type: OperandInteger, Value: int64(80)},
				},
			},
		},
		OrderBy: []OrderBy{
			{Field: Field{Name: "score", AliasPrefix: "d"}, Direction: Desc},
		},
		GroupBy: []Field{{Name: "name", AliasPrefix: "d"}},
	}

	got := stripDerivedTableAliasPrefix(stmt, "d")

	assert.Equal(t, "", got.Fields[0].AliasPrefix)
	assert.Equal(t, "", got.Fields[1].AliasPrefix)
	assert.Equal(t, "", got.Conditions[0][0].Operand1.Value.(Field).AliasPrefix)
	assert.Equal(t, "", got.OrderBy[0].Field.AliasPrefix)
	assert.Equal(t, "", got.GroupBy[0].AliasPrefix)

	// Original must not be mutated.
	assert.Equal(t, "d", stmt.Fields[0].AliasPrefix)
}
