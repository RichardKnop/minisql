package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRowDistinctKey(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "name", Kind: Varchar},
		{Name: "active", Kind: Boolean},
		{Name: "score", Kind: Double},
	}

	t.Run("nil values produce null markers", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[:1], []OptionalValue{{Value: nil, Valid: false}})
		key := row.rowDistinctKey()
		assert.Contains(t, key, "null")
	})

	t.Run("int64 value", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[:1], []OptionalValue{{Value: int64(42), Valid: true}})
		key := row.rowDistinctKey()
		assert.Contains(t, key, "i64:42")
	})

	t.Run("bool value", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[2:3], []OptionalValue{{Value: true, Valid: true}})
		key := row.rowDistinctKey()
		assert.Contains(t, key, "b:true")
	})

	t.Run("float64 value", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[3:4], []OptionalValue{{Value: float64(3.14), Valid: true}})
		key := row.rowDistinctKey()
		assert.Contains(t, key, "f64:")
	})

	t.Run("TextPointer value", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[1:2], []OptionalValue{{Value: NewTextPointer([]byte("hello")), Valid: true}})
		key := row.rowDistinctKey()
		assert.Contains(t, key, "hello")
	})

	t.Run("int64 and float64 same value have different keys", func(t *testing.T) {
		t.Parallel()
		intRow := NewRowWithValues(cols[:1], []OptionalValue{{Value: int64(1), Valid: true}})
		floatRow := NewRowWithValues(cols[:1], []OptionalValue{{Value: float64(1), Valid: true}})
		assert.NotEqual(t, intRow.rowDistinctKey(), floatRow.rowDistinctKey())
	})

	t.Run("multiple columns use separator", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[:2], []OptionalValue{
			{Value: int64(1), Valid: true},
			{Value: NewTextPointer([]byte("alice")), Valid: true},
		})
		key := row.rowDistinctKey()
		assert.Contains(t, key, "i64:1")
		assert.Contains(t, key, "alice")
	})
}

func TestDeduplicateRows(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: MaxInlineVarchar},
	}
	fields := []Field{
		{Name: "id"},
		{Name: "name"},
	}

	row := func(id int64, name string) Row {
		return NewRowWithValues(cols, []OptionalValue{
			{Value: id, Valid: true},
			{Value: NewTextPointer([]byte(name)), Valid: true},
		})
	}

	t.Run("no duplicates returns all rows", func(t *testing.T) {
		t.Parallel()
		rows := []Row{row(1, "alice"), row(2, "bob"), row(3, "carol")}
		result := deduplicateRows(rows, fields)
		assert.Len(t, result, 3)
	})

	t.Run("exact duplicates deduplicated", func(t *testing.T) {
		t.Parallel()
		rows := []Row{row(1, "alice"), row(1, "alice"), row(2, "bob")}
		result := deduplicateRows(rows, fields)
		assert.Len(t, result, 2)
	})

	t.Run("preserves first occurrence", func(t *testing.T) {
		t.Parallel()
		rows := []Row{row(1, "alice"), row(1, "alice")}
		result := deduplicateRows(rows, fields)
		assert.Len(t, result, 1)
		assert.Equal(t, int64(1), result[0].Values[0].Value)
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		t.Parallel()
		result := deduplicateRows(nil, fields)
		assert.Empty(t, result)
	})
}

func TestFindExpressionIndex(t *testing.T) {
	t.Parallel()

	exprLower := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
	exprUpper := &Expr{FuncName: "UPPER", Args: []*Expr{{Column: "name"}}}

	table := NewTable(zap.NewNop(), nil, nil, "t", testColumns, 0, nil)
	table.SecondaryIndexes["idx_lower_name"] = SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:       "idx_lower_name",
			Expression: exprLower,
		},
	}

	t.Run("found matching expression index", func(t *testing.T) {
		t.Parallel()
		// Build an identical (but different pointer) expression
		query := &Expr{FuncName: "LOWER", Args: []*Expr{{Column: "name"}}}
		si, ok := table.FindExpressionIndex(query)
		assert.True(t, ok)
		assert.Equal(t, "idx_lower_name", si.Name)
	})

	t.Run("no match returns false", func(t *testing.T) {
		t.Parallel()
		_, ok := table.FindExpressionIndex(exprUpper)
		assert.False(t, ok)
	})

	t.Run("nil expression no match", func(t *testing.T) {
		t.Parallel()
		_, ok := table.FindExpressionIndex(nil)
		assert.False(t, ok)
	})

	t.Run("table with no expression indexes", func(t *testing.T) {
		t.Parallel()
		empty := NewTable(zap.NewNop(), nil, nil, "empty", testColumns, 0, nil)
		_, ok := empty.FindExpressionIndex(exprLower)
		assert.False(t, ok)
	})
}

func TestBuildGroupKey(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "age", Kind: Int8},
		{Name: "name", Kind: Varchar},
		{Name: "active", Kind: Boolean},
		{Name: "score", Kind: Double},
		{Name: "ratio", Kind: Real},
	}

	makeRow := func(vals ...OptionalValue) Row {
		return NewRowWithValues(cols[:len(vals)], vals)
	}

	t.Run("int64 encoded with i64 prefix", func(t *testing.T) {
		t.Parallel()
		row := makeRow(OptionalValue{Value: int64(42), Valid: true})
		key := string(buildGroupKey(nil, row, []int{0}))
		assert.Equal(t, "i64:42", key)
	})

	t.Run("int32 encoded with i32 prefix", func(t *testing.T) {
		t.Parallel()
		row := makeRow(OptionalValue{Value: int32(7), Valid: true})
		key := string(buildGroupKey(nil, row, []int{0}))
		assert.Equal(t, "i32:7", key)
	})

	t.Run("bool true encoded as b:true", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[2:3], []OptionalValue{{Value: true, Valid: true}})
		key := string(buildGroupKey(nil, row, []int{0}))
		assert.Equal(t, "b:true", key)
	})

	t.Run("bool false encoded as b:false", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[2:3], []OptionalValue{{Value: false, Valid: true}})
		key := string(buildGroupKey(nil, row, []int{0}))
		assert.Equal(t, "b:false", key)
	})

	t.Run("float64 encoded with f64 prefix", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[3:4], []OptionalValue{{Value: float64(3.14), Valid: true}})
		key := string(buildGroupKey(nil, row, []int{0}))
		assert.Contains(t, key, "f64:")
	})

	t.Run("float32 encoded with f32 prefix", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[4:5], []OptionalValue{{Value: float32(1.5), Valid: true}})
		key := string(buildGroupKey(nil, row, []int{0}))
		assert.Contains(t, key, "f32:")
	})

	t.Run("TextPointer encoded with t prefix", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[1:2], []OptionalValue{{Value: NewTextPointer([]byte("hello")), Valid: true}})
		key := string(buildGroupKey(nil, row, []int{0}))
		assert.Contains(t, key, "t5:hello")
	})

	t.Run("null value encoded as null", func(t *testing.T) {
		t.Parallel()
		row := makeRow(OptionalValue{Valid: false})
		key := string(buildGroupKey(nil, row, []int{0}))
		assert.Equal(t, "null", key)
	})

	t.Run("out-of-range index encoded as null", func(t *testing.T) {
		t.Parallel()
		row := makeRow(OptionalValue{Value: int64(1), Valid: true})
		key := string(buildGroupKey(nil, row, []int{99}))
		assert.Equal(t, "null", key)
	})

	t.Run("negative index encoded as null", func(t *testing.T) {
		t.Parallel()
		row := makeRow(OptionalValue{Value: int64(1), Valid: true})
		key := string(buildGroupKey(nil, row, []int{-1}))
		assert.Equal(t, "null", key)
	})

	t.Run("multiple columns separated by unit separator", func(t *testing.T) {
		t.Parallel()
		row := NewRowWithValues(cols[:2], []OptionalValue{
			{Value: int64(1), Valid: true},
			{Value: NewTextPointer([]byte("alice")), Valid: true},
		})
		key := string(buildGroupKey(nil, row, []int{0, 1}))
		assert.Equal(t, "i64:1\x1ft5:alice", key)
	})

	t.Run("int64 and float64 with same value produce distinct keys", func(t *testing.T) {
		t.Parallel()
		r1 := makeRow(OptionalValue{Value: int64(1), Valid: true})
		r2 := NewRowWithValues(cols[3:4], []OptionalValue{{Value: float64(1), Valid: true}})
		k1 := string(buildGroupKey(nil, r1, []int{0}))
		k2 := string(buildGroupKey(nil, r2, []int{0}))
		assert.NotEqual(t, k1, k2)
	})

	t.Run("buf is reused across calls", func(t *testing.T) {
		t.Parallel()
		row := makeRow(OptionalValue{Value: int64(5), Valid: true})
		buf := make([]byte, 0, 64)
		buf = buildGroupKey(buf[:0], row, []int{0})
		assert.Equal(t, "i64:5", string(buf))
		buf = buildGroupKey(buf[:0], row, []int{0})
		assert.Equal(t, "i64:5", string(buf))
	})

	t.Run("empty column index list produces empty key", func(t *testing.T) {
		t.Parallel()
		row := makeRow(OptionalValue{Value: int64(1), Valid: true})
		key := string(buildGroupKey(nil, row, []int{}))
		assert.Equal(t, "", key)
	})
}

func TestGroupByColumnIndex(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "name", Kind: Varchar},
		{Name: "age", Kind: Int8},
	}

	t.Run("plain name found at correct index", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 1, groupByColumnIndex(cols, Field{Name: "name"}))
	})

	t.Run("first column found", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, groupByColumnIndex(cols, Field{Name: "id"}))
	})

	t.Run("last column found", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 2, groupByColumnIndex(cols, Field{Name: "age"}))
	})

	t.Run("missing name returns -1", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, -1, groupByColumnIndex(cols, Field{Name: "score"}))
	})

	t.Run("alias-qualified name falls back to bare name when column not qualified", func(t *testing.T) {
		t.Parallel()
		// Column is stored as plain "name", not "t.name" — expect fallback.
		assert.Equal(t, 1, groupByColumnIndex(cols, Field{Name: "name", AliasPrefix: "t"}))
	})

	t.Run("alias-qualified name found when column is stored qualified", func(t *testing.T) {
		t.Parallel()
		qualifiedCols := []Column{
			{Name: "t.id", Kind: Int8},
			{Name: "t.name", Kind: Varchar},
		}
		assert.Equal(t, 1, groupByColumnIndex(qualifiedCols, Field{Name: "name", AliasPrefix: "t"}))
	})

	t.Run("empty cols returns -1", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, -1, groupByColumnIndex(nil, Field{Name: "id"}))
	})
}

func TestWhereCondColumns(t *testing.T) {
	t.Parallel()

	t.Run("empty WhereCond returns nil", func(t *testing.T) {
		t.Parallel()
		ii := IndexInfo{}
		assert.Nil(t, ii.WhereCondColumns())
	})

	t.Run("single field condition", func(t *testing.T) {
		t.Parallel()
		ii := IndexInfo{
			WhereCond: OneOrMore{
				{
					Condition{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "active"}},
						Operator: Eq,
						Operand2: Operand{Type: OperandBoolean, Value: true},
					},
				},
			},
		}
		cols := ii.WhereCondColumns()
		assert.Equal(t, []string{"active"}, cols)
	})

	t.Run("deduplicated across groups", func(t *testing.T) {
		t.Parallel()
		ii := IndexInfo{
			WhereCond: OneOrMore{
				{
					Condition{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
						Operator: Eq,
						Operand2: Operand{Type: OperandInteger, Value: int64(1)},
					},
				},
				{
					Condition{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
						Operator: Eq,
						Operand2: Operand{Type: OperandInteger, Value: int64(2)},
					},
				},
			},
		}
		cols := ii.WhereCondColumns()
		assert.Equal(t, []string{"status"}, cols)
	})

	t.Run("non-field operand skipped", func(t *testing.T) {
		t.Parallel()
		ii := IndexInfo{
			WhereCond: OneOrMore{
				{
					Condition{
						Operand1: Operand{Type: OperandInteger, Value: int64(1)},
						Operator: Eq,
						Operand2: Operand{Type: OperandInteger, Value: int64(1)},
					},
				},
			},
		}
		assert.Nil(t, ii.WhereCondColumns())
	})
}
