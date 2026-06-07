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
		assert.Empty(t, key)
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

func TestDeduplicateSortedRows(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "name", Kind: Varchar, Size: MaxInlineVarchar},
		{Name: "age", Kind: Int8, Size: 8},
	}
	nameField := Field{Name: "name"}
	bothFields := []Field{{Name: "name"}, {Name: "age"}}

	row := func(name string, age int64) Row {
		return NewRowWithValues(cols, []OptionalValue{
			{Value: NewTextPointer([]byte(name)), Valid: true},
			{Value: age, Valid: true},
		})
	}

	t.Run("empty input returns empty", func(t *testing.T) {
		t.Parallel()
		result := deduplicateSortedRows(nil, bothFields)
		assert.Empty(t, result)
	})

	t.Run("single row returned unchanged", func(t *testing.T) {
		t.Parallel()
		rows := []Row{row("alice", 30)}
		result := deduplicateSortedRows(rows, bothFields)
		assert.Len(t, result, 1)
	})

	t.Run("no duplicates all preserved", func(t *testing.T) {
		t.Parallel()
		rows := []Row{row("alice", 30), row("bob", 25), row("carol", 40)}
		result := deduplicateSortedRows(rows, bothFields)
		assert.Len(t, result, 3)
	})

	t.Run("adjacent duplicates removed", func(t *testing.T) {
		t.Parallel()
		rows := []Row{row("alice", 30), row("alice", 30), row("bob", 25)}
		result := deduplicateSortedRows(rows, bothFields)
		assert.Len(t, result, 2)
	})

	t.Run("all identical rows collapsed to one", func(t *testing.T) {
		t.Parallel()
		rows := []Row{row("alice", 30), row("alice", 30), row("alice", 30)}
		result := deduplicateSortedRows(rows, bothFields)
		assert.Len(t, result, 1)
	})

	t.Run("runs of duplicates each collapsed", func(t *testing.T) {
		t.Parallel()
		rows := []Row{
			row("alice", 30), row("alice", 30),
			row("bob", 25), row("bob", 25), row("bob", 25),
			row("carol", 40),
		}
		result := deduplicateSortedRows(rows, bothFields)
		assert.Len(t, result, 3)
	})

	t.Run("dedup on single field ignores second column difference", func(t *testing.T) {
		t.Parallel()
		// same name, different age — when only comparing on name they look equal
		rows := []Row{row("alice", 30), row("alice", 35)}
		result := deduplicateSortedRows(rows, []Field{nameField})
		assert.Len(t, result, 1)
	})

	t.Run("in-place: modifies and sub-slices original backing array", func(t *testing.T) {
		t.Parallel()
		rows := []Row{row("a", 1), row("a", 1), row("b", 2)}
		result := deduplicateSortedRows(rows, bothFields)
		assert.Len(t, result, 2)
		// result must be a sub-slice of the same backing array
		assert.Equal(t, &rows[0], &result[0])
	})
}

func TestDistinctExtendOrderBy(t *testing.T) {
	t.Parallel()

	emailField := Field{Name: "email"}
	nameField := Field{Name: "name"}
	ageField := Field{Name: "age"}
	exprField := Field{Name: "lower_name", Expr: &Expr{FuncName: "LOWER"}}

	orderByEmail := []OrderBy{{Field: emailField, Direction: Asc}}
	orderByName := []OrderBy{{Field: nameField, Direction: Desc}}

	t.Run("empty requestedFields returns orderBy unchanged", func(t *testing.T) {
		t.Parallel()
		result := distinctExtendOrderBy(orderByEmail, nil)
		assert.Equal(t, orderByEmail, result)
	})

	t.Run("all fields already in ORDER BY — no extension", func(t *testing.T) {
		t.Parallel()
		result := distinctExtendOrderBy(orderByEmail, []Field{emailField})
		assert.Len(t, result, 1)
		assert.Equal(t, "email", result[0].Field.Name)
	})

	t.Run("missing field appended as ascending tiebreaker", func(t *testing.T) {
		t.Parallel()
		result := distinctExtendOrderBy(orderByName, []Field{nameField, ageField})
		assert.Len(t, result, 2)
		assert.Equal(t, "name", result[0].Field.Name)
		assert.Equal(t, Desc, result[0].Direction)
		assert.Equal(t, "age", result[1].Field.Name)
		assert.Equal(t, Asc, result[1].Direction)
	})

	t.Run("expression fields skipped", func(t *testing.T) {
		t.Parallel()
		result := distinctExtendOrderBy(orderByName, []Field{nameField, exprField})
		assert.Len(t, result, 1, "expr field must not be appended")
	})

	t.Run("alias-qualified ORDER BY field not re-appended", func(t *testing.T) {
		t.Parallel()
		qualified := Field{Name: "email", AliasPrefix: "u"}
		orderBy := []OrderBy{{Field: qualified, Direction: Asc}}
		// requestedField without alias but same name — should still be skipped
		result := distinctExtendOrderBy(orderBy, []Field{emailField})
		// email is already in ORDER BY (bare name lookup), so no extension
		assert.Len(t, result, 1)
	})

	t.Run("multiple missing fields all appended", func(t *testing.T) {
		t.Parallel()
		result := distinctExtendOrderBy(nil, []Field{nameField, ageField, emailField})
		assert.Len(t, result, 3)
		assert.Equal(t, Asc, result[0].Direction)
		assert.Equal(t, Asc, result[1].Direction)
		assert.Equal(t, Asc, result[2].Direction)
	})

	t.Run("original ORDER BY direction preserved", func(t *testing.T) {
		t.Parallel()
		result := distinctExtendOrderBy(orderByName, []Field{nameField, emailField})
		assert.Equal(t, Desc, result[0].Direction)
		assert.Equal(t, Asc, result[1].Direction)
	})
}

func TestAllProjectedInOrderBy(t *testing.T) {
	t.Parallel()

	emailField := Field{Name: "email"}
	nameField := Field{Name: "name"}
	ageField := Field{Name: "age"}
	exprField := Field{Name: "lower_name", Expr: &Expr{FuncName: "LOWER"}}

	orderByEmail := []OrderBy{{Field: emailField, Direction: Asc}}
	orderByBoth := []OrderBy{{Field: emailField, Direction: Asc}, {Field: nameField, Direction: Asc}}

	t.Run("empty requestedFields returns false", func(t *testing.T) {
		t.Parallel()
		assert.False(t, allProjectedInOrderBy(nil, orderByEmail))
	})

	t.Run("empty orderBy returns false", func(t *testing.T) {
		t.Parallel()
		assert.False(t, allProjectedInOrderBy([]Field{emailField}, nil))
	})

	t.Run("all fields covered returns true", func(t *testing.T) {
		t.Parallel()
		assert.True(t, allProjectedInOrderBy([]Field{emailField}, orderByEmail))
	})

	t.Run("all fields covered multi-column returns true", func(t *testing.T) {
		t.Parallel()
		assert.True(t, allProjectedInOrderBy([]Field{emailField, nameField}, orderByBoth))
	})

	t.Run("uncovered field returns false", func(t *testing.T) {
		t.Parallel()
		assert.False(t, allProjectedInOrderBy([]Field{emailField, ageField}, orderByEmail))
	})

	t.Run("expression field returns false", func(t *testing.T) {
		t.Parallel()
		assert.False(t, allProjectedInOrderBy([]Field{exprField}, orderByEmail))
	})

	t.Run("alias-qualified ORDER BY covers bare requested field via bare-name entry", func(t *testing.T) {
		t.Parallel()
		qualifiedOB := []OrderBy{{Field: Field{Name: "email", AliasPrefix: "u"}, Direction: Asc}}
		// The map stores both "email" (bare) and "u.email" (qualified) for the ORDER BY clause.
		// A bare emailField lookup hits the bare-name entry → covered.
		assert.True(t, allProjectedInOrderBy([]Field{emailField}, qualifiedOB))
	})

	t.Run("alias-qualified requested field matched by alias-qualified ORDER BY", func(t *testing.T) {
		t.Parallel()
		qualifiedField := Field{Name: "email", AliasPrefix: "u"}
		qualifiedOB := []OrderBy{{Field: qualifiedField, Direction: Asc}}
		assert.True(t, allProjectedInOrderBy([]Field{qualifiedField}, qualifiedOB))
	})

	t.Run("single field not in ORDER BY returns false", func(t *testing.T) {
		t.Parallel()
		assert.False(t, allProjectedInOrderBy([]Field{ageField}, orderByEmail))
	})
}

func TestDistinctSeenCapacityFromEstimate(t *testing.T) {
	t.Parallel()

	t.Run("zero estimate returns zero", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, distinctSeenCapacityFromEstimate(0))
	})

	t.Run("negative estimate returns zero", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, distinctSeenCapacityFromEstimate(-1))
	})

	t.Run("small positive estimate returned as-is", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 100, distinctSeenCapacityFromEstimate(100))
	})

	t.Run("large estimate within int range returned as-is", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 10_000, distinctSeenCapacityFromEstimate(10_000))
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
