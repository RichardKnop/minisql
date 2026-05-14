package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestContextWithUpdateFromRows(t *testing.T) {
	t.Parallel()

	cols := []Column{{Name: "id", Kind: Int8}}
	rows := []Row{
		NewRowWithValues(cols, []OptionalValue{{Valid: true, Value: int64(1)}}),
	}

	ctx := contextWithUpdateFromRows(context.Background(), rows)

	got, ok := updateFromRowsFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, rows, got)
}

func TestUpdateFromRowsFromContext_Missing(t *testing.T) {
	t.Parallel()

	_, ok := updateFromRowsFromContext(context.Background())
	assert.False(t, ok)
}

func TestUpdateFromRowsFromContext_EmptySlice(t *testing.T) {
	t.Parallel()

	ctx := contextWithUpdateFromRows(context.Background(), []Row{})
	got, ok := updateFromRowsFromContext(ctx)
	require.True(t, ok)
	assert.Empty(t, got)
}

func TestPrefixRowColumns(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "name", Kind: Text},
	}
	vals := []OptionalValue{
		{Valid: true, Value: int64(42)},
		{Valid: true, Value: NewTextPointer([]byte("Engineering"))},
	}
	row := NewRowWithValues(cols, vals)

	t.Run("applies alias prefix", func(t *testing.T) {
		t.Parallel()
		got := prefixRowColumns(row, "d")
		assert.Equal(t, "d.id", got.Columns[0].Name)
		assert.Equal(t, "d.name", got.Columns[1].Name)
		assert.Equal(t, vals[0], got.Values[0])
		assert.Equal(t, vals[1], got.Values[1])
	})

	t.Run("empty alias returns row unchanged", func(t *testing.T) {
		t.Parallel()
		got := prefixRowColumns(row, "")
		assert.Equal(t, "id", got.Columns[0].Name)
		assert.Equal(t, "name", got.Columns[1].Name)
	})

	t.Run("does not mutate original", func(t *testing.T) {
		t.Parallel()
		_ = prefixRowColumns(row, "x")
		assert.Equal(t, "id", row.Columns[0].Name)
		assert.Equal(t, "name", row.Columns[1].Name)
	})
}

func TestBuildUpdateFromMergedRow(t *testing.T) {
	t.Parallel()

	targetCols := []Column{
		{Name: "id", Kind: Int8},
		{Name: "dept_id", Kind: Int8},
	}
	targetVals := []OptionalValue{
		{Valid: true, Value: int64(1)},
		{Valid: true, Value: int64(10)},
	}
	targetRow := NewRowWithValues(targetCols, targetVals)

	fromCols := []Column{
		{Name: "emp.id", Kind: Int8},
		{Name: "emp.name", Kind: Text},
	}
	fromVals := []OptionalValue{
		{Valid: true, Value: int64(10)},
		{Valid: true, Value: NewTextPointer([]byte("Engineering"))},
	}
	fromRow := NewRowWithValues(fromCols, fromVals)

	merged := buildUpdateFromMergedRow(targetRow, "t", fromRow)

	// Target columns appear both as plain names and alias-qualified names.
	_, idxPlainID := merged.GetColumn("id")
	_, idxQualID := merged.GetColumn("t.id")
	_, idxPlainDept := merged.GetColumn("dept_id")
	_, idxQualDept := merged.GetColumn("t.dept_id")
	assert.GreaterOrEqual(t, idxPlainID, 0, "plain 'id' must exist")
	assert.GreaterOrEqual(t, idxQualID, 0, "qualified 't.id' must exist")
	assert.GreaterOrEqual(t, idxPlainDept, 0, "plain 'dept_id' must exist")
	assert.GreaterOrEqual(t, idxQualDept, 0, "qualified 't.dept_id' must exist")

	// FROM columns appear only under their alias-prefixed names.
	_, idxFromID := merged.GetColumn("emp.id")
	_, idxFromName := merged.GetColumn("emp.name")
	assert.GreaterOrEqual(t, idxFromID, 0, "from 'emp.id' must exist")
	assert.GreaterOrEqual(t, idxFromName, 0, "from 'emp.name' must exist")

	// Values are correct.
	col, i := merged.GetColumn("t.dept_id")
	require.NotNil(t, col)
	assert.Equal(t, int64(10), merged.Values[i].Value)

	col2, j := merged.GetColumn("emp.name")
	require.NotNil(t, col2)
	assert.Equal(t, NewTextPointer([]byte("Engineering")), merged.Values[j].Value)
}

func TestBuildUpdateFromMergedRow_ColumnCount(t *testing.T) {
	t.Parallel()

	targetCols := []Column{{Name: "a", Kind: Int8}, {Name: "b", Kind: Int8}}
	targetVals := []OptionalValue{{Valid: true, Value: int64(1)}, {Valid: true, Value: int64(2)}}
	targetRow := NewRowWithValues(targetCols, targetVals)

	fromCols := []Column{{Name: "x.c", Kind: Int8}}
	fromVals := []OptionalValue{{Valid: true, Value: int64(3)}}
	fromRow := NewRowWithValues(fromCols, fromVals)

	merged := buildUpdateFromMergedRow(targetRow, "tgt", fromRow)

	// 2 target cols × 2 (plain + qualified) + 1 FROM col = 5
	assert.Equal(t, 5, len(merged.Columns))
	assert.Equal(t, 5, len(merged.Values))
}

func TestResolveUpdateFromExprs(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "salary", Kind: Int8},
		{Name: "bonus", Kind: Int8},
	}
	mergedRow := NewRowWithValues(cols, []OptionalValue{
		{Valid: true, Value: int64(1000)},
		{Valid: true, Value: int64(200)},
	})

	t.Run("non-expr values are kept as-is", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Updates: map[string]OptionalValue{
				"salary": {Valid: true, Value: int64(9999)},
			},
		}
		resolved, err := resolveUpdateFromExprs(stmt, mergedRow)
		require.NoError(t, err)
		assert.Equal(t, int64(9999), resolved.Updates["salary"].Value)
	})

	t.Run("expr values are evaluated", func(t *testing.T) {
		t.Parallel()
		// salary + bonus => 1000 + 200 = 1200
		expr := &Expr{
			Left:  &Expr{Column: "salary"},
			Right: &Expr{Column: "bonus"},
			Op:    ArithAdd,
		}
		stmt := Statement{
			Updates: map[string]OptionalValue{
				"salary": {Valid: true, Value: expr},
			},
		}
		resolved, err := resolveUpdateFromExprs(stmt, mergedRow)
		require.NoError(t, err)
		assert.Equal(t, int64(1200), resolved.Updates["salary"].Value)
	})

	t.Run("expr evaluating to nil produces NULL", func(t *testing.T) {
		t.Parallel()
		// IsNull = true → Eval returns nil without error.
		expr := &Expr{IsNull: true}
		stmt := Statement{
			Updates: map[string]OptionalValue{
				"salary": {Valid: true, Value: expr},
			},
		}
		resolved, err := resolveUpdateFromExprs(stmt, mergedRow)
		require.NoError(t, err)
		assert.False(t, resolved.Updates["salary"].Valid, "NULL expr should resolve to NULL")
	})

	t.Run("empty updates returns stmt unchanged", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{}
		resolved, err := resolveUpdateFromExprs(stmt, mergedRow)
		require.NoError(t, err)
		assert.Nil(t, resolved.Updates)
	})
}

// updateFromTestDB creates a small two-table database suitable for testing
// executeUpdateFrom and materialiseFromSource.
//
//	employees(id INT8 PK, name VARCHAR, dept_id INT8)
//	departments(id INT8 PK, name VARCHAR)
func updateFromTestDB(t *testing.T) *Database {
	t.Helper()
	pager, dbFile := initTest(t)
	mockParser := new(MockParser)
	ctx := context.Background()

	db, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, pager, pager, nil)
	require.NoError(t, err)

	empCols := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "name", Nullable: true},
		{Kind: Int8, Size: 8, Name: "dept_id", Nullable: true},
	}
	deptCols := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "dept_name", Nullable: true},
	}

	empStmt := Statement{
		Kind:       CreateTable,
		TableName:  "employees",
		Columns:    empCols,
		PrimaryKey: NewPrimaryKey(PrimaryKeyName("employees"), empCols[0:1], true),
	}
	deptStmt := Statement{
		Kind:       CreateTable,
		TableName:  "departments",
		Columns:    deptCols,
		PrimaryKey: NewPrimaryKey(PrimaryKeyName("departments"), deptCols[0:1], true),
	}

	mockParser.On("Parse", mock.Anything, empStmt.DDL()).Return([]Statement{empStmt}, nil)
	mockParser.On("Parse", mock.Anything, deptStmt.DDL()).Return([]Statement{deptStmt}, nil)

	for _, s := range []Statement{empStmt, deptStmt} {
		stmt := s
		err = db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := db.ExecuteStatement(ctx, stmt)
			return err
		})
		require.NoError(t, err)
	}

	// Insert departments: id=1 → Engineering, id=2 → Sales
	err = db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, row := range [][]OptionalValue{
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("Engineering"))}},
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("Sales"))}},
		} {
			_, err := db.ExecuteStatement(ctx, Statement{
				Kind:      Insert,
				TableName: "departments",
				Columns:   deptCols,
				Fields:    fieldsFromColumns(deptCols...),
				Inserts:   [][]OptionalValue{row},
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Insert employees: id=1 name=Alice dept_id=0 (will be updated), id=2 name=Bob dept_id=2
	err = db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, row := range [][]OptionalValue{
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("Alice"))}, {Valid: true, Value: int64(0)}},
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("Bob"))}, {Valid: true, Value: int64(2)}},
		} {
			_, err := db.ExecuteStatement(ctx, Statement{
				Kind:      Insert,
				TableName: "employees",
				Columns:   empCols,
				Fields:    fieldsFromColumns(empCols...),
				Inserts:   [][]OptionalValue{row},
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	return db
}

func TestMaterialiseFromSource_Table(t *testing.T) {
	db := updateFromTestDB(t)
	ctx := context.Background()

	deptCols := db.tables["departments"].Columns
	stmt := Statement{
		UpdateFromTable: "departments",
		UpdateFromAlias: "d",
	}

	var rows []Row
	err := db.txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		var err error
		rows, err = db.materialiseFromSource(ctx, stmt)
		return err
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)

	// All column names must be alias-prefixed.
	for _, row := range rows {
		for _, col := range row.Columns {
			assert.True(t, len(col.Name) > 2 && col.Name[:2] == "d.", "column %q must be prefixed with 'd.'", col.Name)
		}
	}
	_ = deptCols
}

func TestMaterialiseFromSource_NonExistentTable(t *testing.T) {
	db := updateFromTestDB(t)
	ctx := context.Background()

	stmt := Statement{
		UpdateFromTable: "nonexistent",
		UpdateFromAlias: "x",
	}

	err := db.txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		_, err := db.materialiseFromSource(ctx, stmt)
		return err
	})
	require.Error(t, err)
}

func TestExecuteUpdateFrom_BasicJoin(t *testing.T) {
	db := updateFromTestDB(t)
	ctx := context.Background()

	empCols := db.tables["employees"].Columns

	// UPDATE employees AS e SET dept_id = 1 FROM departments AS d WHERE e.id = 1 AND d.id = 1
	stmt := Statement{
		Kind:            Update,
		TableName:       "employees",
		TableAlias:      "e",
		Columns:         empCols,
		UpdateFromTable: "departments",
		UpdateFromAlias: "d",
		Updates: map[string]OptionalValue{
			"dept_id": {Valid: true, Value: int64(1)},
		},
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id", AliasPrefix: "e"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandInteger, Value: int64(1)},
				},
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id", AliasPrefix: "d"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandInteger, Value: int64(1)},
				},
			},
		},
	}

	var result StatementResult
	err := db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		fromRows, err := db.materialiseFromSource(ctx, stmt)
		if err != nil {
			return err
		}
		ctx = contextWithUpdateFromRows(ctx, fromRows)
		result, err = db.executeUpdateFrom(ctx, stmt)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, 1, result.RowsAffected)
}

func TestExecuteUpdateFrom_NonExistentTargetTable(t *testing.T) {
	db := updateFromTestDB(t)
	ctx := context.Background()

	stmt := Statement{
		Kind:            Update,
		TableName:       "no_such_table",
		UpdateFromTable: "departments",
		UpdateFromAlias: "d",
		Updates:         map[string]OptionalValue{"x": {Valid: true, Value: int64(1)}},
	}

	err := db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		fromRows, _ := db.materialiseFromSource(ctx, Statement{
			UpdateFromTable: "departments",
			UpdateFromAlias: "d",
		})
		ctx = contextWithUpdateFromRows(ctx, fromRows)
		_, err := db.executeUpdateFrom(ctx, stmt)
		return err
	})
	require.Error(t, err)
}

func TestExecuteUpdateFrom_NoMatchingFromRow(t *testing.T) {
	db := updateFromTestDB(t)
	ctx := context.Background()

	empCols := db.tables["employees"].Columns

	// WHERE condition that matches no department → 0 rows updated
	stmt := Statement{
		Kind:            Update,
		TableName:       "employees",
		TableAlias:      "e",
		Columns:         empCols,
		UpdateFromTable: "departments",
		UpdateFromAlias: "d",
		Updates: map[string]OptionalValue{
			"dept_id": {Valid: true, Value: int64(99)},
		},
		Conditions: OneOrMore{
			{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id", AliasPrefix: "d"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandInteger, Value: int64(999)},
				},
			},
		},
	}

	var result StatementResult
	err := db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		fromRows, err := db.materialiseFromSource(ctx, stmt)
		if err != nil {
			return err
		}
		ctx = contextWithUpdateFromRows(ctx, fromRows)
		result, err = db.executeUpdateFrom(ctx, stmt)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, 0, result.RowsAffected)
}
