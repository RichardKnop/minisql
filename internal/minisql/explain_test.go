package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoinTypeString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "inner", joinTypeString(Inner))
	assert.Equal(t, "left", joinTypeString(Left))
	assert.Equal(t, "right", joinTypeString(Right))
	assert.Equal(t, "unknown", joinTypeString(JoinType(99)))
}

func TestJoinDetail(t *testing.T) {
	t.Parallel()

	plan := QueryPlan{
		Scans: []Scan{
			{TableAlias: "u"},
			{TableAlias: "o"},
		},
	}

	t.Run("inner join with column pairs", func(t *testing.T) {
		t.Parallel()
		join := JoinPlan{
			Type:           Inner,
			LeftScanIndex:  0,
			RightScanIndex: 1,
			JoinColumnPairs: []JoinColumnPair{
				{
					BaseTableColumn: Field{Name: "id", AliasPrefix: "u"},
					JoinTableColumn: Field{Name: "user_id", AliasPrefix: "o"},
				},
			},
		}
		detail := joinDetail(plan, join)
		assert.Contains(t, detail, "type=inner")
		assert.Contains(t, detail, "left=u")
		assert.Contains(t, detail, "right=o")
		assert.Contains(t, detail, "on=u.id=o.user_id")
	})

	t.Run("left join with simple columns", func(t *testing.T) {
		t.Parallel()
		join := JoinPlan{
			Type:            Left,
			LeftScanIndex:   0,
			RightScanIndex:  1,
			OuterJoinColumn: "id",
			InnerJoinColumn: "user_id",
		}
		detail := joinDetail(plan, join)
		assert.Contains(t, detail, "type=left")
		assert.Contains(t, detail, "on=id=user_id")
	})

	t.Run("out-of-range scan indexes", func(t *testing.T) {
		t.Parallel()
		join := JoinPlan{Type: Right, LeftScanIndex: -1, RightScanIndex: 99}
		detail := joinDetail(plan, join)
		assert.Equal(t, "type=right algorithm=nested_loop", detail)
	})
}

func TestOrderByDetail(t *testing.T) {
	t.Parallel()

	t.Run("single ASC", func(t *testing.T) {
		t.Parallel()
		got := orderByDetail([]OrderBy{{Field: Field{Name: "name"}, Direction: Asc}})
		assert.Equal(t, "order_by=name ASC", got)
	})

	t.Run("single DESC", func(t *testing.T) {
		t.Parallel()
		got := orderByDetail([]OrderBy{{Field: Field{Name: "score"}, Direction: Desc}})
		assert.Equal(t, "order_by=score DESC", got)
	})

	t.Run("multiple columns", func(t *testing.T) {
		t.Parallel()
		got := orderByDetail([]OrderBy{
			{Field: Field{Name: "score"}, Direction: Desc},
			{Field: Field{Name: "name"}, Direction: Asc},
		})
		assert.Equal(t, "order_by=score DESC,name ASC", got)
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "order_by=", orderByDetail(nil))
	})
}

func TestRangeDetail(t *testing.T) {
	t.Parallel()

	t.Run("lower only exclusive", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{Lower: &RangeBound{Value: int64(10), Inclusive: false}}
		assert.Equal(t, "> 10", rangeDetail(rc))
	})

	t.Run("lower only inclusive", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{Lower: &RangeBound{Value: int64(10), Inclusive: true}}
		assert.Equal(t, ">= 10", rangeDetail(rc))
	})

	t.Run("upper only exclusive", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{Upper: &RangeBound{Value: int64(50), Inclusive: false}}
		assert.Equal(t, "< 50", rangeDetail(rc))
	})

	t.Run("upper only inclusive", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{Upper: &RangeBound{Value: int64(50), Inclusive: true}}
		assert.Equal(t, "<= 50", rangeDetail(rc))
	})

	t.Run("both bounds", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(10), Inclusive: true},
			Upper: &RangeBound{Value: int64(50), Inclusive: false},
		}
		assert.Equal(t, ">= 10 and < 50", rangeDetail(rc))
	})

	t.Run("no bounds", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", rangeDetail(RangeCondition{}))
	})
}

func TestConditionCount(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, conditionCount(nil))
	assert.Equal(t, 0, conditionCount(OneOrMore{}))
	assert.Equal(t, 1, conditionCount(OneOrMore{{{}}}))
	assert.Equal(t, 3, conditionCount(OneOrMore{{{}}, {{}, {}}}))
}

func TestScanOperation_CoveringIndex(t *testing.T) {
	t.Parallel()

	scan := Scan{Type: ScanTypeIndexPoint, CoveringIndex: true}
	assert.Equal(t, "covering_index_point", scanOperation(scan))

	scanSeq := Scan{Type: ScanTypeSequential, CoveringIndex: true}
	assert.Equal(t, "sequential", scanOperation(scanSeq))
}

func TestScanDetail_AllFields(t *testing.T) {
	t.Parallel()

	scan := Scan{
		TableName:  "orders",
		TableAlias: "o",
		IndexName:  "idx_user",
		IndexColumns: []Column{
			{Name: "user_id", Kind: Int8},
		},
		IndexKeys: []any{int64(1)},
		RangeCondition: RangeCondition{
			Lower: &RangeBound{Value: int64(0), Inclusive: true},
		},
		Filters: OneOrMore{{{}}},
	}
	detail := scanDetail(scan)
	assert.Contains(t, detail, "table=orders")
	assert.Contains(t, detail, "alias=o")
	assert.Contains(t, detail, "index=idx_user")
	assert.Contains(t, detail, "columns=user_id")
	assert.Contains(t, detail, "keys=")
	assert.Contains(t, detail, "range=")
	assert.Contains(t, detail, "filters=1")
}

func TestEstimateScanRows_AllTypes(t *testing.T) {
	t.Parallel()

	table := NewTable(testLogger, nil, nil, testTableName, testColumns[0:2], 0, nil,
		WithRowCountGetter(func() int64 { return 42 }),
	)

	t.Run("sequential with row count", func(t *testing.T) {
		t.Parallel()
		got := estimateScanRows(table, Scan{Type: ScanTypeSequential})
		assert.Equal(t, int64(42), got.Value)
	})

	t.Run("index all with row count", func(t *testing.T) {
		t.Parallel()
		got := estimateScanRows(table, Scan{Type: ScanTypeIndexAll})
		assert.Equal(t, int64(42), got.Value)
	})

	t.Run("index first returns 1", func(t *testing.T) {
		t.Parallel()
		got := estimateScanRows(table, Scan{Type: ScanTypeIndexFirst})
		assert.Equal(t, int64(1), got.Value)
	})

	t.Run("index last returns 1", func(t *testing.T) {
		t.Parallel()
		got := estimateScanRows(table, Scan{Type: ScanTypeIndexLast})
		assert.Equal(t, int64(1), got.Value)
	})

	t.Run("index point no stats uses key count", func(t *testing.T) {
		t.Parallel()
		got := estimateScanRows(table, Scan{
			Type:      ScanTypeIndexPoint,
			IndexName: "nonexistent",
			IndexKeys: []any{int64(1), int64(2)},
		})
		assert.Equal(t, int64(2), got.Value)
	})

	t.Run("index range no stats returns invalid", func(t *testing.T) {
		t.Parallel()
		got := estimateScanRows(table, Scan{Type: ScanTypeIndexRange, IndexName: "none"})
		assert.False(t, got.Valid)
	})
}

func TestExplainSelectedFields(t *testing.T) {
	t.Parallel()

	table := NewTable(testLogger, nil, nil, testTableName, testColumns[0:2], 0, nil)

	t.Run("select aggregate", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:       Select,
			Aggregates: []AggregateExpr{{Kind: AggregateSum, Column: "email"}},
		}
		fields := explainSelectedFields(table, stmt)
		assert.NotEmpty(t, fields)
	})

	t.Run("count all", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "COUNT(*)"}},
		}
		fields := explainSelectedFields(table, stmt)
		assert.Empty(t, fields)
	})

	t.Run("plain fields with conditions", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "id"}, {Name: "email"}},
			Conditions: OneOrMore{{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "id"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandInteger, Value: int64(1)},
				},
			}},
		}
		fields := explainSelectedFields(table, stmt)
		names := make([]string, len(fields))
		for i, f := range fields {
			names[i] = f.Name
		}
		assert.Contains(t, names, "id")
	})
}

func TestAppendConditionFields(t *testing.T) {
	t.Parallel()

	conds := OneOrMore{
		{
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "score"}},
				Operator: Gt,
				Operand2: Operand{Type: OperandField, Value: Field{Name: "min_score"}},
			},
		},
	}
	fields := appendConditionFields(nil, conds)
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	assert.Contains(t, names, "score")
	assert.Contains(t, names, "min_score")
}

func TestStatement_ExplainReadOnlyCloneAndBind(t *testing.T) {
	t.Parallel()

	stmt := Statement{
		Kind:           Explain,
		ExplainAnalyze: true,
		ExplainStatement: &Statement{
			Kind:      Select,
			TableName: testTableName,
			Fields:    []Field{{Name: "*"}},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "id"}, OperandPlaceholder, Placeholder{}),
				},
			},
		},
	}

	assert.True(t, stmt.ReadOnly())
	assert.Equal(t, 1, stmt.NumPlaceholders())

	clone := stmt.Clone()
	require.NotNil(t, clone.ExplainStatement)
	clone.ExplainStatement.TableName = "other"
	assert.Equal(t, testTableName, stmt.ExplainStatement.TableName)

	bound, err := stmt.BindArguments(int64(42))
	require.NoError(t, err)
	require.NotNil(t, bound.ExplainStatement)
	assert.Equal(t, OperandInteger, bound.ExplainStatement.Conditions[0][0].Operand2.Type)
	assert.Equal(t, int64(42), bound.ExplainStatement.Conditions[0][0].Operand2.Value)
}

func TestBuildExplainResult_PlainPlan(t *testing.T) {
	t.Parallel()

	table := NewTable(testLogger, nil, nil, testTableName, testColumns[0:2], 0, nil,
		WithPrimaryKey(NewPrimaryKey(PrimaryKeyName(testTableName), testColumns[0:1], true)),
		WithRowCountGetter(func() int64 { return 10 }),
	)
	plan := QueryPlan{
		Scans: []Scan{
			{
				TableName:    testTableName,
				Type:         ScanTypeIndexPoint,
				IndexName:    PrimaryKeyName(testTableName),
				IndexColumns: testColumns[0:1],
				IndexKeys:    []any{int64(1)},
			},
		},
	}

	result := buildExplainResult(plan, table, nil)
	require.Equal(t, explainColumns, result.Columns)
	require.True(t, result.Rows.Next(context.Background()))

	row := result.Rows.Row()
	require.Len(t, row.Values, len(explainColumns))
	assert.Equal(t, int64(1), row.Values[0].Value)
	assert.Equal(t, "index_point", row.Values[1].Value.(TextPointer).String())
	assert.Contains(t, row.Values[2].Value.(TextPointer).String(), "index=pkey__test_table")
	assert.Equal(t, int64(1), row.Values[3].Value)
	assert.False(t, row.Values[4].Valid)
	assert.False(t, row.Values[5].Valid)
}

func TestTable_AnalyzePlanSequentialScan(t *testing.T) {
	table, txManager, _ := newTestTable(t, testColumns[0:2])
	ctx := context.Background()
	insertStmt := Statement{
		Kind:    Insert,
		Columns: table.Columns,
		Fields:  fieldsFromColumns(table.Columns...),
		Inserts: [][]OptionalValue{
			{
				{Valid: true, Value: int64(1)},
				{Valid: true, Value: NewTextPointer([]byte("a@example.com"))},
			},
			{
				{Valid: true, Value: int64(2)},
				{Valid: true, Value: NewTextPointer([]byte("b@example.com"))},
			},
		},
	}
	mustInsert(ctx, t, table, txManager, insertStmt)

	selectStmt := Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(table.Columns...),
	}
	var metrics map[int]explainMetric
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		plan, err := table.PlanQuery(ctx, selectStmt)
		if err != nil {
			return err
		}
		metrics, err = table.analyzePlan(ctx, selectStmt, plan)
		return err
	})
	require.NoError(t, err)
	require.Contains(t, metrics, 1)
	assert.Equal(t, int64(2), metrics[1].rows)
	assert.GreaterOrEqual(t, metrics[1].durationUS, int64(0))
}
