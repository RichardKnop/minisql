package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
