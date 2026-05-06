package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanJoinTableScan_NoConditions(t *testing.T) {
	table, _, _ := newTestTable(t, testColumns[0:2],
		WithPrimaryKey(NewPrimaryKey(PrimaryKeyName(testTableName), testColumns[0:1], true)),
	)

	scan := planJoinTableScan(table, testTableName, "t", nil)
	assert.Equal(t, ScanTypeSequential, scan.Type)
	assert.Empty(t, scan.Filters)
}

func TestPlanJoinTableScan_NoIndex(t *testing.T) {
	// Table with no indexes — planJoinTableScan must fall back to sequential.
	table, _, _ := newTestTable(t, testColumns[0:2])

	conds := OneOrMore{{
		FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(42)),
	}}
	scan := planJoinTableScan(table, testTableName, "t", conds)
	assert.Equal(t, ScanTypeSequential, scan.Type)
	assert.Equal(t, conds, scan.Filters)
}

func TestPlanJoinTableScan_PKEqualityUsesIndexPoint(t *testing.T) {
	table, _, _ := newTestTable(t, testColumns[0:2],
		WithPrimaryKey(NewPrimaryKey(PrimaryKeyName(testTableName), testColumns[0:1], true)),
	)

	conds := OneOrMore{{
		FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(7)),
	}}
	scan := planJoinTableScan(table, testTableName, "t", conds)
	assert.Equal(t, ScanTypeIndexPoint, scan.Type)
	assert.Equal(t, "t", scan.TableAlias)
	assert.Equal(t, testTableName, scan.TableName)
	assert.Equal(t, []any{int64(7)}, scan.IndexKeys)
}

func TestPlanJoinTableScan_AliasedConditionUsesIndex(t *testing.T) {
	// Conditions with AliasPrefix are common in JOIN WHERE clauses (e.g. "t.id = 7").
	// planJoinTableScan must match them against column names, ignoring the prefix.
	table, _, _ := newTestTable(t, testColumns[0:2],
		WithPrimaryKey(NewPrimaryKey(PrimaryKeyName(testTableName), testColumns[0:1], true)),
	)

	conds := OneOrMore{{
		FieldIsEqual(Field{Name: "id", AliasPrefix: "t"}, OperandInteger, int64(7)),
	}}
	scan := planJoinTableScan(table, testTableName, "t", conds)
	assert.Equal(t, ScanTypeIndexPoint, scan.Type, "aliased equality condition should use PK index")
}

func TestPlanJoinTableScan_MultipleORGroupsFallsBackToSequential(t *testing.T) {
	table, _, _ := newTestTable(t, testColumns[0:2],
		WithPrimaryKey(NewPrimaryKey(PrimaryKeyName(testTableName), testColumns[0:1], true)),
	)

	// Two OR groups each with an index match produce two scans — fall back to
	// sequential to avoid multi-scan union complexity in the JOIN execution path.
	conds := OneOrMore{
		{FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(1))},
		{FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(2))},
	}
	scan := planJoinTableScan(table, testTableName, "t", conds)
	assert.Equal(t, ScanTypeSequential, scan.Type)
	assert.Equal(t, conds, scan.Filters)
}

func TestRunTableScan_SequentialDispatch(t *testing.T) {
	// Verify runTableScan dispatches sequential scans correctly and returns rows.
	ctx := context.Background()
	table, txManager, _ := newTestTable(t, testColumns[0:2])

	mustInsert(ctx, t, table, txManager, Statement{
		Kind:    Insert,
		Columns: table.Columns,
		Fields:  fieldsFromColumns(table.Columns...),
		Inserts: [][]OptionalValue{
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("a@example.com"))}},
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("b@example.com"))}},
		},
	})

	scan := Scan{TableName: testTableName, TableAlias: "t", Type: ScanTypeSequential}
	var got []Row
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		return runTableScan(ctx, QueryPlan{}, table, scan, fieldsFromColumns(table.Columns...), func(row Row) error {
			got = append(got, row)
			return nil
		})
	})
	require.NoError(t, err)
	assert.Len(t, got, 2)
}
