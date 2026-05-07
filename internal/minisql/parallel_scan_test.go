package minisql

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeafPageList_Empty(t *testing.T) {
	table, _, _ := newTestTable(t, testColumns[0:2])
	ctx := context.Background()

	pages, err := table.leafPageList(ctx)
	require.NoError(t, err)
	assert.Empty(t, pages)
}

func TestLeafPageList_NonEmpty(t *testing.T) {
	table, txManager, _ := newTestTable(t, testColumns[0:2])
	ctx := context.Background()

	mustInsert(ctx, t, table, txManager, Statement{
		Kind:    Insert,
		Columns: table.Columns,
		Fields:  fieldsFromColumns(table.Columns...),
		Inserts: [][]OptionalValue{
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("a@example.com"))}},
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("b@example.com"))}},
		},
	})

	var pages []PageIndex
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		var err error
		pages, err = table.leafPageList(ctx)
		return err
	})
	require.NoError(t, err)
	assert.NotEmpty(t, pages)
	// All returned indices must be distinct.
	seen := make(map[PageIndex]bool)
	for _, p := range pages {
		assert.False(t, seen[p], "duplicate page index %d", p)
		seen[p] = true
	}
}

func TestParallelSequentialScan_MatchesSequential(t *testing.T) {
	// Insert enough rows to guarantee multiple leaf pages, then compare the row
	// sets produced by sequential and parallel scans.
	table, txManager, _ := newTestTable(t, testColumns[0:2])
	ctx := context.Background()

	const n = 200
	inserts := make([][]OptionalValue, 0, n)
	for i := range n {
		inserts = append(inserts, []OptionalValue{
			{Valid: true, Value: int64(i + 1)},
			{Valid: true, Value: NewTextPointer([]byte("user@example.com"))},
		})
	}
	mustInsert(ctx, t, table, txManager, Statement{
		Kind:    Insert,
		Columns: table.Columns,
		Fields:  fieldsFromColumns(table.Columns...),
		Inserts: inserts,
	})

	scan := Scan{TableName: testTableName, TableAlias: "t", Type: ScanTypeSequential}
	fields := fieldsFromColumns(table.Columns...)

	var seqRows []Row
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		return runTableScan(ctx, QueryPlan{}, table, scan, fields, func(row Row) error {
			seqRows = append(seqRows, row)
			return nil
		})
	})
	require.NoError(t, err)

	table.parallelScan = true
	var parRows []Row
	err = txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		return runTableScan(ctx, QueryPlan{}, table, scan, fields, func(row Row) error {
			parRows = append(parRows, row)
			return nil
		})
	})
	require.NoError(t, err)

	// Parallel scan may deliver rows in a different order — sort both by key before comparing.
	sortRowsByKey := func(rows []Row) {
		sort.Slice(rows, func(i, j int) bool {
			vi, _ := rows[i].GetValue(table.Columns[0].Name)
			vj, _ := rows[j].GetValue(table.Columns[0].Name)
			return vi.Value.(int64) < vj.Value.(int64)
		})
	}
	sortRowsByKey(seqRows)
	sortRowsByKey(parRows)

	assert.Equal(t, len(seqRows), len(parRows))
	assert.Equal(t, seqRows, parRows)
}

func TestParallelSequentialScan_WithFilter(t *testing.T) {
	// Verify the filter predicate is applied correctly under parallel scan.
	table, txManager, _ := newTestTable(t, testColumns[0:2])
	ctx := context.Background()

	const n = 100
	inserts := make([][]OptionalValue, 0, n)
	for i := range n {
		inserts = append(inserts, []OptionalValue{
			{Valid: true, Value: int64(i + 1)},
			{Valid: true, Value: NewTextPointer([]byte("user@example.com"))},
		})
	}
	mustInsert(ctx, t, table, txManager, Statement{
		Kind:    Insert,
		Columns: table.Columns,
		Fields:  fieldsFromColumns(table.Columns...),
		Inserts: inserts,
	})

	// Filter: id > 50
	conds := OneOrMore{{
		FieldIsGreater(Field{Name: "id"}, OperandInteger, int64(50)),
	}}
	scan := Scan{
		TableName:  testTableName,
		TableAlias: "t",
		Type:       ScanTypeSequential,
		Filters:    conds,
	}
	fields := fieldsFromColumns(table.Columns...)

	table.parallelScan = true
	var parRows []Row
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		return runTableScan(ctx, QueryPlan{}, table, scan, fields, func(row Row) error {
			parRows = append(parRows, row)
			return nil
		})
	})
	require.NoError(t, err)
	assert.Len(t, parRows, 50)
}

func TestParallelSequentialScan_EmptyTable(t *testing.T) {
	table, txManager, _ := newTestTable(t, testColumns[0:2])
	ctx := context.Background()

	table.parallelScan = true
	scan := Scan{TableName: testTableName, TableAlias: "t", Type: ScanTypeSequential}
	var got []Row
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		return runTableScan(ctx, QueryPlan{}, table, scan, fieldsFromColumns(table.Columns...), func(row Row) error {
			got = append(got, row)
			return nil
		})
	})
	require.NoError(t, err)
	assert.Empty(t, got)
}
