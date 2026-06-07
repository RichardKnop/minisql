package minisql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExternalSort_SpillsToDisc verifies that ORDER BY produces a correctly
// sorted result when the memory limit is set low enough to force at least one
// run to be flushed to disk.
func TestExternalSort_SpillsToDisc(t *testing.T) {
	const rowCount = 200

	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "name", Kind: Varchar, Size: 255},
	}

	// sortMemLimit of 1 byte forces a spill after every row.
	table, txManager, _ := newTestTable(t, columns, withSortMemLimit(1))

	ctx := context.Background()

	// Insert rows in reverse order so a naïve (stable) scan would fail without sort.
	for i := rowCount; i >= 1; i-- {
		name := fmt.Sprintf("user_%04d", i)
		err := txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
			_, err := table.Insert(txCtx, Statement{
				Kind:   Insert,
				Fields: fieldsFromColumns(columns...),
				Inserts: [][]OptionalValue{{{Value: int64(i), Valid: true}, {
					Value: NewTextPointer([]byte(name)), Valid: true,
				}}},
			})
			return err
		})
		require.NoError(t, err)
	}

	// SELECT id, name ORDER BY name ASC — name is alphabetically equal to id order.
	stmt := Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(columns...),
		Conditions: OneOrMore{{}},
		OrderBy: []OrderBy{
			{Field: Field{Name: "name"}, Direction: Asc},
		},
	}
	// Remove empty condition
	stmt.Conditions = nil

	var result StatementResult
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(txCtx context.Context) error {
		var err error
		result, err = table.Select(txCtx, Statement{
			Kind:    Select,
			Fields:  fieldsFromColumns(columns...),
			OrderBy: []OrderBy{{Field: Field{Name: "name"}, Direction: Asc}},
		})
		return err
	})
	require.NoError(t, err)

	var ids []int64
	for result.Rows.Next(ctx) {
		row := result.Rows.Row()
		id, ok := row.Values[0].Value.(int64)
		require.True(t, ok)
		ids = append(ids, id)
	}
	require.NoError(t, result.Rows.Err())

	require.Len(t, ids, rowCount, "expected %d rows", rowCount)
	for i, id := range ids {
		assert.Equal(t, int64(i+1), id, "row %d should have id %d, got %d", i, i+1, id)
	}
}

// TestExternalSort_NoSpill verifies that when sortMemLimit == 0 (disabled)
// large sorts still work correctly via the existing in-memory path.
func TestExternalSort_NoSpill(t *testing.T) {
	const rowCount = 100

	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
	}

	// sortMemLimit of 0 means no external sort; everything stays in memory.
	table, txManager, _ := newTestTable(t, columns, withSortMemLimit(0))
	ctx := context.Background()

	for i := rowCount; i >= 1; i-- {
		err := txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
			_, err := table.Insert(txCtx, Statement{
				Kind:    Insert,
				Fields:  fieldsFromColumns(columns...),
				Inserts: [][]OptionalValue{{{Value: int64(i), Valid: true}}},
			})
			return err
		})
		require.NoError(t, err)
	}

	var result StatementResult
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(txCtx context.Context) error {
		var err error
		result, err = table.Select(txCtx, Statement{
			Kind:    Select,
			Fields:  fieldsFromColumns(columns...),
			OrderBy: []OrderBy{{Field: Field{Name: "id"}, Direction: Asc}},
		})
		return err
	})
	require.NoError(t, err)

	var ids []int64
	for result.Rows.Next(ctx) {
		row := result.Rows.Row()
		id, _ := row.Values[0].Value.(int64)
		ids = append(ids, id)
	}
	require.Len(t, ids, rowCount)
	for i, id := range ids {
		assert.Equal(t, int64(i+1), id)
	}
}

// TestExternalSort_DescOrder verifies DESC sort works correctly after external merge.
func TestExternalSort_DescOrder(t *testing.T) {
	const rowCount = 50

	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
	}

	table, txManager, _ := newTestTable(t, columns, withSortMemLimit(1))
	ctx := context.Background()

	for i := 1; i <= rowCount; i++ {
		err := txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
			_, err := table.Insert(txCtx, Statement{
				Kind:    Insert,
				Fields:  fieldsFromColumns(columns...),
				Inserts: [][]OptionalValue{{{Value: int64(i), Valid: true}}},
			})
			return err
		})
		require.NoError(t, err)
	}

	var result StatementResult
	err := txManager.ExecuteReadOnlyTransaction(ctx, func(txCtx context.Context) error {
		var err error
		result, err = table.Select(txCtx, Statement{
			Kind:    Select,
			Fields:  fieldsFromColumns(columns...),
			OrderBy: []OrderBy{{Field: Field{Name: "id"}, Direction: Desc}},
		})
		return err
	})
	require.NoError(t, err)

	var ids []int64
	for result.Rows.Next(ctx) {
		row := result.Rows.Row()
		id, _ := row.Values[0].Value.(int64)
		ids = append(ids, id)
	}
	require.Len(t, ids, rowCount)
	for i, id := range ids {
		assert.Equal(t, int64(rowCount-i), id, "position %d", i)
	}
}
