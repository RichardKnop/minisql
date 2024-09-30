package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Select(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	aDatabase, err := NewDatabase("db", nil, nil)
	require.NoError(t, err)
	aTable, err := aDatabase.CreateTable(ctx, "foo", testColumns)
	require.NoError(t, err)

	rows := gen.Rows(20)
	insertRows(ctx, t, aTable, rows)

	selectStmt := Statement{
		Kind:      Select,
		TableName: "foo",
		Fields:    []string{"id", "email", "age"},
	}
	aResult, err := aTable.Select(ctx, selectStmt)
	require.NoError(t, err)

	for i := 0; i < len(rows); i++ {
		selectRow, err := aResult.Rows(ctx)
		require.NoError(t, err)
		assert.Equal(t, rows[i], selectRow)
	}

	_, err = aResult.Rows(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoMoreRows)
}

func insertRows(ctx context.Context, t *testing.T, aTable *Table, rows []Row) {
	insertStmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    []string{"id", "email", "age"},
		Inserts:   make([][]any, 0, len(rows)),
	}
	for _, aRow := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	aResult, err := aTable.Insert(ctx, insertStmt)
	require.NoError(t, err)
	assert.Equal(t, len(rows), aResult.RowsAffected)
}