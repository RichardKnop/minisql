package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnection_ExecuteStatements(t *testing.T) {
	aPager := initTest(t)

	var (
		ctx  = context.Background()
		rows = gen.Rows(1)
	)
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager, aPager)
	require.NoError(t, err)

	// Create test table
	testConn := aDatabase.NewConnection(1, nil)
	_, err = testConn.ExecuteStatements(ctx, Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumns,
	})
	require.NoError(t, err)
	testTable := aDatabase.tables[testTableName]

	// Batch insert test rows
	results, err := testConn.ExecuteStatements(ctx, Statement{
		Kind:      Insert,
		TableName: testTable.Name,
		Fields:    columnNames(testColumns...),
		Inserts:   [][]OptionalValue{rows[0].Values},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, StatementResult{
		RowsAffected: 1,
	}, results[0])

	// Select all rows from the test table for baseline comparison
	expectedRows := []Row{}
	t.Run("SELECT statement", func(t *testing.T) {
		results, err := testConn.ExecuteStatements(ctx, Statement{
			Kind:      Select,
			TableName: testTable.Name,
			Fields:    columnNames(testColumns...),
		})
		require.NoError(t, err)
		require.Len(t, results, 1)

		aRow, err := results[0].Rows(ctx)
		for ; err == nil; aRow, err = results[0].Rows(ctx) {
			expectedRows = append(expectedRows, aRow)
		}
		require.Len(t, expectedRows, 1)
	})

	t.Run("BEGIN and then ROLLBACK", func(t *testing.T) {
		aConnection := aDatabase.NewConnection(3, nil)
		results, err := aConnection.ExecuteStatements(ctx, Statement{Kind: BeginTransaction})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, StatementResult{}, results[0])
		require.True(t, aConnection.HasActiveTransaction())

		results, err = aConnection.ExecuteStatements(ctx, Statement{
			TableName: testTable.Name,
			Kind:      Delete,
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, StatementResult{
			Columns:      testColumns,
			RowsAffected: 1,
		}, results[0])
		require.True(t, aConnection.HasActiveTransaction())

		results, err = aConnection.ExecuteStatements(ctx, Statement{Kind: RollbackTransaction})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, StatementResult{}, results[0])
		require.False(t, aConnection.HasActiveTransaction())

		checkRows(ctx, t, testTable, expectedRows)
	})

	t.Run("BEGIN and then COMMIT", func(t *testing.T) {
		aConnection := aDatabase.NewConnection(4, nil)
		results, err := aConnection.ExecuteStatements(ctx, Statement{Kind: BeginTransaction})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, StatementResult{}, results[0])
		require.True(t, aConnection.HasActiveTransaction())

		results, err = aConnection.ExecuteStatements(ctx, Statement{
			TableName: testTable.Name,
			Kind:      Delete,
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, StatementResult{
			Columns:      testColumns,
			RowsAffected: 1,
		}, results[0])
		require.True(t, aConnection.HasActiveTransaction())

		results, err = aConnection.ExecuteStatements(ctx, Statement{Kind: CommitTransaction})
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, StatementResult{}, results[0])
		require.False(t, aConnection.HasActiveTransaction())

		checkRows(ctx, t, testTable, nil)
	})
}
