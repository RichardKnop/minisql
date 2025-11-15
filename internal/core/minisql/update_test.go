package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Update(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		rows       = gen.Rows(38)
		txManager  = NewTransactionManager()
		tablePager = NewTransactionalPager(
			aPager.ForTable(testColumns),
			txManager,
		)
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testColumns, 0)
	)

	// Batch insert test rows
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aTable.Insert(ctx, insertStmt)
	}, aPager)
	require.NoError(t, err)

	t.Run("Update no rows", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: "updatednone@foo.bar", Valid: true},
			},
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: "bogus",
						},
					},
				},
			},
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		}, aPager)
		require.NoError(t, err)
		assert.Equal(t, 0, aResult.RowsAffected)

		checkRows(ctx, t, aTable, rows)
	})

	t.Run("Update single row", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: "updatedsingle@foo.bar", Valid: true},
			},
			Conditions: FieldIsInAny("id", OperandInteger, rows[5].Values[0].Value.(int64)),
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		}, aPager)
		require.NoError(t, err)
		assert.Equal(t, 1, aResult.RowsAffected)

		// Prepare expected rows with one updated row
		expected := make([]Row, 0, len(rows))
		for i, aRow := range rows {
			expectedRow := aRow.Clone()
			if i == 5 {
				expectedRow.SetValue("email", OptionalValue{Value: "updatedsingle@foo.bar", Valid: true})
			}

			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, aTable, expected)
	})

	t.Run("Update single row, set column to NULL", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Valid: false},
			},
			Conditions: FieldIsInAny("id", OperandInteger, rows[18].Values[0].Value.(int64)),
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		}, aPager)
		require.NoError(t, err)
		assert.Equal(t, 1, aResult.RowsAffected)

		// Prepare expected rows with one updated row
		expected := make([]Row, 0, len(rows))
		for i, aRow := range rows {
			expectedRow := aRow.Clone()
			if i == 5 {
				expectedRow.SetValue("email", OptionalValue{Value: "updatedsingle@foo.bar", Valid: true})
			}
			if i == 18 {
				expectedRow.SetValue("email", OptionalValue{Valid: false})
			}

			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, aTable, expected)
	})

	t.Run("Update all rows", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: "updatedall@foo.bar", Valid: true},
			},
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		}, aPager)
		require.NoError(t, err)
		assert.Equal(t, 38, aResult.RowsAffected)

		// Prepare expected rows with all rows updated
		expected := make([]Row, 0, len(rows))
		for _, aRow := range rows {
			expectedRow := aRow.Clone()
			expectedRow.SetValue("email", OptionalValue{Value: "updatedall@foo.bar", Valid: true})
			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, aTable, expected)
	})
}
