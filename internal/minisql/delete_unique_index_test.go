package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Delete_UniqueIndex(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager(zap.NewNop())
		tablePager = NewTransactionalPager(
			aPager.ForTable(testColumnsWithUniqueIndex),
			txManager,
		)
		rows      = gen.RowsWithUniqueIndex(10)
		aTable    *Table
		indexName = uniqueIndexName(testTableName, "email")
	)

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := tablePager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		freePage.LeafNode = NewLeafNode()
		freePage.LeafNode.Header.IsRoot = true
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testColumnsWithUniqueIndex, freePage.Index)
		return nil
	}, aPager)
	require.NoError(t, err)

	indexPager := NewTransactionalPager(
		aPager.ForIndex(
			aTable.UniqueIndexes[indexName].Column.Kind,
			uint64(aTable.UniqueIndexes[indexName].Column.Size),
			true,
		),
		aTable.txManager,
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(aTable.Columns...),
		Inserts: make([][]OptionalValue, 0, len(rows)),
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := indexPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		uniqueIndex := aTable.UniqueIndexes[indexName]
		uniqueIndex.Index, err = aTable.createBTreeIndex(
			indexPager,
			freePage,
			aTable.UniqueIndexes[indexName].Column,
			aTable.UniqueIndexes[indexName].Name,
		)
		aTable.UniqueIndexes[indexName] = uniqueIndex
		if err != nil {
			return err
		}
		return aTable.Insert(ctx, stmt)
	}, aPager)
	require.NoError(t, err)

	checkRows(ctx, t, aTable, rows)

	t.Run("Delete single row", func(t *testing.T) {
		email, ok := rows[0].GetValue("email")
		require.True(t, ok)

		stmt := Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsEqual("email", OperandQuotedString, email.Value.(TextPointer)),
				},
			},
		}

		var aResult StatementResult
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, stmt)
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 1, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows[1:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind: Delete,
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 9, aResult.RowsAffected)
		checkRows(ctx, t, aTable, nil)
	})
}
