package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Delete_PrimaryKey(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager(zap.NewNop())
		tablePager = NewTransactionalPager(
			aPager.ForTable(testColumnsWithPrimaryKey),
			txManager,
		)
		rows   = gen.RowsWithPrimaryKey(10)
		aTable *Table
	)

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := tablePager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		freePage.LeafNode = NewLeafNode()
		freePage.LeafNode.Header.IsRoot = true
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testColumnsWithPrimaryKey, freePage.Index)
		return nil
	}, aPager)
	require.NoError(t, err)

	primaryKeyPager := NewTransactionalPager(
		aPager.ForIndex(aTable.PrimaryKey.Column.Kind, uint64(aTable.PrimaryKey.Column.Size)),
		aTable.txManager,
	)

	t.Run("Insert rows with primary key", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(testColumnsWithPrimaryKey...),
			Inserts: make([][]OptionalValue, 0, len(rows)),
		}
		for _, aRow := range rows {
			stmt.Inserts = append(stmt.Inserts, aRow.Values)
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			freePage, err := primaryKeyPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			aTable.PrimaryKey.Index, err = aTable.newPrimaryKeyIndex(primaryKeyPager, freePage)
			if err != nil {
				return err
			}
			return aTable.Insert(ctx, stmt)
		}, aPager)
		require.NoError(t, err)
	})

	t.Run("Delete single row", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind:       Delete,
			Conditions: FieldIsInAny("id", OperandInteger, id.Value.(int64)),
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
