package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Insert_PrimaryKey(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager()
		tablePager = NewTransactionalPager(
			aPager.ForTable(Row{Columns: testColumnsWithPrimaryKey}.Size()),
			txManager,
		)
		rows    = gen.RowsWithPrimaryKey(100)
		rowSize = Row{Columns: testColumnsWithPrimaryKey}.Size()
		aTable  *Table
	)

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := tablePager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		freePage.LeafNode = NewLeafNode(rowSize)
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
			Fields:  columnNames(testColumnsWithPrimaryKey...),
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

	t.Run("Try to insert duplicate primary key", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  columnNames(testColumnsWithPrimaryKey...),
			Inserts: [][]OptionalValue{rows[0].Values},
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return aTable.Insert(ctx, stmt)
		}, aPager)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})
}
