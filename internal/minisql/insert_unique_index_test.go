package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Insert_UniqueIndex(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager(zap.NewNop())
		tablePager = NewTransactionalPager(
			aPager.ForTable(testColumnsWithUniqueIndex),
			txManager,
		)
		rows      = gen.RowsWithUniqueIndex(100)
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
	}, TxCommitter{aPager, nil})
	require.NoError(t, err)

	indexPager := NewTransactionalPager(
		aPager.ForIndex(
			aTable.UniqueIndexes[indexName].Column.Kind,
			uint64(aTable.UniqueIndexes[indexName].Column.Size),
			true,
		),
		aTable.txManager,
	)

	t.Run("Insert rows with unique index", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(aTable.Columns...),
			Inserts: make([][]OptionalValue, 0, len(rows)),
		}
		for _, aRow := range rows {
			stmt.Inserts = append(stmt.Inserts, aRow.Values)
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
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
		}, TxCommitter{aPager, nil})
		require.NoError(t, err)

		checkRows(ctx, t, aTable, rows)
	})

	t.Run("Try to insert duplicate primary key", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(aTable.Columns...),
			Inserts: [][]OptionalValue{rows[0].Values},
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			return aTable.Insert(ctx, stmt)
		}, TxCommitter{aPager, nil})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateKey)

		checkRows(ctx, t, aTable, rows)
	})
}
