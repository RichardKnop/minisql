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
		aPager       = initTest(t)
		ctx          = context.Background()
		tablePager   = aPager.ForTable(testColumnsWithUniqueIndex)
		txManager    = NewTransactionManager(zap.NewNop(), testDbName, mockPagerFactory(tablePager), aPager, nil)
		txPager      = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows         = gen.RowsWithUniqueIndex(100)
		aTable       *Table
		indexName    = uniqueIndexName(testTableName, "email")
		indexPager   = aPager.ForIndex(Varchar, true)
		txIndexPager = NewTransactionalPager(
			indexPager,
			NewTransactionManager(zap.NewNop(), testDbName, mockPagerFactory(indexPager), aPager, nil),
			testTableName,
			indexName,
		)
	)

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := txPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		freePage.LeafNode = NewLeafNode()
		freePage.LeafNode.Header.IsRoot = true
		aTable = NewTable(testLogger, txPager, txManager, testTableName, testColumnsWithUniqueIndex, freePage.Index)
		return nil
	})
	require.NoError(t, err)

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
			freePage, err := txIndexPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			uniqueIndex := aTable.UniqueIndexes[indexName]
			uniqueIndex.Index, err = aTable.createBTreeIndex(
				txIndexPager,
				freePage,
				aTable.UniqueIndexes[indexName].Column,
				aTable.UniqueIndexes[indexName].Name,
				true,
			)
			aTable.UniqueIndexes[indexName] = uniqueIndex
			if err != nil {
				return err
			}
			_, err = aTable.Insert(ctx, stmt)
			return err
		})
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
			_, err := aTable.Insert(ctx, stmt)
			return err
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateKey)

		checkRows(ctx, t, aTable, rows)
	})
}
