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
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testColumns[0:2])
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithUniqueIndex(10)
		aTable         *Table
		indexName      = UniqueIndexName(testTableName, "email")
		indexPager     = aPager.ForIndex(testColumns[1:2], true)
		txIndexPager   = NewTransactionalPager(
			indexPager,
			NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), aPager, nil),
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
		aTable = NewTable(
			testLogger,
			txPager,
			txManager,
			testTableName,
			testColumns[0:2],
			freePage.Index,
			WithUniqueIndex(UniqueIndex{
				IndexInfo: IndexInfo{
					Name:    indexName,
					Columns: testColumns[1:2],
				},
			}),
		)
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
				aTable.UniqueIndexes[indexName].Columns,
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

func TestTable_Insert_CompositeUniqueIndex(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testCompositeKeyColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithCompositeKey(10)
		aTable         *Table
		indexName      = UniqueIndexName(testTableName, "email")
		indexPager     = aPager.ForIndex(testCompositeKeyColumns[1:3], true)
		txIndexPager   = NewTransactionalPager(
			indexPager,
			NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), aPager, nil),
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
		aTable = NewTable(
			testLogger,
			txPager,
			txManager,
			testTableName,
			testCompositeKeyColumns,
			freePage.Index,
			WithUniqueIndex(UniqueIndex{
				IndexInfo: IndexInfo{
					Name:    indexName,
					Columns: testCompositeKeyColumns[1:3],
				},
			}),
		)
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
				aTable.UniqueIndexes[indexName].Columns,
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
