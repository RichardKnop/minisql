package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Insert_UniqueIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testColumns[0:2])
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithUniqueIndex(10)
		table         *Table
		indexName     = UniqueIndexName(testTableName, "email")
		indexPager    = pager.ForIndex(testColumns[1:2], true)
		txIndexPager  = NewTransactionalPager(
			indexPager,
			NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), pager, nil),
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
		table = NewTable(
			testLogger,
			txPager,
			txManager,
			testTableName,
			testColumns[0:2],
			freePage.Index,
			nil,
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
			Fields:  fieldsFromColumns(table.Columns...),
			Inserts: make([][]OptionalValue, 0, len(rows)),
		}
		for _, row := range rows {
			stmt.Inserts = append(stmt.Inserts, row.Values)
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			freePage, err := txIndexPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			uniqueIndex := table.UniqueIndexes[indexName]
			uniqueIndex.Index, err = table.createBTreeIndex(
				txIndexPager,
				freePage,
				table.UniqueIndexes[indexName].Columns,
				table.UniqueIndexes[indexName].Name,
				true,
			)
			table.UniqueIndexes[indexName] = uniqueIndex
			if err != nil {
				return err
			}
			_, err = table.Insert(ctx, stmt)
			return err
		})
		require.NoError(t, err)

		checkRows(ctx, t, table, rows)
	})

	t.Run("Try to insert duplicate primary key", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(table.Columns...),
			Inserts: [][]OptionalValue{rows[0].Values},
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := table.Insert(ctx, stmt)
			return err
		})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDuplicateKey)

		checkRows(ctx, t, table, rows)
	})
}

func TestTable_Insert_CompositeUniqueIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testCompositeKeyColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithCompositeKey(10)
		table         *Table
		indexName     = UniqueIndexName(testTableName, "email")
		indexPager    = pager.ForIndex(testCompositeKeyColumns[1:3], true)
		txIndexPager  = NewTransactionalPager(
			indexPager,
			NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), pager, nil),
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
		table = NewTable(
			testLogger,
			txPager,
			txManager,
			testTableName,
			testCompositeKeyColumns,
			freePage.Index,
			nil,
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
			Fields:  fieldsFromColumns(table.Columns...),
			Inserts: make([][]OptionalValue, 0, len(rows)),
		}
		for _, row := range rows {
			stmt.Inserts = append(stmt.Inserts, row.Values)
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			freePage, err := txIndexPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			uniqueIndex := table.UniqueIndexes[indexName]
			uniqueIndex.Index, err = table.createBTreeIndex(
				txIndexPager,
				freePage,
				table.UniqueIndexes[indexName].Columns,
				table.UniqueIndexes[indexName].Name,
				true,
			)
			table.UniqueIndexes[indexName] = uniqueIndex
			if err != nil {
				return err
			}
			_, err = table.Insert(ctx, stmt)
			return err
		})
		require.NoError(t, err)

		checkRows(ctx, t, table, rows)
	})

	t.Run("Try to insert duplicate primary key", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(table.Columns...),
			Inserts: [][]OptionalValue{rows[0].Values},
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := table.Insert(ctx, stmt)
			return err
		})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDuplicateKey)

		checkRows(ctx, t, table, rows)
	})
}
