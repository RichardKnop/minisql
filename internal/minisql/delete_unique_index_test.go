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
		pager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = pager.ForTable(testColumns[0:2])
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithUniqueIndex(10)
		table         *Table
		indexName      = UniqueIndexName(testTableName, "email")
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

	txIndexPager := NewTransactionalPager(
		pager.ForIndex(
			table.UniqueIndexes[indexName].Columns,
			true,
		),
		table.txManager,
		table.Name,
		table.UniqueIndexes[indexName].Name,
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(table.Columns...),
		Inserts: make([][]OptionalValue, 0, len(rows)),
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
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

	t.Run("Delete single row", func(t *testing.T) {
		email, ok := rows[0].GetValue("email")
		require.True(t, ok)

		stmt := Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "email"}, OperandQuotedString, email.Value.(TextPointer)),
				},
			},
		}

		result := mustDelete(ctx, t, table, txManager, pager, stmt)

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{Kind: Delete})

		assert.Equal(t, 9, result.RowsAffected)
		checkRows(ctx, t, table, nil)
	})
}

func TestTable_Delete_CompositeUniqueIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = pager.ForTable(testCompositeKeyColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithCompositeKey(10)
		table         *Table
		indexName      = UniqueIndexName(testTableName, "first_name", "last_name")
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

	txIndexPager := NewTransactionalPager(
		pager.ForIndex(table.UniqueIndexes[indexName].Columns, true),
		table.txManager,
		testTableName,
		table.UniqueIndexes[indexName].Name,
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(table.Columns...),
		Inserts: make([][]OptionalValue, 0, len(rows)),
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := txIndexPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		uniqueIndex := table.UniqueIndexes[indexName]
		var idx BTreeIndex
		idx, err = table.createBTreeIndex(
			txIndexPager,
			freePage,
			table.UniqueIndexes[indexName].Columns,
			table.UniqueIndexes[indexName].Name,
			true,
		)
		if err != nil {
			return err
		}
		uniqueIndex.Index = idx
		table.UniqueIndexes[indexName] = uniqueIndex
		_, err = table.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	checkRows(ctx, t, table, rows)

	t.Run("Delete single row", func(t *testing.T) {
		firstName, ok := rows[0].GetValue("first_name")
		require.True(t, ok)
		lastName, ok := rows[0].GetValue("last_name")
		require.True(t, ok)

		stmt := Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "first_name"}, OperandQuotedString, firstName.Value),
					FieldIsEqual(Field{Name: "last_name"}, OperandQuotedString, lastName.Value),
				},
			},
		}

		result := mustDelete(ctx, t, table, txManager, pager, stmt)

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{Kind: Delete})

		assert.Equal(t, 9, result.RowsAffected)
		checkRows(ctx, t, table, nil)
	})
}
