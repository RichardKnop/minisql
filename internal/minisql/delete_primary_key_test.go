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
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testColumns[0:2])
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithPrimaryKey(10)
		table         *Table
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
			WithPrimaryKey(NewPrimaryKey("foo", testColumns[0:1], true)),
		)
		return nil
	})
	require.NoError(t, err)

	idxPager, err := pager.ForIndex(table.PrimaryKey.Columns, true)
	require.NoError(t, err)
	txIndexPager := NewTransactionalPager(idxPager, table.txManager, testTableName, table.PrimaryKey.Name)

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
		table.PrimaryKey.Index, err = table.createBTreeIndex(
			txIndexPager,
			freePage,
			table.PrimaryKey.Columns,
			table.PrimaryKey.Name,
			true,
		)
		if err != nil {
			return err
		}
		_, err = table.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	checkRows(ctx, t, table, rows)

	t.Run("Delete single row", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "id"}, OperandInteger, id.Value.(int64)),
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

func TestTable_Delete_CompositePrimaryKey(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testCompositeKeyColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithCompositeKey(10)
		table         *Table
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
			WithPrimaryKey(NewPrimaryKey("foo", testCompositeKeyColumns[1:3], true)),
		)
		return nil
	})
	require.NoError(t, err)

	idxPager2, err := pager.ForIndex(table.PrimaryKey.Columns, true)
	require.NoError(t, err)
	txIndexPager := NewTransactionalPager(idxPager2, table.txManager, testTableName, table.PrimaryKey.Name)

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
		table.PrimaryKey.Index, err = table.createBTreeIndex(
			txIndexPager,
			freePage,
			table.PrimaryKey.Columns,
			table.PrimaryKey.Name,
			true,
		)
		if err != nil {
			return err
		}
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
