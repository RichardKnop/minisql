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

		checkRows(ctx, t, aTable, rows)
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

func TestTable_Insert_PrimaryKey_Autoincrement(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager()
		tablePager = NewTransactionalPager(
			aPager.ForTable(Row{Columns: testColumnsWithPrimaryKey}.Size()),
			txManager,
		)
		rows    = gen.RowsWithPrimaryKey(1)
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

	t.Run("Insert rows without primary key, autoincrement should generate primary keys", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  columnNames(testColumnsWithPrimaryKey[1:]...), // exclude primary key column
			Inserts: make([][]OptionalValue, 0, len(rows)),
		}
		for _, aRow := range rows {
			stmt.Inserts = append(stmt.Inserts, aRow.Values[1:]) // exclude primary key value
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

		checkRowsWithPrimaryKey(ctx, t, aTable, rows)

		// Check that autoincremented primary keys are correct
		expectedPrimaryKeys := make([]int64, 0, len(rows))
		for i := 1; i <= len(rows); i++ {
			expectedPrimaryKeys = append(expectedPrimaryKeys, int64(i))
		}
		checkIndexKeys(ctx, t, aTable.PrimaryKey.Index, expectedPrimaryKeys)
	})
}

func checkRowsWithPrimaryKey(ctx context.Context, t *testing.T, aTable *Table, expectedRows []Row) {
	selectResult, err := aTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: columnNames(testColumnsWithPrimaryKey...),
	})
	require.NoError(t, err)

	expectedIDMap := map[int64]struct{}{}
	for _, r := range expectedRows {
		id, ok := r.GetValue("id")
		require.True(t, ok)
		expectedIDMap[id.Value.(int64)] = struct{}{}
	}

	var actual []Row
	aRow, err := selectResult.Rows(ctx)
	for ; err == nil; aRow, err = selectResult.Rows(ctx) {
		actual = append(actual, aRow)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[aRow.Values[0].Value.(int64)]
			assert.True(t, ok)
		}
	}

	require.Len(t, actual, len(expectedRows))
	for i := range len(expectedRows) {
		assert.Equal(t, actual[i], expectedRows[i], "row %d does not match expected", i)
		assert.Equal(t, actual[i].NullBitmask(), expectedRows[i].NullBitmask(), "row %d null bitmask does not match expected", i)
	}
}

func checkIndexKeys(ctx context.Context, t *testing.T, anIndex BTreeIndex, expectedKeys []int64) {
	actualKeys := make([]int64, 0, 100)
	err := anIndex.BFS(ctx, func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[int64])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	require.Len(t, actualKeys, len(expectedKeys))
	assert.ElementsMatch(t, expectedKeys, actualKeys)
}
