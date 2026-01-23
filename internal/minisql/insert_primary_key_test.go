package minisql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Insert_PrimaryKey(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testColumns[0:2])
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithPrimaryKey(10)
		aTable         *Table
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
			nil,
			WithPrimaryKey(NewPrimaryKey("foo", testColumns[0:1], true)),
		)
		return nil
	})
	require.NoError(t, err)

	txIndexPager := NewTransactionalPager(
		aPager.ForIndex(aTable.PrimaryKey.Columns, true),
		aTable.txManager,
		testTableName,
		aTable.PrimaryKey.Name,
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
		freePage, err := txIndexPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		aTable.PrimaryKey.Index, err = aTable.createBTreeIndex(
			txIndexPager,
			freePage,
			aTable.PrimaryKey.Columns,
			aTable.PrimaryKey.Name,
			true,
		)
		if err != nil {
			return err
		}
		_, err = aTable.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	checkRows(ctx, t, aTable, rows)

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

func TestTable_Insert_PrimaryKey_Autoincrement(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testColumns[0:2])
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithPrimaryKey(1)
		aTable         *Table
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
			nil,
			WithPrimaryKey(NewPrimaryKey("foo", testColumns[0:1], true)),
		)
		return nil
	})
	require.NoError(t, err)

	txIndexPager := NewTransactionalPager(
		aPager.ForIndex(aTable.PrimaryKey.Columns, true),
		aTable.txManager,
		testTableName,
		aTable.PrimaryKey.Name,
	)

	t.Run("Insert rows without primary key, autoincrement should generate primary keys", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: aTable.Name,
			Columns:   aTable.Columns,
			Fields:    fieldsFromColumns(aTable.Columns...),
			Inserts:   make([][]OptionalValue, 0, len(rows)),
		}
		for _, aRow := range rows {
			// Set primary key value to NULL so we can test autoincrement
			aRow.Values[0] = OptionalValue{Valid: false}
			stmt.Inserts = append(stmt.Inserts, aRow.Values)
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			freePage, err := txIndexPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			aTable.PrimaryKey.Index, err = aTable.createBTreeIndex(
				txIndexPager,
				freePage,
				aTable.PrimaryKey.Columns,
				aTable.PrimaryKey.Name,
				true,
			)
			if err != nil {
				return err
			}
			_, err = aTable.Insert(ctx, stmt)
			return err
		})
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

func TestTable_Insert_CompositePrimaryKey(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testCompositeKeyColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithCompositeKey(10)
		aTable         *Table
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
			nil,
			WithPrimaryKey(NewPrimaryKey("foo", testCompositeKeyColumns[1:3], false)),
		)
		return nil
	})
	require.NoError(t, err)

	txIndexPager := NewTransactionalPager(
		aPager.ForIndex(aTable.PrimaryKey.Columns, true),
		aTable.txManager,
		testTableName,
		aTable.PrimaryKey.Name,
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
		freePage, err := txIndexPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		aTable.PrimaryKey.Index, err = aTable.createBTreeIndex(
			txIndexPager,
			freePage,
			aTable.PrimaryKey.Columns,
			aTable.PrimaryKey.Name,
			true,
		)
		if err != nil {
			return err
		}
		_, err = aTable.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	checkRowsWithCompositePrimaryKey(ctx, t, aTable, rows)

	// Check that autoincremented primary keys are correct
	expectedPrimaryKeys := make([]CompositeKey, 0, len(rows))
	for i := range len(rows) {
		expectedPrimaryKeys = append(expectedPrimaryKeys, NewCompositeKey(
			aTable.Columns[1:3],
			rows[i].Values[1].Value.(TextPointer).String(),
			rows[i].Values[2].Value.(TextPointer).String(),
		))
	}
	checkCompositeIndexKeys(ctx, t, aTable.PrimaryKey.Index, expectedPrimaryKeys)

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

		checkRowsWithCompositePrimaryKey(ctx, t, aTable, rows)
	})
}

func checkRowsWithPrimaryKey(ctx context.Context, t *testing.T, aTable *Table, expectedRows []Row) {
	selectResult, err := aTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(aTable.Columns...),
	})
	require.NoError(t, err)

	expectedIDMap := map[int64]struct{}{}
	for _, r := range expectedRows {
		id, ok := r.GetValue("id")
		require.True(t, ok)
		expectedIDMap[id.Value.(int64)] = struct{}{}
	}

	var actual []Row
	for selectResult.Rows.Next(ctx) {
		aRow := selectResult.Rows.Row()
		actual = append(actual, aRow)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[aRow.Values[0].Value.(int64)]
			assert.True(t, ok)
		}
	}
	require.NoError(t, selectResult.Rows.Err())

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

func checkIndexVarcharKeys(ctx context.Context, t *testing.T, anIndex BTreeIndex, expectedKeys []string) {
	actualKeys := make([]string, 0, 100)
	err := anIndex.BFS(ctx, func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[string])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	require.Len(t, actualKeys, len(expectedKeys))
	assert.ElementsMatch(t, expectedKeys, actualKeys)
}

func checkRowsWithCompositePrimaryKey(ctx context.Context, t *testing.T, aTable *Table, expectedRows []Row) {
	selectResult, err := aTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(aTable.Columns...),
	})
	require.NoError(t, err)

	expectedIDMap := map[string]struct{}{}
	for _, r := range expectedRows {
		firstName, ok := r.GetValue("first_name")
		require.True(t, ok)
		lastName, ok := r.GetValue("last_name")
		require.True(t, ok)
		expectedIDMap[fmt.Sprintf("%s|%s", firstName.Value.(TextPointer).String(), lastName.Value.(TextPointer).String())] = struct{}{}
	}

	var actual []Row
	for selectResult.Rows.Next(ctx) {
		aRow := selectResult.Rows.Row()
		actual = append(actual, aRow)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[fmt.Sprintf("%s|%s", aRow.Values[1].Value.(TextPointer).String(), aRow.Values[2].Value.(TextPointer).String())]
			assert.True(t, ok)
		}
	}
	require.NoError(t, selectResult.Rows.Err())

	require.Len(t, actual, len(expectedRows))
	for i := range len(expectedRows) {
		assert.Equal(t, actual[i], expectedRows[i], "row %d does not match expected", i)
		assert.Equal(t, actual[i].NullBitmask(), expectedRows[i].NullBitmask(), "row %d null bitmask does not match expected", i)
	}
}

func checkCompositeIndexKeys(ctx context.Context, t *testing.T, anIndex BTreeIndex, expectedKeys []CompositeKey) {
	actualKeys := make([]CompositeKey, 0, 100)
	err := anIndex.BFS(ctx, func(aPage *Page) {
		node := aPage.IndexNode.(*IndexNode[CompositeKey])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	require.Len(t, actualKeys, len(expectedKeys))
	assert.ElementsMatch(t, expectedKeys, actualKeys)
}
