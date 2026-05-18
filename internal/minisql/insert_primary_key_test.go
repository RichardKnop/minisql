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

	txIndexPager := NewTransactionalPager(
		pager.ForIndex(table.PrimaryKey.Columns, true),
		table.txManager,
		testTableName,
		table.PrimaryKey.Name,
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
		assert.ErrorIs(t, err, ErrDuplicateKey)

		checkRows(ctx, t, table, rows)
	})
}

func TestTable_Insert_PrimaryKey_Autoincrement(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testColumns[0:2])
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithPrimaryKey(1)
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

	txIndexPager := NewTransactionalPager(
		pager.ForIndex(table.PrimaryKey.Columns, true),
		table.txManager,
		testTableName,
		table.PrimaryKey.Name,
	)

	t.Run("Insert rows without primary key, autoincrement should generate primary keys", func(t *testing.T) {
		stmt := Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   table.Columns,
			Fields:    fieldsFromColumns(table.Columns...),
			Inserts:   make([][]OptionalValue, 0, len(rows)),
		}
		for _, row := range rows {
			// Set primary key value to NULL so we can test autoincrement
			row.Values[0] = MakeNull()
			stmt.Inserts = append(stmt.Inserts, row.Values)
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
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

		checkRowsWithPrimaryKey(ctx, t, table, rows)

		// Check that autoincremented primary keys are correct
		expectedPrimaryKeys := make([]int64, 0, len(rows))
		for i := 1; i <= len(rows); i++ {
			expectedPrimaryKeys = append(expectedPrimaryKeys, int64(i))
		}
		checkIndexKeys(ctx, t, table.PrimaryKey.Index, expectedPrimaryKeys)
	})
}

func TestTable_Insert_CompositePrimaryKey(t *testing.T) {
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
			WithPrimaryKey(NewPrimaryKey("foo", testCompositeKeyColumns[1:3], false)),
		)
		return nil
	})
	require.NoError(t, err)

	txIndexPager := NewTransactionalPager(
		pager.ForIndex(table.PrimaryKey.Columns, true),
		table.txManager,
		testTableName,
		table.PrimaryKey.Name,
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

	checkRowsWithCompositePrimaryKey(ctx, t, table, rows)

	// Check that autoincremented primary keys are correct
	expectedPrimaryKeys := make([]CompositeKey, 0, len(rows))
	for i := range len(rows) {
		expectedPrimaryKeys = append(expectedPrimaryKeys, NewCompositeKey(
			table.Columns[1:3],
			rows[i].Values[1].AsTextPointer().String(),
			rows[i].Values[2].AsTextPointer().String(),
		))
	}
	checkCompositeIndexKeys(ctx, t, table.PrimaryKey.Index, expectedPrimaryKeys)

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
		assert.ErrorIs(t, err, ErrDuplicateKey)

		checkRowsWithCompositePrimaryKey(ctx, t, table, rows)
	})
}

func checkRowsWithPrimaryKey(ctx context.Context, t *testing.T, table *Table, expectedRows []Row) {
	selectResult, err := table.Select(ctx, Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(table.Columns...),
	})
	require.NoError(t, err)

	expectedIDMap := map[int64]struct{}{}
	for _, r := range expectedRows {
		id, ok := r.GetValue("id")
		require.True(t, ok)
		expectedIDMap[id.AsInt8()] = struct{}{}
	}

	var actual []Row
	for selectResult.Rows.Next(ctx) {
		row := selectResult.Rows.Row()
		actual = append(actual, row)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[row.Values[0].AsInt8()]
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

func checkIndexKeys(ctx context.Context, t *testing.T, idx BTreeIndex, expectedKeys []int64) {
	actualKeys := make([]int64, 0, 100)
	err := idx.BFS(ctx, func(page *Page) {
		node := page.IndexNode.(*IndexNode[int64])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	require.Len(t, actualKeys, len(expectedKeys))
	assert.ElementsMatch(t, expectedKeys, actualKeys)
}

func checkIndexVarcharKeys(ctx context.Context, t *testing.T, idx BTreeIndex, expectedKeys []string) {
	actualKeys := make([]string, 0, 100)
	err := idx.BFS(ctx, func(page *Page) {
		node := page.IndexNode.(*IndexNode[string])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	require.Len(t, actualKeys, len(expectedKeys))
	assert.ElementsMatch(t, expectedKeys, actualKeys)
}

func checkRowsWithCompositePrimaryKey(ctx context.Context, t *testing.T, table *Table, expectedRows []Row) {
	selectResult, err := table.Select(ctx, Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(table.Columns...),
	})
	require.NoError(t, err)

	expectedIDMap := map[string]struct{}{}
	for _, r := range expectedRows {
		firstName, ok := r.GetValue("first_name")
		require.True(t, ok)
		lastName, ok := r.GetValue("last_name")
		require.True(t, ok)
		expectedIDMap[fmt.Sprintf("%s|%s", firstName.AsTextPointer().String(), lastName.AsTextPointer().String())] = struct{}{}
	}

	var actual []Row
	for selectResult.Rows.Next(ctx) {
		row := selectResult.Rows.Row()
		actual = append(actual, row)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[fmt.Sprintf("%s|%s", row.Values[1].AsTextPointer().String(), row.Values[2].AsTextPointer().String())]
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

func checkCompositeIndexKeys(ctx context.Context, t *testing.T, idx BTreeIndex, expectedKeys []CompositeKey) {
	actualKeys := make([]CompositeKey, 0, 100)
	err := idx.BFS(ctx, func(page *Page) {
		node := page.IndexNode.(*IndexNode[CompositeKey])
		actualKeys = append(actualKeys, node.Keys()...)
	})
	require.NoError(t, err)

	require.Len(t, actualKeys, len(expectedKeys))
	assert.ElementsMatch(t, expectedKeys, actualKeys)
}
