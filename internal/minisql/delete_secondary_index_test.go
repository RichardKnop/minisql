package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestTable_Delete_SingleSecondaryIndex exercises the single-column secondary
// index deletion path (deleteSecondaryIndexKeys for a non-composite index).
func TestTable_Delete_SingleSecondaryIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		indexCol      = testCompositeKeyColumns[3:4] // "email" column
		indexName     = "idx_email"
		tablePager    = pager.ForTable(testCompositeKeyColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithCompositeKey(5)
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
		)
		return nil
	})
	require.NoError(t, err)

	txIndexPager := NewTransactionalPager(
		pager.ForIndex(indexCol, false),
		txManager,
		testTableName,
		indexName,
	)

	// Build the secondary index and insert rows.
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testCompositeKeyColumns...),
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
		idx, err := table.createBTreeIndex(txIndexPager, freePage, indexCol, indexName, false)
		if err != nil {
			return err
		}
		table.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: indexName, Columns: indexCol}, Index: idx})
		_, err = table.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	t.Run("Delete single row removes it from secondary index", func(t *testing.T) {
		emailVal, ok := rows[0].GetValue("email")
		require.True(t, ok)

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, emailVal.Value)},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{Kind: Delete})

		assert.Equal(t, 4, result.RowsAffected)
		checkRows(ctx, t, table, nil)
	})
}

// TestTable_Delete_CompositeSecondaryIndex exercises the composite secondary
// index deletion path (deleteCompositeSecondaryIndexKey).
func TestTable_Delete_CompositeSecondaryIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		indexCols     = testCompositeKeyColumns[1:3] // first_name, last_name
		indexName     = "idx_full_name"
		tablePager    = pager.ForTable(testCompositeKeyColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithCompositeKey(5)
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
		)
		return nil
	})
	require.NoError(t, err)

	txIndexPager := NewTransactionalPager(
		pager.ForIndex(indexCols, false),
		txManager,
		testTableName,
		indexName,
	)

	// Build the composite secondary index and insert rows.
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testCompositeKeyColumns...),
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
		idx, err := table.createBTreeIndex(txIndexPager, freePage, indexCols, indexName, false)
		if err != nil {
			return err
		}
		table.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: indexName, Columns: indexCols}, Index: idx})
		_, err = table.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	t.Run("Delete single row removes it from composite secondary index", func(t *testing.T) {
		firstNameVal, ok := rows[0].GetValue("first_name")
		require.True(t, ok)
		lastNameVal, ok := rows[0].GetValue("last_name")
		require.True(t, ok)

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "first_name"}, OperandQuotedString, firstNameVal.Value),
					FieldIsEqual(Field{Name: "last_name"}, OperandQuotedString, lastNameVal.Value),
				},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{Kind: Delete})

		assert.Equal(t, 4, result.RowsAffected)
		checkRows(ctx, t, table, nil)
	})
}
