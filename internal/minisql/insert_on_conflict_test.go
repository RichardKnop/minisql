package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Insert_OnConflictDoNothing_UniqueIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testColumns[0:2])
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithUniqueIndex(5)
		table         *Table
		indexName     = UniqueIndexName(testTableName, "email")
		indexPager    = pager.ForIndex(testColumns[1:2], true)
		// Use the same txManager for the index pager so page-version tracking is consistent.
		txIndexPager = NewTransactionalPager(indexPager, txManager, testTableName, indexName)
	)

	// Set up table and insert initial rows
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

	// Insert initial rows
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
		if err != nil {
			return err
		}
		table.UniqueIndexes[indexName] = uniqueIndex

		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(table.Columns...),
			Inserts: make([][]OptionalValue, 0, len(rows)),
		}
		for _, row := range rows {
			stmt.Inserts = append(stmt.Inserts, row.Values)
		}
		_, err = table.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	t.Run("ON CONFLICT DO NOTHING skips conflicting row", func(t *testing.T) {
		// Attempt to insert a duplicate row — should be silently skipped
		stmt := Statement{
			Kind:           Insert,
			Fields:         fieldsFromColumns(table.Columns...),
			ConflictAction: ConflictActionDoNothing,
			Inserts:        [][]OptionalValue{rows[0].Values},
		}

		var result StatementResult
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Insert(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 0, result.RowsAffected)

		// Original rows must be unchanged
		checkRows(ctx, t, table, rows)
	})

	t.Run("ON CONFLICT DO NOTHING inserts non-conflicting rows", func(t *testing.T) {
		newRow := gen.RowWithUniqueIndex()
		stmt := Statement{
			Kind:           Insert,
			Fields:         fieldsFromColumns(table.Columns...),
			ConflictAction: ConflictActionDoNothing,
			Inserts:        [][]OptionalValue{newRow.Values},
		}

		var result StatementResult
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Insert(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 1, result.RowsAffected)
	})

	t.Run("ON CONFLICT DO NOTHING with mixed batch skips conflicts", func(t *testing.T) {
		newRow := gen.RowWithUniqueIndex()
		// Two rows: one conflicting (rows[1]) and one new
		stmt := Statement{
			Kind:           Insert,
			Fields:         fieldsFromColumns(table.Columns...),
			ConflictAction: ConflictActionDoNothing,
			Inserts: [][]OptionalValue{
				rows[1].Values, // conflict
				newRow.Values,  // no conflict
			},
		}

		var result StatementResult
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Insert(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 1, result.RowsAffected)
	})

	t.Run("Without ON CONFLICT duplicate still returns error", func(t *testing.T) {
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
	})
}
