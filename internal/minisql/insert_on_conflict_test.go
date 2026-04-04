package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Insert_OnConflictDoUpdate_UniqueIndex(t *testing.T) {
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
		txIndexPager  = NewTransactionalPager(indexPager, txManager, testTableName, indexName)
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

	t.Run("ON CONFLICT DO UPDATE inserts non-conflicting row normally", func(t *testing.T) {
		newRow := gen.RowWithUniqueIndex()
		stmt := Statement{
			Kind:           Insert,
			Fields:         fieldsFromColumns(table.Columns...),
			ConflictAction: ConflictActionDoUpdate,
			Inserts:        [][]OptionalValue{newRow.Values},
			Updates: map[string]OptionalValue{
				"id": {Value: int64(9999), Valid: true},
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

	t.Run("ON CONFLICT DO UPDATE applies SET on conflicting row", func(t *testing.T) {
		newID := int64(42)
		stmt := Statement{
			Kind:           Insert,
			Fields:         fieldsFromColumns(table.Columns...),
			ConflictAction: ConflictActionDoUpdate,
			// Reuse existing email (rows[0]) — triggers unique index conflict.
			Inserts: [][]OptionalValue{rows[0].Values},
			Updates: map[string]OptionalValue{
				"id": {Value: newID, Valid: true},
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

		// Verify that the conflicting row now has the updated id.
		selectResult, err := table.Select(ctx, Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
						Operator: Eq,
						Operand2: Operand{Type: OperandQuotedString, Value: rows[0].Values[1].Value},
					},
				},
			},
		})
		require.NoError(t, err)
		require.True(t, selectResult.Rows.Next(ctx))
		updatedRow := selectResult.Rows.Row()
		assert.Equal(t, newID, updatedRow.Values[0].Value.(int64))
	})

	t.Run("ON CONFLICT DO UPDATE with no actual change reports 0 rows affected", func(t *testing.T) {
		// Update id to the same value it already has — no change.
		existingID := rows[1].Values[0].Value.(int64)
		stmt := Statement{
			Kind:           Insert,
			Fields:         fieldsFromColumns(table.Columns...),
			ConflictAction: ConflictActionDoUpdate,
			Inserts:        [][]OptionalValue{rows[1].Values},
			Updates: map[string]OptionalValue{
				"id": {Value: existingID, Valid: true},
			},
		}

		var result StatementResult
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Insert(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 0, result.RowsAffected)
	})

	t.Run("ON CONFLICT DO UPDATE with mixed batch handles each row independently", func(t *testing.T) {
		newRow := gen.RowWithUniqueIndex()
		// rows[2] conflicts; newRow does not.
		stmt := Statement{
			Kind:           Insert,
			Fields:         fieldsFromColumns(table.Columns...),
			ConflictAction: ConflictActionDoUpdate,
			Inserts: [][]OptionalValue{
				rows[2].Values, // conflict → update
				newRow.Values,  // no conflict → insert
			},
			Updates: map[string]OptionalValue{
				"id": {Value: int64(100), Valid: true},
			},
		}

		var result StatementResult
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Insert(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		// One update (rows[2]) + one insert (newRow).
		assert.Equal(t, 2, result.RowsAffected)
	})
}

func TestTable_Insert_OnConflictDoUpdate_ExcludedRef_UniqueIndex(t *testing.T) {
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
		txIndexPager  = NewTransactionalPager(indexPager, txManager, testTableName, indexName)
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

	t.Run("EXCLUDED.col resolves to proposed value for single-row upsert", func(t *testing.T) {
		newID := int64(77)
		// Same email as rows[0] → conflict. EXCLUDED.id should resolve to newID.
		conflictingValues := []OptionalValue{
			{Value: newID, Valid: true},
			rows[0].Values[1], // same email
		}
		stmt := Statement{
			Kind:           Insert,
			Fields:         append(fieldsFromColumns(table.Columns...), Field{Name: "id"}),
			ConflictAction: ConflictActionDoUpdate,
			Inserts:        [][]OptionalValue{conflictingValues},
			Updates: map[string]OptionalValue{
				"id": {Value: ExcludedRef{Column: "id"}, Valid: true},
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

		// Verify the id was updated to the proposed value.
		selectResult, err := table.Select(ctx, Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
						Operator: Eq,
						Operand2: Operand{Type: OperandQuotedString, Value: rows[0].Values[1].Value},
					},
				},
			},
		})
		require.NoError(t, err)
		require.True(t, selectResult.Rows.Next(ctx))
		assert.Equal(t, newID, selectResult.Rows.Row().Values[0].Value.(int64))
	})

	t.Run("EXCLUDED.col in multi-row insert applies per-row proposed value", func(t *testing.T) {
		// rows[1] and rows[2] both conflict. Each should get their own proposed id.
		newID1, newID2 := int64(101), int64(102)
		stmt := Statement{
			Kind:  Insert,
			Fields: append(fieldsFromColumns(table.Columns...), Field{Name: "id"}),
			ConflictAction: ConflictActionDoUpdate,
			Inserts: [][]OptionalValue{
				{{Value: newID1, Valid: true}, rows[1].Values[1]},
				{{Value: newID2, Valid: true}, rows[2].Values[1]},
			},
			Updates: map[string]OptionalValue{
				"id": {Value: ExcludedRef{Column: "id"}, Valid: true},
			},
		}

		var result StatementResult
		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Insert(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 2, result.RowsAffected)

		// rows[1] → newID1
		sel1, err := table.Select(ctx, Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandQuotedString, Value: rows[1].Values[1].Value},
				},
			}},
		})
		require.NoError(t, err)
		require.True(t, sel1.Rows.Next(ctx))
		assert.Equal(t, newID1, sel1.Rows.Row().Values[0].Value.(int64))

		// rows[2] → newID2
		sel2, err := table.Select(ctx, Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandQuotedString, Value: rows[2].Values[1].Value},
				},
			}},
		})
		require.NoError(t, err)
		require.True(t, sel2.Rows.Next(ctx))
		assert.Equal(t, newID2, sel2.Rows.Row().Values[0].Value.(int64))
	})
}

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
