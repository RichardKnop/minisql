package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Update_UniqueIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testColumns[0:2])
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithUniqueIndex(10)
		table         *Table
		indexName     = UniqueIndexName(testTableName, "email")
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

	t.Run("Duplicate unique index key error", func(t *testing.T) {
		email1, ok := rows[0].GetValue("email")
		require.True(t, ok)
		email2, ok := rows[1].GetValue("email")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: email2.Value, Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "email"}, OperandInteger, email1.Value.(TextPointer)),
				},
			},
		}

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDuplicateKey)
		assert.Equal(t, 0, result.RowsAffected)

		checkRows(ctx, t, table, rows)
	})

	t.Run("Update unique index key no change", func(t *testing.T) {
		email, ok := rows[0].GetValue("email")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: email.Value, Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "email"}, OperandInteger, email.Value.(TextPointer)),
				},
			},
		}

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 0, result.RowsAffected)

		checkRows(ctx, t, table, rows)
	})

	expected := make([]Row, 0, len(rows))
	for _, row := range rows {
		expected = append(expected, row.Clone())
	}

	t.Run("Update unique index key", func(t *testing.T) {
		email, ok := rows[0].GetValue("email")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: NewTextPointer([]byte("newemail@example.com")), Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "email"}, OperandInteger, email.Value.(TextPointer)),
				},
			},
		}

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 1, result.RowsAffected)

		// Prepare expected rows with one updated row
		for i := range expected {
			if i != 0 {
				continue
			}
			expected[i], _ = expected[i].SetValue("email", OptionalValue{Value: NewTextPointer([]byte("newemail@example.com")), Valid: true})
		}

		checkRows(ctx, t, table, expected)
	})
}

func TestTable_Update_CompositeUniqueIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testCompositeKeyColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithCompositeKey(10)
		table         *Table
		indexName     = UniqueIndexName(testTableName, "email")
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
		pager.ForIndex(
			table.UniqueIndexes[indexName].Columns,
			true,
		),
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

	t.Run("Duplicate unique index key error", func(t *testing.T) {
		firstName, ok := rows[0].GetValue("first_name")
		require.True(t, ok)
		lastName, ok := rows[0].GetValue("last_name")
		require.True(t, ok)

		firstName2, ok := rows[1].GetValue("first_name")
		require.True(t, ok)
		lastName2, ok := rows[1].GetValue("last_name")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"first_name": {Value: firstName2.Value, Valid: true},
				"last_name":  {Value: lastName2.Value, Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "first_name"}, OperandInteger, firstName.Value),
					FieldIsEqual(Field{Name: "last_name"}, OperandInteger, lastName.Value),
				},
			},
		}

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDuplicateKey)
		assert.Equal(t, 0, result.RowsAffected)

		checkRows(ctx, t, table, rows)
	})

	t.Run("Update unique index key no change", func(t *testing.T) {
		firstName, ok := rows[0].GetValue("first_name")
		require.True(t, ok)
		lastName, ok := rows[0].GetValue("last_name")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"first_name": {Value: firstName.Value, Valid: true},
				"last_name":  {Value: lastName.Value, Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "first_name"}, OperandInteger, firstName.Value),
					FieldIsEqual(Field{Name: "last_name"}, OperandInteger, lastName.Value),
				},
			},
		}

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 0, result.RowsAffected)

		checkRows(ctx, t, table, rows)
	})

	expected := make([]Row, 0, len(rows))
	for _, row := range rows {
		expected = append(expected, row.Clone())
	}

	t.Run("Update unique index key", func(t *testing.T) {
		firstName, ok := rows[0].GetValue("first_name")
		require.True(t, ok)
		lastName, ok := rows[0].GetValue("last_name")
		require.True(t, ok)

		newFirstName := firstName.Value.(TextPointer).String() + " 2"
		newLastName := lastName.Value.(TextPointer).String() + " 2"

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"first_name": {Value: NewTextPointer([]byte(newFirstName)), Valid: true},
				"last_name":  {Value: NewTextPointer([]byte(newLastName)), Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "first_name"}, OperandInteger, firstName.Value),
					FieldIsEqual(Field{Name: "last_name"}, OperandInteger, lastName.Value),
				},
			},
		}

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 1, result.RowsAffected)

		// Prepare expected rows with one updated row
		for i := range expected {
			if i != 0 {
				continue
			}
			expected[i], _ = expected[i].SetValue("first_name", OptionalValue{Value: NewTextPointer([]byte(newFirstName)), Valid: true})
			expected[i], _ = expected[i].SetValue("last_name", OptionalValue{Value: NewTextPointer([]byte(newLastName)), Valid: true})
		}

		checkRows(ctx, t, table, expected)
	})
}
