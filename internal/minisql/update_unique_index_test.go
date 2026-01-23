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
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testColumns[0:2])
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithUniqueIndex(10)
		aTable         *Table
		indexName      = UniqueIndexName(testTableName, "email")
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
		aPager.ForIndex(
			aTable.UniqueIndexes[indexName].Columns,
			true,
		),
		aTable.txManager,
		testTableName,
		aTable.UniqueIndexes[indexName].Name,
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
		uniqueIndex := aTable.UniqueIndexes[indexName]
		uniqueIndex.Index, err = aTable.createBTreeIndex(
			txIndexPager,
			freePage,
			aTable.UniqueIndexes[indexName].Columns,
			aTable.UniqueIndexes[indexName].Name,
			true,
		)
		aTable.UniqueIndexes[indexName] = uniqueIndex
		if err != nil {
			return err
		}
		_, err = aTable.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	checkRows(ctx, t, aTable, rows)

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
					FieldIsEqual("email", OperandInteger, email1.Value.(TextPointer)),
				},
			},
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateKey)
		assert.Equal(t, 0, aResult.RowsAffected)

		checkRows(ctx, t, aTable, rows)
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
					FieldIsEqual("email", OperandInteger, email.Value.(TextPointer)),
				},
			},
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 0, aResult.RowsAffected)

		checkRows(ctx, t, aTable, rows)
	})

	expected := make([]Row, 0, len(rows))
	for _, aRow := range rows {
		expected = append(expected, aRow.Clone())
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
					FieldIsEqual("email", OperandInteger, email.Value.(TextPointer)),
				},
			},
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 1, aResult.RowsAffected)

		// Prepare expected rows with one updated row
		for i, aRow := range expected {
			if i != 0 {
				continue
			}
			aRow, _ = aRow.SetValue("email", OptionalValue{Value: NewTextPointer([]byte("newemail@example.com")), Valid: true})
		}

		checkRows(ctx, t, aTable, expected)
	})
}

func TestTable_Update_CompositeUniqueIndex(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testCompositeKeyColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows           = gen.RowsWithCompositeKey(10)
		aTable         *Table
		indexName      = UniqueIndexName(testTableName, "email")
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
		aPager.ForIndex(
			aTable.UniqueIndexes[indexName].Columns,
			true,
		),
		aTable.txManager,
		testTableName,
		aTable.UniqueIndexes[indexName].Name,
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
		uniqueIndex := aTable.UniqueIndexes[indexName]
		uniqueIndex.Index, err = aTable.createBTreeIndex(
			txIndexPager,
			freePage,
			aTable.UniqueIndexes[indexName].Columns,
			aTable.UniqueIndexes[indexName].Name,
			true,
		)
		aTable.UniqueIndexes[indexName] = uniqueIndex
		if err != nil {
			return err
		}
		_, err = aTable.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	checkRows(ctx, t, aTable, rows)

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
					FieldIsEqual("first_name", OperandInteger, firstName.Value),
					FieldIsEqual("last_name", OperandInteger, lastName.Value),
				},
			},
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateKey)
		assert.Equal(t, 0, aResult.RowsAffected)

		checkRows(ctx, t, aTable, rows)
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
					FieldIsEqual("first_name", OperandInteger, firstName.Value),
					FieldIsEqual("last_name", OperandInteger, lastName.Value),
				},
			},
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 0, aResult.RowsAffected)

		checkRows(ctx, t, aTable, rows)
	})

	expected := make([]Row, 0, len(rows))
	for _, aRow := range rows {
		expected = append(expected, aRow.Clone())
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
					FieldIsEqual("first_name", OperandInteger, firstName.Value),
					FieldIsEqual("last_name", OperandInteger, lastName.Value),
				},
			},
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 1, aResult.RowsAffected)

		// Prepare expected rows with one updated row
		for i, aRow := range expected {
			if i != 0 {
				continue
			}
			aRow, _ = aRow.SetValue("first_name", OptionalValue{Value: NewTextPointer([]byte(newFirstName)), Valid: true})
			aRow, _ = aRow.SetValue("last_name", OptionalValue{Value: NewTextPointer([]byte(newLastName)), Valid: true})
		}

		checkRows(ctx, t, aTable, expected)
	})
}
