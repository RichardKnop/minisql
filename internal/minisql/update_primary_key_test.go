package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Update_PrimaryKey(t *testing.T) {
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

	txPrimaryKeyPager := NewTransactionalPager(
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
		freePage, err := txPrimaryKeyPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		aTable.PrimaryKey.Index, err = aTable.createBTreeIndex(
			txPrimaryKeyPager,
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

	t.Run("Duplicate primary key error", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)
		id2, ok := rows[1].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"id": {Value: id2.Value, Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "id"}, OperandInteger, id.Value.(int64)),
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

	t.Run("Update primary key no change", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"id": {Value: id.Value, Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "id"}, OperandInteger, id.Value.(int64)),
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

	t.Run("Update primary key", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"id": {Value: int64(42), Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "id"}, OperandInteger, id.Value.(int64)),
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
			aRow, _ = aRow.SetValue("id", OptionalValue{Value: int64(42), Valid: true})
		}

		checkRows(ctx, t, aTable, expected)
	})
}

func TestTable_Update_CompositePrimaryKey(t *testing.T) {
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

	txPrimaryKeyPager := NewTransactionalPager(
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
		freePage, err := txPrimaryKeyPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		aTable.PrimaryKey.Index, err = aTable.createBTreeIndex(
			txPrimaryKeyPager,
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

	t.Run("Duplicate primary key error", func(t *testing.T) {
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
				"first_name": {Value: firstName2.Value.(TextPointer), Valid: true},
				"last_name":  {Value: lastName2.Value.(TextPointer), Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "first_name"}, OperandQuotedString, firstName.Value.(TextPointer)),
					FieldIsEqual(Field{Name: "last_name"}, OperandQuotedString, lastName.Value.(TextPointer)),
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

		checkRowsWithCompositePrimaryKey(ctx, t, aTable, rows)
	})

	t.Run("Update primary key no change", func(t *testing.T) {
		firstName, ok := rows[0].GetValue("first_name")
		require.True(t, ok)
		lastName, ok := rows[0].GetValue("last_name")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"first_name": {Value: firstName.Value.(TextPointer), Valid: true},
				"last_name":  {Value: lastName.Value.(TextPointer), Valid: true},
			},
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "first_name"}, OperandQuotedString, firstName.Value.(TextPointer)),
					FieldIsEqual(Field{Name: "last_name"}, OperandQuotedString, lastName.Value.(TextPointer)),
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

		checkRowsWithCompositePrimaryKey(ctx, t, aTable, rows)
	})

	expected := make([]Row, 0, len(rows))
	for _, aRow := range rows {
		expected = append(expected, aRow.Clone())
	}

	t.Run("Update primary key", func(t *testing.T) {
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
					FieldIsEqual(Field{Name: "first_name"}, OperandQuotedString, firstName.Value),
					FieldIsEqual(Field{Name: "last_name"}, OperandQuotedString, lastName.Value),
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

		checkRowsWithCompositePrimaryKey(ctx, t, aTable, expected)
	})
}
