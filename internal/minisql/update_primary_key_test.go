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

	txPrimaryKeyPager := NewTransactionalPager(
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
		freePage, err := txPrimaryKeyPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		table.PrimaryKey.Index, err = table.createBTreeIndex(
			txPrimaryKeyPager,
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
			expected[i], _ = expected[i].SetValue("id", OptionalValue{Value: int64(42), Valid: true})
		}

		checkRows(ctx, t, table, expected)
	})
}

func TestTable_Update_CompositePrimaryKey(t *testing.T) {
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

	txPrimaryKeyPager := NewTransactionalPager(
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
		freePage, err := txPrimaryKeyPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		table.PrimaryKey.Index, err = table.createBTreeIndex(
			txPrimaryKeyPager,
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

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDuplicateKey)
		assert.Equal(t, 0, result.RowsAffected)

		checkRowsWithCompositePrimaryKey(ctx, t, table, rows)
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

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 0, result.RowsAffected)

		checkRowsWithCompositePrimaryKey(ctx, t, table, rows)
	})

	expected := make([]Row, 0, len(rows))
	for _, row := range rows {
		expected = append(expected, row.Clone())
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

		checkRowsWithCompositePrimaryKey(ctx, t, table, expected)
	})
}
