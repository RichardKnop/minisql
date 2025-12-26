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
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager(zap.NewNop())
		tablePager = NewTransactionalPager(
			aPager.ForTable(testColumnsWithUniqueIndex),
			txManager,
		)
		rows      = gen.RowsWithUniqueIndex(10)
		aTable    *Table
		indexName = uniqueIndexName(testTableName, "email")
	)

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := tablePager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		freePage.LeafNode = NewLeafNode()
		freePage.LeafNode.Header.IsRoot = true
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testColumnsWithUniqueIndex, freePage.Index)
		return nil
	}, TxCommitter{aPager, nil})
	require.NoError(t, err)

	indexPager := NewTransactionalPager(
		aPager.ForIndex(
			aTable.UniqueIndexes[indexName].Column.Kind,
			uint64(aTable.UniqueIndexes[indexName].Column.Size),
			true,
		),
		aTable.txManager,
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
		freePage, err := indexPager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		uniqueIndex := aTable.UniqueIndexes[indexName]
		uniqueIndex.Index, err = aTable.createBTreeIndex(
			indexPager,
			freePage,
			aTable.UniqueIndexes[indexName].Column,
			aTable.UniqueIndexes[indexName].Name,
			true,
		)
		aTable.UniqueIndexes[indexName] = uniqueIndex
		if err != nil {
			return err
		}
		_, err = aTable.Insert(ctx, stmt)
		return err
	}, TxCommitter{aPager, nil})
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
		}, TxCommitter{aPager, nil})
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
		}, TxCommitter{aPager, nil})
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
		}, TxCommitter{aPager, nil})
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
