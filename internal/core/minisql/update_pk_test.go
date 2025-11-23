package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Update_PrimaryKey(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager()
		tablePager = NewTransactionalPager(
			aPager.ForTable(testColumnsWithPrimaryKey),
			txManager,
		)
		rows   = gen.RowsWithPrimaryKey(10)
		aTable *Table
	)

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		freePage, err := tablePager.GetFreePage(ctx)
		if err != nil {
			return err
		}
		freePage.LeafNode = NewLeafNode()
		freePage.LeafNode.Header.IsRoot = true
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testColumnsWithPrimaryKey, freePage.Index)
		return nil
	}, aPager)
	require.NoError(t, err)

	primaryKeyPager := NewTransactionalPager(
		aPager.ForIndex(aTable.PrimaryKey.Column.Kind, uint64(aTable.PrimaryKey.Column.Size)),
		aTable.txManager,
	)

	expected := make([]Row, 0, len(rows))
	for _, aRow := range rows {
		expected = append(expected, aRow.Clone())
	}

	t.Run("Insert rows with primary key", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  columnNames(testColumnsWithPrimaryKey...),
			Inserts: make([][]OptionalValue, 0, len(rows)),
		}
		for _, aRow := range rows {
			stmt.Inserts = append(stmt.Inserts, aRow.Values)
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			freePage, err := primaryKeyPager.GetFreePage(ctx)
			if err != nil {
				return err
			}
			aTable.PrimaryKey.Index, err = aTable.newPrimaryKeyIndex(primaryKeyPager, freePage)
			if err != nil {
				return err
			}
			return aTable.Insert(ctx, stmt)
		}, aPager)
		require.NoError(t, err)
	})

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
			Conditions: FieldIsInAny("id", OperandInteger, id.Value.(int64)),
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		}, aPager)
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
			Conditions: FieldIsInAny("id", OperandInteger, id.Value.(int64)),
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		}, aPager)
		require.NoError(t, err)
		assert.Equal(t, 0, aResult.RowsAffected)

		checkRows(ctx, t, aTable, rows)
	})

	t.Run("Update primary key", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"id": {Value: int64(42), Valid: true},
			},
			Conditions: FieldIsInAny("id", OperandInteger, id.Value.(int64)),
		}

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Update(ctx, stmt)
			return err
		}, aPager)
		require.NoError(t, err)
		assert.Equal(t, 1, aResult.RowsAffected)

		// Prepare expected rows with one updated row
		for i, aRow := range expected {
			if i == 0 {
				aRow.SetValue("id", OptionalValue{Value: int64(42), Valid: true})
			}
		}

		checkRows(ctx, t, aTable, expected)
	})
}
