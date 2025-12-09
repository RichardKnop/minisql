package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Select_PrimaryKey(t *testing.T) {
	aPager := initTest(t)

	var (
		ctx        = context.Background()
		rows       = gen.RowsWithPrimaryKey(38)
		txManager  = NewTransactionManager(zap.NewNop())
		tablePager = NewTransactionalPager(
			aPager.ForTable(testColumns),
			txManager,
		)
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testColumnsWithPrimaryKey, 0)
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

	t.Run("Insert rows with primary key", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(testColumnsWithPrimaryKey...),
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

		checkRows(ctx, t, aTable, rows)
	})

	t.Run("Select all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumnsWithPrimaryKey...),
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows, aResult.CollectRows(ctx))
	})

	t.Run("Select single row by primary key (index scan)", func(t *testing.T) {
		id := rowIDs(rows[5])[0]
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumnsWithPrimaryKey...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "id",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: id,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		actual := aResult.CollectRows(ctx)
		assert.Len(t, actual, 1)
		assert.Equal(t, rows[5], actual[0])
	})

	t.Run("Select multiple rows by primary key and other column (sequential scan)", func(t *testing.T) {
		var (
			id       = rowIDs(rows[5])[0]
			email, _ = rows[15].GetValue("email")
		)
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumnsWithPrimaryKey...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "id",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: id,
						},
					},
				},
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: email.Value,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		expected := []Row{rows[5].Clone(), rows[15].Clone()}
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})
}
