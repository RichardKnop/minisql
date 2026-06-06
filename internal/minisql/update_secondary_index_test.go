package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestTable_Update_SingleSecondaryIndex exercises updateSecondaryIndexKey.
func TestTable_Update_SingleSecondaryIndex(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		indexCol      = testCompositeKeyColumns[3:4] // "email" column
		indexName     = "idx_email"
		tablePager    = pager.ForTable(testCompositeKeyColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		rows          = gen.RowsWithCompositeKey(3)
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

	idxPager, err := pager.ForIndex(indexCol, false)
	require.NoError(t, err)
	txIndexPager := NewTransactionalPager(idxPager, txManager, testTableName, indexName)

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

	t.Run("update email changes secondary index entry", func(t *testing.T) {
		oldEmail, ok := rows[0].GetValue("email")
		require.True(t, ok)

		newEmailStr := "newemail@example.com"
		updateStmt := Statement{
			Kind:      Update,
			TableName: testTableName,
			Columns:   testCompositeKeyColumns,
			Updates: map[string]OptionalValue{
				"email": {Value: NewTextPointer([]byte(newEmailStr)), Valid: true},
			},
			Conditions: OneOrMore{{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
					Operand2: Operand{Type: OperandQuotedString, Value: oldEmail.Value},
					Operator: Eq,
				},
			}},
		}

		err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := table.Update(ctx, updateStmt)
			return err
		})
		require.NoError(t, err)

		// New email should be findable via secondary index scan.
		result, err := table.Select(ctx, Statement{
			Kind:    Select,
			Columns: testCompositeKeyColumns,
			Fields:  fieldsFromColumns(testCompositeKeyColumns...),
			Conditions: OneOrMore{{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "email"}},
					Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte(newEmailStr))},
					Operator: Eq,
				},
			}},
		})
		require.NoError(t, err)
		require.True(t, result.Rows.Next(ctx))
		updatedRow := result.Rows.Row()
		emailVal, ok := updatedRow.GetValue("email")
		require.True(t, ok)
		assert.True(t, emailVal.Valid)
	})
}

// TestSecondaryIndex_RowSatisfiesWhereCond exercises the rowSatisfiesWhereCond helper.
func TestSecondaryIndex_RowSatisfiesWhereCond(t *testing.T) {
	t.Parallel()

	cols := []Column{
		{Name: "status", Kind: Varchar, Size: MaxInlineVarchar},
		{Name: "amount", Kind: Int8, Size: 8},
	}

	makeRow := func(status string, amount int64) Row {
		return Row{
			Columns: cols,
			Values: []OptionalValue{
				{Value: NewTextPointer([]byte(status)), Valid: true},
				{Value: amount, Valid: true},
			},
		}
	}

	statusActiveTP := NewTextPointer([]byte("active"))
	cond := Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "status"}},
		Operand2: Operand{Type: OperandQuotedString, Value: statusActiveTP},
		Operator: Eq,
	}
	indexWhereCond := OneOrMore{{cond}}

	si := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:      "idx_active",
			Columns:   cols[1:2],
			WhereCond: indexWhereCond,
		},
	}

	t.Run("full index (no WhereCond) always satisfied", func(t *testing.T) {
		t.Parallel()
		fullSI := SecondaryIndex{IndexInfo: IndexInfo{Name: "idx_all", Columns: cols[1:2]}}
		ok, err := fullSI.rowSatisfiesWhereCond(makeRow("inactive", 50))
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("row satisfying WHERE condition returns true", func(t *testing.T) {
		t.Parallel()
		ok, err := si.rowSatisfiesWhereCond(makeRow("active", 100))
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("row not satisfying WHERE condition returns false", func(t *testing.T) {
		t.Parallel()
		ok, err := si.rowSatisfiesWhereCond(makeRow("inactive", 100))
		require.NoError(t, err)
		assert.False(t, ok)
	})
}
