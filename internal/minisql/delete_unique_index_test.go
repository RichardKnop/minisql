package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Delete_UniqueIndex(t *testing.T) {
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
		aTable.Name,
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

	t.Run("Delete single row", func(t *testing.T) {
		email, ok := rows[0].GetValue("email")
		require.True(t, ok)

		stmt := Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsEqual("email", OperandQuotedString, email.Value.(TextPointer)),
				},
			},
		}

		aResult := mustDelete(t, ctx, aTable, txManager, aPager, stmt)

		assert.Equal(t, 1, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows[1:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		aResult := mustDelete(t, ctx, aTable, txManager, aPager, Statement{Kind: Delete})

		assert.Equal(t, 9, aResult.RowsAffected)
		checkRows(ctx, t, aTable, nil)
	})
}
