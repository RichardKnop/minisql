package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Update(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx            = context.Background()
		rows           = gen.Rows(38)
		tablePager     = pager.ForTable(testColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testColumns, 0, nil)
	)

	// Batch insert test rows
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, row.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Insert(ctx, insertStmt)
		return err
	})
	require.NoError(t, err)

	t.Run("Update no rows", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: NewTextPointer([]byte("updatednone@foo.bar")), Valid: true},
			},
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "email"},
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: NewTextPointer([]byte("bogus")),
						},
					},
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

	t.Run("Update single row", func(t *testing.T) {
		id, ok := rows[5].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email":   {Value: NewTextPointer([]byte("updatedsingle@foo.bar")), Valid: true},
				"created": {Value: MustParseTimestamp("2000-01-01 00:00:00"), Valid: true},
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
		expected := make([]Row, 0, len(rows))
		for i, row := range rows {
			expectedRow := row.Clone()
			if i == 5 {
				expectedRow, _ = expectedRow.SetValue("email", OptionalValue{Value: NewTextPointer([]byte("updatedsingle@foo.bar")), Valid: true})
				expectedRow, _ = expectedRow.SetValue("created", OptionalValue{Value: MustParseTimestamp("2000-01-01 00:00:00"), Valid: true})
				rows[i] = expectedRow
			}

			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, table, expected)
	})

	t.Run("Update single row, set column to NULL", func(t *testing.T) {
		id, ok := rows[18].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Valid: false},
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
		expected := make([]Row, 0, len(rows))
		for i, row := range rows {
			expectedRow := row.Clone()
			if i == 18 {
				expectedRow, _ = expectedRow.SetValue("email", OptionalValue{Valid: false})
			}

			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, table, expected)
	})

	t.Run("Update all rows", func(t *testing.T) {
		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"email": {Value: NewTextPointer([]byte("updatedall@foo.bar")), Valid: true},
			},
		}

		var result StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			result, err = table.Update(ctx, stmt)
			return err
		})
		require.NoError(t, err)
		assert.Equal(t, 38, result.RowsAffected)

		// Prepare expected rows with all rows updated
		expected := make([]Row, 0, len(rows))
		for _, row := range rows {
			expectedRow := row.Clone()
			expectedRow, _ = expectedRow.SetValue("email", OptionalValue{Value: NewTextPointer([]byte("updatedall@foo.bar")), Valid: true})
			expected = append(expected, expectedRow)
		}

		checkRows(ctx, t, table, expected)
	})
}

func TestTable_Update_Overflow(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = pager.ForTable(testOverflowColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testOverflowColumns, 0, nil)
		rows           = gen.OverflowRows(3, []uint32{
			MaxInlineVarchar,          // inline text
			MaxInlineVarchar + 100,    // text overflows to 1 page
			MaxOverflowPageData + 100, // text overflows to multiple pages
		})
		updatedOverflowText       = gen.textOfLength(MaxInlineVarchar + 200)
		updatedInlineText         = gen.textOfLength(MaxInlineVarchar)
		updatedShrunkOverflowText = gen.textOfLength(MaxOverflowPageData - 100)
		expandedOverflowText      = gen.textOfLength(MaxOverflowPageData + 200)
	)

	// Batch insert test rows
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testOverflowColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, row.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Insert(ctx, insertStmt)
		return err
	})
	require.NoError(t, err)

	require.Equal(t, 4, int(pager.TotalPages()))

	expected := make([]Row, 0, len(rows))
	for _, row := range rows {
		expected = append(expected, row.Clone())
	}

	t.Run("Update inline text to overflow text", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"profile": {Value: updatedOverflowText, Valid: true},
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
			expected[i], _ = expected[i].SetValue("profile", OptionalValue{Value: updatedOverflowText, Valid: true})
		}

		checkRows(ctx, t, table, expected)

		require.Equal(t, 5, int(pager.TotalPages()))
		assert.NotNil(t, pager.pages[0].LeafNode)
		assert.NotNil(t, pager.pages[1].OverflowPage)
		assert.NotNil(t, pager.pages[2].OverflowPage)
		assert.NotNil(t, pager.pages[3].OverflowPage)
		assert.NotNil(t, pager.pages[4].OverflowPage)

		assert.Equal(t, 0, int(pager.pages[1].OverflowPage.Header.NextPage))
		assert.Equal(t, pager.pages[3].Index, pager.pages[2].OverflowPage.Header.NextPage)
		assert.Equal(t, 0, int(pager.pages[3].OverflowPage.Header.NextPage))
		assert.Equal(t, 0, int(pager.pages[4].OverflowPage.Header.NextPage))

		assert.Equal(t, MaxOverflowPageData, int(pager.pages[2].OverflowPage.Header.DataSize))
		assert.Equal(t, 100, int(pager.pages[3].OverflowPage.Header.DataSize))
		assert.Equal(t, 455, int(pager.pages[4].OverflowPage.Header.DataSize))
	})

	t.Run("Update overflow text to inline text", func(t *testing.T) {
		id, ok := rows[1].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"profile": {Value: updatedInlineText, Valid: true},
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

		// Prepare expected rows with second updated row
		for i := range expected {
			if i != 1 {
				continue
			}
			expected[i], _ = expected[i].SetValue("profile", OptionalValue{Value: updatedInlineText, Valid: true})
		}

		checkRows(ctx, t, table, expected)

		require.Equal(t, 5, int(pager.TotalPages()))
		assert.NotNil(t, pager.pages[0].LeafNode)
		assert.NotNil(t, pager.pages[1].FreePage) // freed overflow page
		assert.NotNil(t, pager.pages[2].OverflowPage)
		assert.NotNil(t, pager.pages[3].OverflowPage)
		assert.NotNil(t, pager.pages[4].OverflowPage)

		assert.Equal(t, pager.pages[3].Index, pager.pages[2].OverflowPage.Header.NextPage)
		assert.Equal(t, 0, int(pager.pages[3].OverflowPage.Header.NextPage))
		assert.Equal(t, 0, int(pager.pages[4].OverflowPage.Header.NextPage))

		assert.Equal(t, MaxOverflowPageData, int(pager.pages[2].OverflowPage.Header.DataSize))
		assert.Equal(t, 100, int(pager.pages[3].OverflowPage.Header.DataSize))
		assert.Equal(t, 455, int(pager.pages[4].OverflowPage.Header.DataSize))

		assertFreePages(t, tablePager, []PageIndex{1})
	})

	t.Run("Update overflow text to shrink overflow pages from 2 to 1", func(t *testing.T) {
		id, ok := rows[2].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"profile": {Value: updatedShrunkOverflowText, Valid: true},
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

		// Prepare expected rows with third updated row
		for i := range expected {
			if i != 2 {
				continue
			}
			expected[i], _ = expected[i].SetValue("profile", OptionalValue{Value: updatedShrunkOverflowText, Valid: true})
		}

		checkRows(ctx, t, table, expected)

		require.Equal(t, 5, int(pager.TotalPages()))
		assert.NotNil(t, pager.pages[0].LeafNode)
		assert.NotNil(t, pager.pages[1].FreePage)
		assert.NotNil(t, pager.pages[2].FreePage) // freed overflow page
		// freed overflow page which gets reused when shrinking from 2 to 1 overflow pages
		assert.NotNil(t, pager.pages[3].OverflowPage)
		assert.NotNil(t, pager.pages[4].OverflowPage)

		assert.Equal(t, 0, int(pager.pages[3].OverflowPage.Header.NextPage))
		assert.Equal(t, 0, int(pager.pages[4].OverflowPage.Header.NextPage))

		assert.Equal(t, MaxOverflowPageData-100, int(pager.pages[3].OverflowPage.Header.DataSize))
		assert.Equal(t, 455, int(pager.pages[4].OverflowPage.Header.DataSize))

		assertFreePages(t, tablePager, []PageIndex{2, 1})
	})

	t.Run("Update overflow text to expand overflow pages from 1 to 2", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		stmt := Statement{
			Kind: Update,
			Updates: map[string]OptionalValue{
				"profile": {Value: expandedOverflowText, Valid: true},
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

		// Prepare expected rows with first updated row (re-expanded)
		for i := range expected {
			if i != 0 {
				continue
			}
			expected[i], _ = expected[i].SetValue("profile", OptionalValue{Value: expandedOverflowText, Valid: true})
		}

		checkRows(ctx, t, table, expected)

		require.Equal(t, 5, int(pager.TotalPages()))
		assert.NotNil(t, pager.pages[0].LeafNode)
		assert.NotNil(t, pager.pages[1].FreePage)
		// this free page gets reused when expanding from 1 to 2 overflow pages
		assert.NotNil(t, pager.pages[2].OverflowPage)
		// freed overflow page which gets reused when shrinking from 2 to 1 overflow pages
		assert.NotNil(t, pager.pages[3].OverflowPage)
		assert.NotNil(t, pager.pages[4].OverflowPage)

		assert.Equal(t, 0, int(pager.pages[3].OverflowPage.Header.NextPage))
		assert.Equal(t, 2, int(pager.pages[4].OverflowPage.Header.NextPage))
		assert.Equal(t, 0, int(pager.pages[2].OverflowPage.Header.NextPage))

		assert.Equal(t, MaxOverflowPageData-100, int(pager.pages[3].OverflowPage.Header.DataSize))
		assert.Equal(t, MaxOverflowPageData, int(pager.pages[4].OverflowPage.Header.DataSize))
		assert.Equal(t, 200, int(pager.pages[2].OverflowPage.Header.DataSize))

		assertFreePages(t, tablePager, []PageIndex{1})
	})
}
