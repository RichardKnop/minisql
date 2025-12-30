package minisql

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Select_UniqueIndex(t *testing.T) {
	aPager, dbFile := initTest(t)

	var (
		ctx        = context.Background()
		rows       = gen.RowsWithUniqueIndex(38)
		tablePager = aPager.ForTable(testColumns[0:2])
		txManager  = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager    = NewTransactionalPager(tablePager, txManager, testTableName, "")
		aTable     *Table
		indexName  = UniqueIndexName(testTableName, "email")
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

	var (
		indexPager   = aPager.ForIndex(Varchar, true)
		txIndexPager = NewTransactionalPager(
			indexPager,
			aTable.txManager,
			testTableName,
			aTable.UniqueIndexes[indexName].Name,
		)
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
			aTable.UniqueIndexes[indexName].Columns[0],
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

	t.Run("Select all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows, collectRows(ctx, aResult))
	})

	t.Run("Select single row by unique index key - index scan", func(t *testing.T) {
		email := rows[5].Values[1].Value.(TextPointer)
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: Eq,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: email,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		actual := collectRows(ctx, aResult)
		assert.Len(t, actual, 1)
		assert.Equal(t, rows[5], actual[0])
	})

	t.Run("Select multiple rows by unique index - index scan", func(t *testing.T) {
		emails := []any{
			rows[5].Values[1].Value.(TextPointer),
			rows[11].Values[1].Value.(TextPointer),
			rows[12].Values[1].Value.(TextPointer),
			rows[33].Values[1].Value.(TextPointer),
		}
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: In,
						Operand2: Operand{
							Type:  OperandList,
							Value: emails,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows 5, 11, 12, and 33
		expected := make([]Row, 0, len(emails))
		for i, aRow := range rows {
			if i != 5 && i != 11 && i != 12 && i != 33 {
				continue
			}
			expected = append(expected, aRow)
		}
		assert.Equal(t, expected, collectRows(ctx, aResult))
	})

	t.Run("Select rows where unique key is NOT IN - sequential scan", func(t *testing.T) {
		emails := []any{
			rows[5].Values[1].Value.(TextPointer),
			rows[11].Values[1].Value.(TextPointer),
			rows[12].Values[1].Value.(TextPointer),
			rows[33].Values[1].Value.(TextPointer),
		}
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: NotIn,
						Operand2: Operand{
							Type:  OperandList,
							Value: emails,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows other than 5, 11, 12, and 33
		expected := make([]Row, 0, len(emails))
		for i, aRow := range rows {
			if i == 5 || i == 11 || i == 12 || i == 33 {
				continue
			}
			expected = append(expected, aRow)
		}
		actual := collectRows(ctx, aResult)

		assert.Equal(t, expected, actual)
	})

	rowsOrderedByEmail := make([]Row, len(rows))
	copy(rowsOrderedByEmail, rows)
	sort.Slice(rowsOrderedByEmail, func(i, j int) bool {
		email1, _ := rowsOrderedByEmail[i].GetValue("email")
		email2, _ := rowsOrderedByEmail[j].GetValue("email")
		return email1.Value.(TextPointer).String() < email2.Value.(TextPointer).String()
	})

	t.Run("Select rows by range with lower bound - range scan", func(t *testing.T) {
		email := rowsOrderedByEmail[10].Values[1].Value.(TextPointer)
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: Gt,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: email,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows 12 and onwards
		expected := make([]Row, 0, len(rowsOrderedByEmail)-11)
		for i, aRow := range rowsOrderedByEmail {
			if i < 11 {
				continue
			}
			expected = append(expected, aRow)
		}
		actual := collectRows(ctx, aResult)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select rows by range with upper bound - range scan", func(t *testing.T) {
		email := rowsOrderedByEmail[30].Values[1].Value.(TextPointer)
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: Lt,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: email,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows until 30
		expected := make([]Row, 0, len(rowsOrderedByEmail)-9)
		for i, aRow := range rowsOrderedByEmail {
			if i >= 30 {
				continue
			}
			expected = append(expected, aRow)
		}
		actual := collectRows(ctx, aResult)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select rows by range with both lower and upper bound - range scan", func(t *testing.T) {
		email1 := rowsOrderedByEmail[10].Values[1].Value.(TextPointer)
		email2 := rowsOrderedByEmail[30].Values[1].Value.(TextPointer)
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: Gte,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: email1,
						},
					},
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "email",
						},
						Operator: Lte,
						Operand2: Operand{
							Type:  OperandQuotedString,
							Value: email2,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows between 10 and 30 inclusive
		expected := make([]Row, 0, len(rowsOrderedByEmail)-9)
		for i, aRow := range rowsOrderedByEmail {
			if i < 10 || i > 30 {
				continue
			}
			expected = append(expected, aRow)
		}
		actual := collectRows(ctx, aResult)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select multiple rows by unique index key and other column - sequential scan", func(t *testing.T) {
		var (
			id       = rowIDs(rows[5])[0]
			email, _ = rows[15].GetValue("email")
		)
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
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
		assert.Equal(t, expected, collectRows(ctx, aResult))
	})

	t.Run("Select with order by sort with index asc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "email"},
					Direction: Asc,
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by email ascending
		assert.Equal(t, rowsOrderedByEmail, collectRows(ctx, aResult))
	})

	rowsOrderedByEmailDesc := make([]Row, len(rows))
	copy(rowsOrderedByEmailDesc, rows)
	sort.Slice(rowsOrderedByEmailDesc, func(i, j int) bool {
		email1, _ := rowsOrderedByEmailDesc[i].GetValue("email")
		email2, _ := rowsOrderedByEmailDesc[j].GetValue("email")
		return email1.Value.(TextPointer).String() > email2.Value.(TextPointer).String()
	})

	t.Run("Select with order by sort with index desc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "email"},
					Direction: Desc,
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by ID descending
		assert.Equal(t, rowsOrderedByEmailDesc, collectRows(ctx, aResult))
	})
}
