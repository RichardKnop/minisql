package minisql

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Select_PrimaryKey(t *testing.T) {
	aPager, dbFile := initTest(t)

	var (
		ctx        = context.Background()
		rows       = gen.RowsWithPrimaryKey(38)
		tablePager = aPager.ForTable(testColumns[0:2])
		txManager  = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager    = NewTransactionalPager(tablePager, txManager, testTableName, "")
		aTable     *Table
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

	t.Run("Select single row by primary key - index scan", func(t *testing.T) {
		id := rowIDs(rows[5])[0]
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
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		actual := collectRows(ctx, aResult)
		assert.Len(t, actual, 1)
		assert.Equal(t, rows[5], actual[0])
	})

	t.Run("Select multiple rows by primary keys - index scan", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
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
						Operator: In,
						Operand2: Operand{
							Type:  OperandList,
							Value: ids,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows 5, 11, 12, and 33
		expected := make([]Row, 0, len(ids))
		for i, aRow := range rows {
			if i != 5 && i != 11 && i != 12 && i != 33 {
				continue
			}
			expected = append(expected, aRow)
		}
		assert.Equal(t, expected, collectRows(ctx, aResult))
	})

	t.Run("Select rows where primary key is NOT IN - sequential scan", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
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
						Operator: NotIn,
						Operand2: Operand{
							Type:  OperandList,
							Value: ids,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows other than 5, 11, 12, and 33
		expected := make([]Row, 0, len(ids))
		for i, aRow := range rows {
			if i == 5 || i == 11 || i == 12 || i == 33 {
				continue
			}
			expected = append(expected, aRow)
		}
		assert.Equal(t, expected, collectRows(ctx, aResult))
	})

	t.Run("Select rows by range with lower bound - range scan", func(t *testing.T) {
		id := rowIDs(rows[10])[0]
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
						Operator: Gt,
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

		// We expect rows 12 and onwards
		expected := make([]Row, 0, len(rows)-11)
		for i, aRow := range rows {
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
		id := rowIDs(rows[30])[0]
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
						Operator: Lt,
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

		// We expect rows until 30
		expected := make([]Row, 0, len(rows)-9)
		for i, aRow := range rows {
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
		id1 := rowIDs(rows[10])[0]
		id2 := rowIDs(rows[30])[0]
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
						Operator: Gte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: id1,
						},
					},
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: "id",
						},
						Operator: Lte,
						Operand2: Operand{
							Type:  OperandInteger,
							Value: id2,
						},
					},
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows between 10 and 30 inclusive
		expected := make([]Row, 0, len(rows)-9)
		for i, aRow := range rows {
			if i < 10 || i > 30 {
				continue
			}
			expected = append(expected, aRow)
		}
		actual := collectRows(ctx, aResult)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select multiple rows by primary key and other column - sequential scan", func(t *testing.T) {
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
					Field:     Field{Name: "id"},
					Direction: Asc,
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by ID ascending
		expected := make([]Row, 0, len(rows))
		for _, aRow := range rows {
			expected = append(expected, aRow)
		}
		sort.Slice(expected, func(i, j int) bool {
			id1, _ := expected[i].GetValue("id")
			id2, _ := expected[j].GetValue("id")
			return id1.Value.(int64) < id2.Value.(int64)
		})
		assert.Equal(t, expected, collectRows(ctx, aResult))
	})

	t.Run("Select with order by sort with index desc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(aTable.Columns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "id"},
					Direction: Desc,
				},
			},
		}

		aResult, err := aTable.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by ID descending
		expected := make([]Row, 0, len(rows))
		for _, aRow := range rows {
			expected = append(expected, aRow)
		}
		sort.Slice(expected, func(i, j int) bool {
			id1, _ := expected[i].GetValue("id")
			id2, _ := expected[j].GetValue("id")
			return id1.Value.(int64) > id2.Value.(int64)
		})
		assert.Equal(t, expected, collectRows(ctx, aResult))
	})
}
