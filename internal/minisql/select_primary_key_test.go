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
	pager, dbFile := initTest(t)

	var (
		ctx        = context.Background()
		rows       = gen.RowsWithPrimaryKey(38)
		tablePager = pager.ForTable(testColumns[0:2])
		txManager  = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager    = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table      *Table
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

	checkRows(ctx, t, table, rows)

	t.Run("Select all rows", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, rows, collectRows(ctx, result))
	})

	t.Run("Select single row by primary key - index scan", func(t *testing.T) {
		id := rowIDs(rows[5])[0]
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)
		assert.Len(t, result.RowViewFieldIndexes, len(table.Columns))

		actual := collectRows(ctx, result)
		assert.Len(t, actual, 1)
		assert.Equal(t, rows[5], actual[0])
	})

	t.Run("Select multiple rows by primary keys - index scan", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)
		assert.Len(t, result.RowViewFieldIndexes, len(table.Columns))

		// We expect rows 5, 11, 12, and 33
		expected := make([]Row, 0, len(ids))
		for i, row := range rows {
			if i != 5 && i != 11 && i != 12 && i != 33 {
				continue
			}
			expected = append(expected, row)
		}
		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select rows where primary key is NOT IN - sequential scan", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows other than 5, 11, 12, and 33
		expected := make([]Row, 0, len(ids))
		for i, row := range rows {
			if i == 5 || i == 11 || i == 12 || i == 33 {
				continue
			}
			expected = append(expected, row)
		}
		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select rows by range with lower bound - range scan", func(t *testing.T) {
		id := rowIDs(rows[10])[0]
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows 12 and onwards
		expected := make([]Row, 0, len(rows)-11)
		for i, row := range rows {
			if i < 11 {
				continue
			}
			expected = append(expected, row)
		}
		actual := collectRows(ctx, result)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select rows by range with upper bound - range scan", func(t *testing.T) {
		id := rowIDs(rows[30])[0]
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect rows until 30
		expected := make([]Row, 0, len(rows)-9)
		for i, row := range rows {
			if i >= 30 {
				continue
			}
			expected = append(expected, row)
		}
		actual := collectRows(ctx, result)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select rows by range with both lower and upper bound - range scan", func(t *testing.T) {
		id1 := rowIDs(rows[10])[0]
		id2 := rowIDs(rows[30])[0]
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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
							Value: Field{Name: "id"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows between 10 and 30 inclusive
		expected := make([]Row, 0, len(rows)-9)
		for i, row := range rows {
			if i < 10 || i > 30 {
				continue
			}
			expected = append(expected, row)
		}
		actual := collectRows(ctx, result)
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
			Fields: fieldsFromColumns(table.Columns...),
			Conditions: OneOrMore{
				{
					{
						Operand1: Operand{
							Type:  OperandField,
							Value: Field{Name: "id"},
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
							Value: Field{Name: "email"},
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

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		expected := []Row{rows[5].Clone(), rows[15].Clone()}
		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select with order by sort with index asc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "id"},
					Direction: Asc,
				},
			},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by ID ascending
		expected := make([]Row, 0, len(rows))
		expected = append(expected, rows...)
		sort.Slice(expected, func(i, j int) bool {
			id1, _ := expected[i].GetValue("id")
			id2, _ := expected[j].GetValue("id")
			return id1.Value.(int64) < id2.Value.(int64)
		})
		assert.Equal(t, expected, collectRows(ctx, result))
	})

	t.Run("Select with order by sort with index desc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(table.Columns...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "id"},
					Direction: Desc,
				},
			},
		}

		result, err := table.Select(ctx, stmt)
		require.NoError(t, err)

		// We expect all rows sorted by ID descending
		expected := make([]Row, 0, len(rows))
		expected = append(expected, rows...)
		sort.Slice(expected, func(i, j int) bool {
			id1, _ := expected[i].GetValue("id")
			id2, _ := expected[j].GetValue("id")
			return id1.Value.(int64) > id2.Value.(int64)
		})
		assert.Equal(t, expected, collectRows(ctx, result))
	})
}
