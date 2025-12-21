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
		aPager.ForIndex(aTable.PrimaryKey.Column.Kind, uint64(aTable.PrimaryKey.Column.Size), true),
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

	t.Run("Select single row by primary key - index scan", func(t *testing.T) {
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

	t.Run("Select multiple rows by primary keys - index scan", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
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
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})

	t.Run("Select rows where primary key is NOT INT - range scan", func(t *testing.T) {
		ids := rowIDs(rows[5], rows[11], rows[12], rows[33])
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
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})

	t.Run("Select rows by range with lower bound - range scan", func(t *testing.T) {
		id := rowIDs(rows[10])[0]
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
		actual := aResult.CollectRows(ctx)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select rows by range with upper bound - range scan", func(t *testing.T) {
		id := rowIDs(rows[30])[0]
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
		actual := aResult.CollectRows(ctx)
		assert.Len(t, actual, len(expected))
		assert.Equal(t, expected, actual)
	})

	t.Run("Select rows by range with both lower and upper bound - range scan", func(t *testing.T) {
		id1 := rowIDs(rows[10])[0]
		id2 := rowIDs(rows[30])[0]
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
		actual := aResult.CollectRows(ctx)
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

	t.Run("Select with order by sort with index asc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumnsWithPrimaryKey...),
			OrderBy: []OrderBy{
				{
					Field:     Field{Name: "id"},
					Direction: Asc,
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
			return id1.Value.(int64) < id2.Value.(int64)
		})
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})

	t.Run("Select with order by sort with index desc", func(t *testing.T) {
		stmt := Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(testColumnsWithPrimaryKey...),
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
		assert.Equal(t, expected, aResult.CollectRows(ctx))
	})
}
