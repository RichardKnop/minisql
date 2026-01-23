package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_PlanQuery_SingleUniqueIndex(t *testing.T) {
	t.Parallel()

	var (
		indexName = "key__test_table__email"
		aTable    = NewTable(zap.NewNop(), nil, nil, testTableName, testColumns[0:2], 0, nil, WithUniqueIndex(
			UniqueIndex{
				IndexInfo: IndexInfo{
					Name:    indexName,
					Columns: testColumns[1:2],
				},
			},
		))
	)

	testCases := []struct {
		Name     string
		Stmt     Statement
		Expected QueryPlan
	}{
		{
			"Sequential scan",
			Statement{
				Kind: Select,
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: testTableName,
						Type:      ScanTypeSequential,
					},
				},
			},
		},
		{
			"Sequential scan with filters",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("id", OperandInteger, int64(42)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: testTableName,
						Type:      ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsEqual("id", OperandInteger, int64(42)),
							},
						},
					},
				},
			},
		},
		{
			"Single index key equality condition but NULL - sequential scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsNull("email"),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: testTableName,
						Type:      ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsNull("email"),
							},
						},
					},
				},
			},
		},
		{
			"Single index key equality condition - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"foo@example.com"},
					},
				},
			},
		},
		{
			"Multiple index key equality conditions - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("bar@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"foo@example.com"},
					},
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"bar@example.com"},
					},
				},
			},
		},
		{
			"Multiple index key equality conditions with extra remaining filters for both groups - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("id", OperandInteger, int64(42)),
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsEqual("id", OperandInteger, int64(69)),
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("bar@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"foo@example.com"},
						Filters: OneOrMore{
							{
								FieldIsEqual("id", OperandInteger, int64(42)),
							},
						},
					},
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"bar@example.com"},
						Filters: OneOrMore{
							{
								FieldIsEqual("id", OperandInteger, int64(69)),
							},
						},
					},
				},
			},
		},
		{
			"Multiple index key equality conditions with extra remaining filters for only one group - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("id", OperandInteger, int64(42)),
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("bar@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"foo@example.com"},
						Filters: OneOrMore{
							{
								FieldIsEqual("id", OperandInteger, int64(42)),
							},
						},
					},
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"bar@example.com"},
					},
				},
			},
		},
		{
			"Multiple index keys IN condition - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsInAny("email", NewTextPointer([]byte("foo@example.com")), NewTextPointer([]byte("bar@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"foo@example.com", "bar@example.com"},
					},
				},
			},
		},
		{
			"Multiple index keys NOT IN condition - sequential scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsNotInAny("email", NewTextPointer([]byte("foo@example.com")), NewTextPointer([]byte("bar@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: testTableName,
						Type:      ScanTypeSequential,
						Filters: OneOrMore{{
							FieldIsNotInAny("email", NewTextPointer([]byte("foo@example.com")), NewTextPointer([]byte("bar@example.com"))),
						}},
					},
				},
			},
		},
		{
			"Single index key NOT equal condition - sequential scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsNotEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: testTableName,
						Type:      ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsNotEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
							},
						},
					},
				},
			},
		},
		{
			"Order in memory - sequential scan",
			Statement{
				Kind: Select,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "id"},
						Direction: Desc,
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: testTableName,
						Type:      ScanTypeSequential,
					},
				},
				SortInMemory: true,
				SortReverse:  true,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "id"},
						Direction: Desc,
					},
				},
			},
		},
		{
			"Ordered by index key descending - index scan",
			Statement{
				Kind: Select,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "email"},
						Direction: Desc,
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexAll,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
					},
				},
				SortReverse: true,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "email"},
						Direction: Desc,
					},
				},
			},
		},
		{
			"Multiple index keys IN condition plus order by - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsInAny("email", NewTextPointer([]byte("foo@example.com")), NewTextPointer([]byte("bar@example.com"))),
						FieldIsEqual("id", OperandInteger, int64(42)),
					},
				},
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "email"},
						Direction: Desc,
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						IndexKeys:    []any{"foo@example.com", "bar@example.com"},
						Filters: OneOrMore{{
							FieldIsEqual("id", OperandInteger, int64(42)),
						}},
					},
				},
				SortInMemory: true,
				SortReverse:  true,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "email"},
						Direction: Desc,
					},
				},
			},
		},
		{
			"A single range scan with remaining filters",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("id", OperandInteger, int64(42)),
						FieldIsGreater("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
						FieldIsLessOrEqual("email", OperandQuotedString, NewTextPointer([]byte("qux@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexRange,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     "foo@example.com",
								Inclusive: false,
							},
							Upper: &RangeBound{
								Value:     "qux@example.com",
								Inclusive: true,
							},
						},
						Filters: OneOrMore{{
							FieldIsEqual("id", OperandInteger, int64(42)),
						}},
					},
				},
			},
		},
		{
			"Multiple range scans",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsGreaterOrEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsLess("email", OperandQuotedString, NewTextPointer([]byte("qux@example.com"))),
						FieldIsEqual("id", OperandInteger, int64(42)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexRange,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     "foo@example.com",
								Inclusive: true,
							},
						},
					},
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexRange,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						RangeCondition: RangeCondition{
							Upper: &RangeBound{
								Value: "qux@example.com",
							},
						},
						Filters: OneOrMore{{
							FieldIsEqual("id", OperandInteger, int64(42)),
						}},
					},
				},
			},
		},
		{
			"A range scan with order by index key - order via index",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsGreater("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
						FieldIsLessOrEqual("email", OperandQuotedString, NewTextPointer([]byte("qux@example.com"))),
					},
				},
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "email"},
						Direction: Desc,
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    testTableName,
						Type:         ScanTypeIndexRange,
						IndexName:    indexName,
						IndexColumns: testColumns[1:2],
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     "foo@example.com",
								Inclusive: false,
							},
							Upper: &RangeBound{
								Value:     "qux@example.com",
								Inclusive: true,
							},
						},
					},
				},
				SortInMemory: false,
				SortReverse:  true,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "email"},
						Direction: Desc,
					},
				},
			},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {

			actual, err := aTable.PlanQuery(context.Background(), aTestCase.Stmt)
			require.NoError(t, err)
			assert.Equal(t, aTestCase.Expected, actual)
		})
	}
}
