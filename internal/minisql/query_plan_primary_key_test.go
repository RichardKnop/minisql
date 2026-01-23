package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_PlanQuery_PrimaryKey(t *testing.T) {
	t.Parallel()

	var (
		indexName = "pkey__users"
		aTable    = NewTable(zap.NewNop(), nil, nil, "users", testColumns[0:2], 0, nil, WithPrimaryKey(
			NewPrimaryKey(indexName, testColumns[0:1], true),
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
						TableName: "users",
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
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: "users",
						Type:      ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
							},
						},
					},
				},
			},
		},
		{
			"Single primary key equality condition but NULL - sequential scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsNull("id"),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: "users",
						Type:      ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsNull("id"),
							},
						},
					},
				},
			},
		},
		{
			"Single primary key equality condition - index point scan",
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
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(42)},
					},
				},
			},
		},
		{
			"Multiple primary key equality conditions - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("id", OperandInteger, int64(42)),
					},
					{
						FieldIsEqual("id", OperandInteger, int64(69)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(42)},
					},
					{
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(69)},
					},
				},
			},
		},
		{
			"Multiple primary key equality conditions with extra remaining filters for both groups - index point scan",
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
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(42)},
						Filters: OneOrMore{
							{
								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
							},
						},
					},
					{
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(69)},
						Filters: OneOrMore{
							{
								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("bar@example.com"))),
							},
						},
					},
				},
			},
		},
		{
			"Multiple primary key equality conditions with extra remaining filters for only one group - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("id", OperandInteger, int64(42)),
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsEqual("id", OperandInteger, int64(69)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(42)},
						Filters: OneOrMore{
							{
								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
							},
						},
					},
					{
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(69)},
					},
				},
			},
		},
		{
			"Multiple primary keys IN condition - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsInAny("id", int64(42), int64(69)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(42), int64(69)},
					},
				},
			},
		},
		{
			"Multiple primary keys NOT IN condition - sequential scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsNotInAny("id", int64(42), int64(69)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: "users",
						Type:      ScanTypeSequential,
						Filters: OneOrMore{{
							FieldIsNotInAny("id", int64(42), int64(69)),
						}},
					},
				},
			},
		},
		{
			"Single primary key NOT equal condition - sequential scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsNotEqual("id", OperandInteger, int64(42)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: "users",
						Type:      ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsNotEqual("id", OperandInteger, int64(42)),
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
						Field:     Field{Name: "email"},
						Direction: Desc,
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: "users",
						Type:      ScanTypeSequential,
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
			"Ordered by primary key descending - index scan",
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
						TableName:    "users",
						Type:         ScanTypeIndexAll,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
					},
				},
				SortReverse: true,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "id"},
						Direction: Desc,
					},
				},
			},
		},
		{
			"Multiple primary keys IN condition plus order by - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsInAny("id", int64(42), int64(69)),
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
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
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						IndexKeys:    []any{int64(42), int64(69)},
						Filters: OneOrMore{{
							FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
						}},
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
			"A single range scan with remaining filters",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsGreater("id", OperandInteger, int64(42)),
						FieldIsLessOrEqual("id", OperandInteger, int64(69)),
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    "users",
						Type:         ScanTypeIndexRange,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     int64(42),
								Inclusive: false,
							},
							Upper: &RangeBound{
								Value:     int64(69),
								Inclusive: true,
							},
						},
						Filters: OneOrMore{{
							FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
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
						FieldIsGreaterOrEqual("id", OperandInteger, int64(42)),
					},
					{
						FieldIsLess("id", OperandInteger, int64(27)),
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    "users",
						Type:         ScanTypeIndexRange,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     int64(42),
								Inclusive: true,
							},
						},
					},
					{
						TableName:    "users",
						Type:         ScanTypeIndexRange,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						RangeCondition: RangeCondition{
							Upper: &RangeBound{
								Value: int64(27),
							},
						},
						Filters: OneOrMore{{
							FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
						}},
					},
				},
			},
		},
		{
			"A range scan with order by primary key - order via index",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsGreater("id", OperandInteger, int64(42)),
						FieldIsLessOrEqual("id", OperandInteger, int64(69)),
					},
				},
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
						TableName:    "users",
						Type:         ScanTypeIndexRange,
						IndexName:    indexName,
						IndexColumns: testColumns[0:1],
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     int64(42),
								Inclusive: false,
							},
							Upper: &RangeBound{
								Value:     int64(69),
								Inclusive: true,
							},
						},
					},
				},
				SortInMemory: false,
				SortReverse:  true,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "id"},
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
