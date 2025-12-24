package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_PlanQuery_PrimaryKey(t *testing.T) {
	t.Parallel()

	indexName := "pkey__users"
	aTable := &Table{
		PrimaryKey: PrimaryKey{
			IndexInfo: IndexInfo{
				Name:   indexName,
				Column: testColumnsWithPrimaryKey[0],
			},
		},
		Columns: testColumnsWithPrimaryKey,
	}

	testCases := []struct {
		Name     string
		Stmt     Statement
		Expected QueryPlan
	}{
		{
			"Sequential scan on table with primary key",
			Statement{
				Kind: Select,
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
					},
				},
			},
		},
		{
			"Sequential scan on table with primary key and filters",
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
						Type: ScanTypeSequential,
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
			"Single unique index key equality condition - index point scan",
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
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42)},
						Filters:         OneOrMore{{}},
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
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42)},
						Filters:         OneOrMore{{}},
					},
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(69)},
						Filters:         OneOrMore{{}},
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
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42)},
						Filters: OneOrMore{
							{
								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
							},
						},
					},
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(69)},
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
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42)},
						Filters: OneOrMore{
							{
								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
							},
						},
					},
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(69)},
						Filters:         OneOrMore{{}},
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
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42), int64(69)},
						Filters:         OneOrMore{{}},
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
						Type: ScanTypeSequential,
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
						Type: ScanTypeSequential,
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
						Type: ScanTypeSequential,
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
						Type:            ScanTypeIndexAll,
						IndexName:       indexName,
						IndexColumnName: "id",
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
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42), int64(69)},
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
						Type:            ScanTypeIndexRange,
						IndexName:       indexName,
						IndexColumnName: "id",
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
						Type:            ScanTypeIndexRange,
						IndexName:       indexName,
						IndexColumnName: "id",
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     int64(42),
								Inclusive: true,
							},
						},
					},
					{
						Type:            ScanTypeIndexRange,
						IndexName:       indexName,
						IndexColumnName: "id",
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
		{ // TODO - perhaps we should be ordering by using the index rather than sorting in memory?
			"A range scan with order by primary key - ordered in memory",
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
						Type:            ScanTypeIndexRange,
						IndexName:       indexName,
						IndexColumnName: "id",
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
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			actual, err := aTable.PlanQuery(context.Background(), aTestCase.Stmt)
			require.NoError(t, err)
			assert.Equal(t, aTestCase.Expected, actual)
		})
	}
}
