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
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: "foo@example.com"},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{
							{
								{
									Operand1: Operand{Type: OperandField, Value: "email"},
									Operator: Eq,
									Operand2: Operand{Type: OperandQuotedString, Value: "foo@example.com"},
								},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(69)},
						},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: "foo@example.com"},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(69)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: "bar@example.com"},
						},
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
								{
									Operand1: Operand{Type: OperandField, Value: "email"},
									Operator: Eq,
									Operand2: Operand{Type: OperandQuotedString, Value: "foo@example.com"},
								},
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
								{
									Operand1: Operand{Type: OperandField, Value: "email"},
									Operator: Eq,
									Operand2: Operand{Type: OperandQuotedString, Value: "bar@example.com"},
								},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: "foo@example.com"},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(69)},
						},
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
								{
									Operand1: Operand{Type: OperandField, Value: "email"},
									Operator: Eq,
									Operand2: Operand{Type: OperandQuotedString, Value: "foo@example.com"},
								},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: In,
							Operand2: Operand{Type: OperandList, Value: []any{int64(42), int64(69)}},
						},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: NotIn,
							Operand2: Operand{Type: OperandList, Value: []any{int64(42), int64(69)}},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{{
							{
								Operand1: Operand{Type: OperandField, Value: "id"},
								Operator: NotIn,
								Operand2: Operand{Type: OperandList, Value: []any{int64(42), int64(69)}},
							},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Ne,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{
							{
								{
									Operand1: Operand{Type: OperandField, Value: "id"},
									Operator: Ne,
									Operand2: Operand{Type: OperandInteger, Value: int64(42)},
								},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: In,
							Operand2: Operand{Type: OperandList, Value: []any{int64(42), int64(69)}},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "verified"},
							Operator: Eq,
							Operand2: Operand{Type: OperandBoolean, Value: true},
						},
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
							{
								Operand1: Operand{Type: OperandField, Value: "verified"},
								Operator: Eq,
								Operand2: Operand{Type: OperandBoolean, Value: true},
							},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Gt,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Lte,
							Operand2: Operand{Type: OperandInteger, Value: int64(69)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
						},
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
							{
								Operand1: Operand{Type: OperandField, Value: "email"},
								Operator: Eq,
								Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
							},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Gte,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Lt,
							Operand2: Operand{Type: OperandInteger, Value: int64(27)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
						},
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
							{
								Operand1: Operand{Type: OperandField, Value: "email"},
								Operator: Eq,
								Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
							},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Gt,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Lte,
							Operand2: Operand{Type: OperandInteger, Value: int64(69)},
						},
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

func TestTable_PlanQuery_UniqueIndex(t *testing.T) {
	t.Parallel()

	indexName := "key__users__email"
	aTable := &Table{
		UniqueIndexes: map[string]UniqueIndex{
			indexName: {
				IndexInfo: IndexInfo{
					Name:   indexName,
					Column: testColumnsWithUniqueIndex[1],
				},
			},
		},
		Columns: testColumnsWithUniqueIndex,
	}

	testCases := []struct {
		Name     string
		Stmt     Statement
		Expected QueryPlan
	}{
		{
			"Sequential scan on table with unique index",
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
			"Sequential scan on table with unique key and filters",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{
							{
								{
									Operand1: Operand{Type: OperandField, Value: "id"},
									Operator: Eq,
									Operand2: Operand{Type: OperandInteger, Value: int64(42)},
								},
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
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"foo@example.com"},
						Filters:         OneOrMore{{}},
					},
				},
			},
		},
		{
			"Multiple unique index key equality conditions - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("bar@example.com"))},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"foo@example.com"},
						Filters:         OneOrMore{{}},
					},
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"bar@example.com"},
						Filters:         OneOrMore{{}},
					},
				},
			},
		},
		{
			"Multiple unique index key equality conditions with extra remaining filters for both groups - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(69)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("bar@example.com"))},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"foo@example.com"},
						Filters: OneOrMore{
							{
								{
									Operand1: Operand{Type: OperandField, Value: "id"},
									Operator: Eq,
									Operand2: Operand{Type: OperandInteger, Value: int64(42)},
								},
							},
						},
					},
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"bar@example.com"},
						Filters: OneOrMore{
							{
								{
									Operand1: Operand{Type: OperandField, Value: "id"},
									Operator: Eq,
									Operand2: Operand{Type: OperandInteger, Value: int64(69)},
								},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("bar@example.com"))},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"foo@example.com"},
						Filters: OneOrMore{
							{
								{
									Operand1: Operand{Type: OperandField, Value: "id"},
									Operator: Eq,
									Operand2: Operand{Type: OperandInteger, Value: int64(42)},
								},
							},
						},
					},
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"bar@example.com"},
						Filters:         OneOrMore{{}},
					},
				},
			},
		},
		{
			"Multiple unique index keys IN condition - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: In,
							Operand2: Operand{Type: OperandList, Value: []any{
								NewTextPointer([]byte("foo@example.com")),
								NewTextPointer([]byte("bar@example.com")),
							}},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"foo@example.com", "bar@example.com"},
						Filters:         OneOrMore{{}},
					},
				},
			},
		},
		{
			"Multiple unique index keys NOT IN condition - sequential scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: NotIn,
							Operand2: Operand{Type: OperandList, Value: []any{
								NewTextPointer([]byte("foo@example.com")),
								NewTextPointer([]byte("bar@example.com")),
							}},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{{
							{
								Operand1: Operand{Type: OperandField, Value: "email"},
								Operator: NotIn,
								Operand2: Operand{Type: OperandList, Value: []any{
									NewTextPointer([]byte("foo@example.com")),
									NewTextPointer([]byte("bar@example.com")),
								}},
							},
						}},
					},
				},
			},
		},
		{
			"Single unique index key NOT equal condition - sequential scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Ne,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{
							{
								{
									Operand1: Operand{Type: OperandField, Value: "email"},
									Operator: Ne,
									Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
								},
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
						Type: ScanTypeSequential,
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
			"Ordered by primary key descending - index scan",
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
						Type:            ScanTypeIndexAll,
						IndexName:       indexName,
						IndexColumnName: "email",
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
			"Multiple unique index keys IN condition plus order by - index point scan",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: In,
							Operand2: Operand{
								Type: OperandList,
								Value: []any{
									NewTextPointer([]byte("foo@example.com")),
									NewTextPointer([]byte("bar@example.com")),
								},
							},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
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
						Type:            ScanTypeIndexPoint,
						IndexName:       indexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"foo@example.com", "bar@example.com"},
						Filters: OneOrMore{{
							{
								Operand1: Operand{Type: OperandField, Value: "id"},
								Operator: Eq,
								Operand2: Operand{Type: OperandInteger, Value: int64(42)},
							},
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
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Gt,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("foo@example.com"))},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Lte,
							Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("qux@example.com"))},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:            ScanTypeIndexRange,
						IndexName:       indexName,
						IndexColumnName: "email",
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
							{
								Operand1: Operand{Type: OperandField, Value: "id"},
								Operator: Eq,
								Operand2: Operand{Type: OperandInteger, Value: int64(42)},
							},
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
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Gte,
							Operand2: Operand{Type: OperandInteger, Value: NewTextPointer([]byte("foo@example.com"))},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Lt,
							Operand2: Operand{Type: OperandInteger, Value: NewTextPointer([]byte("qux@example.com"))},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "id"},
							Operator: Eq,
							Operand2: Operand{Type: OperandInteger, Value: int64(42)},
						},
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:            ScanTypeIndexRange,
						IndexName:       indexName,
						IndexColumnName: "email",
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     "foo@example.com",
								Inclusive: true,
							},
						},
					},
					{
						Type:            ScanTypeIndexRange,
						IndexName:       indexName,
						IndexColumnName: "email",
						RangeCondition: RangeCondition{
							Upper: &RangeBound{
								Value: "qux@example.com",
							},
						},
						Filters: OneOrMore{{
							{
								Operand1: Operand{Type: OperandField, Value: "id"},
								Operator: Eq,
								Operand2: Operand{Type: OperandInteger, Value: int64(42)},
							},
						}},
					},
				},
			},
		},
		{ // TODO - perhaps we should be ordering by using the index rather than sorting in memory?
			"A range scan with order by unique index key - ordered in memory",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Gt,
							Operand2: Operand{Type: OperandInteger, Value: NewTextPointer([]byte("foo@example.com"))},
						},
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Lte,
							Operand2: Operand{Type: OperandInteger, Value: NewTextPointer([]byte("qux@example.com"))},
						},
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
						Type:            ScanTypeIndexRange,
						IndexName:       indexName,
						IndexColumnName: "email",
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
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			actual, err := aTable.PlanQuery(context.Background(), aTestCase.Stmt)
			require.NoError(t, err)
			assert.Equal(t, aTestCase.Expected, actual)
		})
	}
}

func TestTryRangeScan(t *testing.T) {
	t.Parallel()

	indexName := "pkey__users"
	indexInfo := IndexInfo{
		Name:   indexName,
		Column: testColumns[0],
	}
	testCases := []struct {
		Name         string
		Conditions   Conditions
		ExpectedScan Scan
		ExpectedOK   bool
	}{
		{
			"Equality operator does not qualify for range scan",
			Conditions{
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Eq,
					Operand2: Operand{Type: OperandInteger, Value: int64(10)},
				},
			},
			Scan{},
			false,
		},
		{
			"Not equal operator",
			Conditions{
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Ne,
					Operand2: Operand{Type: OperandInteger, Value: int64(10)},
				},
			},
			Scan{},
			false,
		},
		{
			"Range scan with lower bound only",
			Conditions{
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Gt,
					Operand2: Operand{Type: OperandInteger, Value: int64(10)},
				},
			},
			Scan{
				Type:            ScanTypeIndexRange,
				IndexName:       indexName,
				IndexColumnName: "id",
				RangeCondition: RangeCondition{
					Lower: &RangeBound{
						Value: int64(10),
					},
				},
			},
			true,
		},
		{
			"Range scan with lower bound only (inclusive)",
			Conditions{
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Gte,
					Operand2: Operand{Type: OperandInteger, Value: int64(10)},
				},
			},
			Scan{
				Type:            ScanTypeIndexRange,
				IndexName:       indexName,
				IndexColumnName: "id",
				RangeCondition: RangeCondition{
					Lower: &RangeBound{
						Value:     int64(10),
						Inclusive: true,
					},
				},
			},
			true,
		},
		{
			"Range scan with upper bound only",
			Conditions{
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Lt,
					Operand2: Operand{Type: OperandInteger, Value: int64(10)},
				},
			},
			Scan{
				Type:            ScanTypeIndexRange,
				IndexName:       indexName,
				IndexColumnName: "id",
				RangeCondition: RangeCondition{
					Upper: &RangeBound{
						Value: int64(10),
					},
				},
			},
			true,
		},
		{
			"Range scan with upper bound only (inclusive)",
			Conditions{
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Lte,
					Operand2: Operand{Type: OperandInteger, Value: int64(10)},
				},
			},
			Scan{
				Type:            ScanTypeIndexRange,
				IndexName:       indexName,
				IndexColumnName: "id",
				RangeCondition: RangeCondition{
					Upper: &RangeBound{
						Value:     int64(10),
						Inclusive: true,
					},
				},
			},
			true,
		},
		{
			"Range scan with with both lower and upper bounds",
			Conditions{
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Lte,
					Operand2: Operand{Type: OperandInteger, Value: int64(10)},
				},
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Gt,
					Operand2: Operand{Type: OperandInteger, Value: int64(5)},
				},
			},
			Scan{
				Type:            ScanTypeIndexRange,
				IndexName:       indexName,
				IndexColumnName: "id",
				RangeCondition: RangeCondition{
					Lower: &RangeBound{
						Value: int64(5),
					},
					Upper: &RangeBound{
						Value:     int64(10),
						Inclusive: true,
					},
				},
			},
			true,
		},
		{
			"Range scan with with both lower and upper bounds and remaining filters",
			Conditions{
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Lte,
					Operand2: Operand{Type: OperandInteger, Value: int64(10)},
				},
				{
					Operand1: Operand{Type: OperandField, Value: "id"},
					Operator: Gt,
					Operand2: Operand{Type: OperandInteger, Value: int64(5)},
				},
				{
					Operand1: Operand{Type: OperandField, Value: "name"},
					Operator: Eq,
					Operand2: Operand{Type: OperandQuotedString, Value: "foo"},
				},
			},
			Scan{
				Type:            ScanTypeIndexRange,
				IndexName:       indexName,
				IndexColumnName: "id",
				RangeCondition: RangeCondition{
					Lower: &RangeBound{
						Value: int64(5),
					},
					Upper: &RangeBound{
						Value:     int64(10),
						Inclusive: true,
					},
				},
				Filters: OneOrMore{{
					{
						Operand1: Operand{Type: OperandField, Value: "name"},
						Operator: Eq,
						Operand2: Operand{Type: OperandQuotedString, Value: "foo"},
					},
				}},
			},
			true,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aScan, ok := tryRangeScan(indexInfo, aTestCase.Conditions)
			assert.Equal(t, aTestCase.ExpectedOK, ok)
			if ok {
				assert.Equal(t, aTestCase.ExpectedScan, aScan)
			}
		})
	}
}
