package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTable_PlanQuery(t *testing.T) {
	t.Parallel()

	aTableWithPK := &Table{
		PrimaryKey: PrimaryKey{
			Name:   "pk_users",
			Column: testColumnsWithPrimaryKey[0],
		},
		Columns: testColumnsWithPrimaryKey,
	}

	testCases := []struct {
		Name     string
		Table    *Table
		Stmt     Statement
		Expected QueryPlan
	}{
		{
			"Sequential scan on table with primary key",
			aTableWithPK,
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
			aTableWithPK,
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
			"Single primary key equality condition (index point scan)",
			aTableWithPK,
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
						IndexName:       "pk_users",
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42)},
						Filters:         OneOrMore{{}},
					},
				},
			},
		},
		{
			"Multiple primary key equality conditions (index point scan)",
			aTableWithPK,
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
						IndexName:       "pk_users",
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42)},
						Filters:         OneOrMore{{}},
					},
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       "pk_users",
						IndexColumnName: "id",
						IndexKeys:       []any{int64(69)},
						Filters:         OneOrMore{{}},
					},
				},
			},
		},
		{
			"Multiple primary key equality conditions with extra remaining filters for both groups (index point scan)",
			aTableWithPK,
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
						IndexName:       "pk_users",
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
						IndexName:       "pk_users",
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
			"Multiple primary key equality conditions with extra remaining filters for only one group (index point scan)",
			aTableWithPK,
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
						IndexName:       "pk_users",
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
						IndexName:       "pk_users",
						IndexColumnName: "id",
						IndexKeys:       []any{int64(69)},
						Filters:         OneOrMore{{}},
					},
				},
			},
		},
		{
			"Multiple primary keys IN condition (index point scan)",
			aTableWithPK,
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
						IndexName:       "pk_users",
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42), int64(69)},
						Filters:         OneOrMore{{}},
					},
				},
			},
		},
		{
			"Order in memory (sequential scan)",
			aTableWithPK,
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
			"Ordered by primary key descending (index scan)",
			aTableWithPK,
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
						Type:            ScanTypeIndexRange,
						IndexName:       "pk_users",
						IndexColumnName: "id",
					},
				},
				UseIndexForOrder: true,
				SortInMemory:     false,
				SortReverse:      true,
				OrderBy: []OrderBy{
					{
						Field:     Field{Name: "id"},
						Direction: Desc,
					},
				},
			},
		},
		{
			"Multiple primary keys IN condition plus order by (index point scan)",
			aTableWithPK,
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
						IndexName:       "pk_users",
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
				UseIndexForOrder: false,
				SortInMemory:     true,
				SortReverse:      true,
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
			actual := aTableWithPK.PlanQuery(context.Background(), aTestCase.Stmt)
			assert.Equal(t, aTestCase.Expected, actual)
		})
	}
}
