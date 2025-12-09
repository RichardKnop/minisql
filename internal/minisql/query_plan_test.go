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
				ScanType: ScanTypeSequential,
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
				ScanType: ScanTypeSequential,
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
		{
			"Single primary key equality condition",
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
				ScanType:        ScanTypeIndexPoint,
				IndexName:       "pk_users",
				IndexColumnName: "id",
				IndexKeyGroups: [][]any{
					{int64(42)},
				},
				Filters: OneOrMore{{}},
				KeyFiltersMap: map[any]int{
					int64(42): 0,
				},
			},
		},
		{
			"Multiple primary key equality conditions",
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
				ScanType:        ScanTypeIndexPoint,
				IndexName:       "pk_users",
				IndexColumnName: "id",
				IndexKeyGroups: [][]any{
					{int64(42)},
					{int64(69)},
				},
				Filters: OneOrMore{{}, {}},
				KeyFiltersMap: map[any]int{
					int64(42): 0,
					int64(69): 1,
				},
			},
		},
		{
			"Multiple primary key equality conditions with extra remaining filters for both groups",
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
				ScanType:        ScanTypeIndexPoint,
				IndexName:       "pk_users",
				IndexColumnName: "id",
				IndexKeyGroups: [][]any{
					{int64(42)},
					{int64(69)},
				},
				Filters: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: "foo@example.com"},
						},
					},
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: "bar@example.com"},
						},
					},
				},
				KeyFiltersMap: map[any]int{
					int64(42): 0,
					int64(69): 1,
				},
			},
		},
		{
			"Multiple primary key equality conditions with extra remaining filters for only one group",
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
				ScanType:        ScanTypeIndexPoint,
				IndexName:       "pk_users",
				IndexColumnName: "id",
				IndexKeyGroups: [][]any{
					{int64(42)},
					{int64(69)},
				},
				Filters: OneOrMore{
					{
						{
							Operand1: Operand{Type: OperandField, Value: "email"},
							Operator: Eq,
							Operand2: Operand{Type: OperandQuotedString, Value: "foo@example.com"},
						},
					},
					{},
				},
				KeyFiltersMap: map[any]int{
					int64(42): 0,
					int64(69): 1,
				},
			},
		},
		{
			"Single primary key IN condition",
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
				ScanType:        ScanTypeIndexPoint,
				IndexName:       "pk_users",
				IndexColumnName: "id",
				IndexKeyGroups: [][]any{
					{int64(42), int64(69)},
				},
				Filters: OneOrMore{{}},
				KeyFiltersMap: map[any]int{
					int64(42): 0,
					int64(69): 0,
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
