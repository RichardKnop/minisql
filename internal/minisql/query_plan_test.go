package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_PlanQuery_MultipleIndexes(t *testing.T) {
	t.Parallel()

	var (
		columns = []Column{
			{
				Kind:          Int8,
				Size:          8,
				Name:          "id",
				PrimaryKey:    true,
				Autoincrement: true,
			},
			{
				Kind:   Varchar,
				Size:   MaxInlineVarchar,
				Name:   "email",
				Unique: true,
			},
			{
				Kind:     Text,
				Name:     "Name",
				Nullable: true,
			},
			{
				Kind:            Timestamp,
				Name:            "Created",
				DefaultValueNow: true,
			},
		}
		pkIndexName     = "pkey__users"
		uniqueIndexName = "key__users__email"

		aTable = &Table{
			PrimaryKey: PrimaryKey{
				IndexInfo: IndexInfo{
					Name:   pkIndexName,
					Column: columns[0],
				},
			},
			UniqueIndexes: map[string]UniqueIndex{
				uniqueIndexName: {
					IndexInfo: IndexInfo{
						Name:   uniqueIndexName,
						Column: columns[1],
					},
				},
			},
			Columns: columns,
		}
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
						Type: ScanTypeSequential,
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
						FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
							},
						},
					},
				},
			},
		},
		{
			"Two index point scans for both primary and unique indexes",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("id", OperandInteger, int64(42)),
					},
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       pkIndexName,
						IndexColumnName: "id",
						IndexKeys:       []any{int64(42)},
						Filters:         OneOrMore{{}},
					},
					{
						Type:            ScanTypeIndexPoint,
						IndexName:       uniqueIndexName,
						IndexColumnName: "email",
						IndexKeys:       []any{"foo@example.com"},
						Filters:         OneOrMore{{}},
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
				FieldIsEqual("id", OperandInteger, int64(10)),
			},
			Scan{},
			false,
		},
		{
			"Not equal operator",
			Conditions{
				FieldIsNotEqual("id", OperandInteger, int64(42)),
			},
			Scan{},
			false,
		},
		{
			"Range scan with lower bound only",
			Conditions{
				FieldIsGreater("id", OperandInteger, int64(10)),
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
				FieldIsGreaterOrEqual("id", OperandInteger, int64(10)),
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
				FieldIsLess("id", OperandInteger, int64(10)),
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
				FieldIsLessOrEqual("id", OperandInteger, int64(10)),
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
				FieldIsLessOrEqual("id", OperandInteger, int64(10)),
				FieldIsGreater("id", OperandInteger, int64(5)),
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
				FieldIsLessOrEqual("id", OperandInteger, int64(10)),
				FieldIsGreater("id", OperandInteger, int64(5)),
				FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("foo"))),
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
					FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("foo"))),
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
