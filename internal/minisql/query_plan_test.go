package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
