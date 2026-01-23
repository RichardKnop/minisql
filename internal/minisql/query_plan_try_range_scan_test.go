package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryRangeScan(t *testing.T) {
	t.Parallel()

	indexName := "pkey__users"
	indexInfo := IndexInfo{
		Name:    indexName,
		Columns: testColumns[0:1],
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
				TableName:    "users",
				Type:         ScanTypeIndexRange,
				IndexName:    indexName,
				IndexColumns: testColumns[0:1],
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
				TableName:    "users",
				Type:         ScanTypeIndexRange,
				IndexName:    indexName,
				IndexColumns: testColumns[0:1],
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
				TableName:    "users",
				Type:         ScanTypeIndexRange,
				IndexName:    indexName,
				IndexColumns: testColumns[0:1],
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
				TableName:    "users",
				Type:         ScanTypeIndexRange,
				IndexName:    indexName,
				IndexColumns: testColumns[0:1],
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
				TableName:    "users",
				Type:         ScanTypeIndexRange,
				IndexName:    indexName,
				IndexColumns: testColumns[0:1],
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
				TableName:    "users",
				Type:         ScanTypeIndexRange,
				IndexName:    indexName,
				IndexColumns: testColumns[0:1],
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
			aScan, ok, err := tryRangeScan("users", indexInfo, aTestCase.Conditions, nil)
			require.NoError(t, err)
			assert.Equal(t, aTestCase.ExpectedOK, ok)
			if ok {
				assert.Equal(t, aTestCase.ExpectedScan, aScan)
			}
		})
	}
}

// TestTryRangeScanWithStats demonstrates how statistics influence
// the query planner's decision to use index vs table scan for range queries
func TestTryRangeScan_WithStats(t *testing.T) {
	t.Parallel()

	indexInfo := IndexInfo{
		Name:    "idx_age",
		Columns: []Column{{Name: "age", Kind: Int4, Size: 4}},
	}

	t.Run("without stats - uses range scan", func(t *testing.T) {
		t.Parallel()

		// Build range conditions: age >= 25 AND age <= 50
		filters := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
				Operator: Gte,
				Operand2: Operand{Type: OperandInteger, Value: int32(25)},
			},
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
				Operator: Lte,
				Operand2: Operand{Type: OperandInteger, Value: int32(50)},
			},
		}

		// No statistics - should default to using range scan
		scan, ok, err := tryRangeScan("users", indexInfo, filters, nil)
		require.NoError(t, err)
		assert.True(t, ok, "expected range scan to be used without stats")
		assert.Equal(t, ScanTypeIndexRange, scan.Type)
	})

	t.Run("with selective stats - uses range scan", func(t *testing.T) {
		t.Parallel()

		// Build range conditions: age >= 25 AND age <= 50
		filters := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
				Operator: Gte,
				Operand2: Operand{Type: OperandInteger, Value: int32(25)},
			},
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
				Operator: Lte,
				Operand2: Operand{Type: OperandInteger, Value: int32(50)},
			},
		}

		// Statistics showing selective range (30% of rows)
		stats := &IndexStats{
			NEntry:    1000,
			NDistinct: []int64{500},
		}

		scan, ok, err := tryRangeScan("users", indexInfo, filters, stats)
		require.NoError(t, err)
		assert.True(t, ok, "expected range scan to be used without stats")
		assert.Equal(t, ScanTypeIndexRange, scan.Type)
	})

	t.Run("with non-selective stats - skips range scan", func(t *testing.T) {
		t.Parallel()

		// Build range condition: age >= 50 (one bound = 50% selectivity)
		filters := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
				Operator: Gte,
				Operand2: Operand{Type: OperandInteger, Value: int32(50)},
			},
		}

		// Statistics showing non-selective range (50% of rows)
		stats := &IndexStats{
			NEntry:    1000,
			NDistinct: []int64{500},
		}

		_, ok, err := tryRangeScan("users", indexInfo, filters, stats)
		require.NoError(t, err)
		// Should reject range scan due to low selectivity
		assert.False(t, ok, "expected range scan to be rejected with non-selective stats, got scan type")
	})

	t.Run("edge case - zero rows in stats", func(t *testing.T) {
		t.Parallel()

		filters := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
				Operator: Gte,
				Operand2: Operand{Type: OperandInteger, Value: int32(25)},
			},
		}

		// Empty stats - should default to using range scan
		stats := &IndexStats{
			NEntry:    0,
			NDistinct: []int64{},
		}

		scan, ok, err := tryRangeScan("users", indexInfo, filters, stats)
		require.NoError(t, err)
		assert.True(t, ok, "expected range scan to be used with empty stats (default behavior)")
		assert.Equal(t, ScanTypeIndexRange, scan.Type)
	})
}
