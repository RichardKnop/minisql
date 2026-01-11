package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexStats_EstimateRangeRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		stats          IndexStats
		rangeCondition RangeCondition
		want           int64
	}{
		{
			name: "both bounds - 30% selectivity",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 10, Inclusive: true},
				Upper: &RangeBound{Value: 50, Inclusive: true},
			},
			want: 300, // 1000 * 0.3
		},
		{
			name: "lower bound only - 50% selectivity",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 500, Inclusive: true},
			},
			want: 500, // 1000 * 0.5
		},
		{
			name: "upper bound only - 50% selectivity",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			rangeCondition: RangeCondition{
				Upper: &RangeBound{Value: 500, Inclusive: false},
			},
			want: 500, // 1000 * 0.5
		},
		{
			name: "no bounds - 100% selectivity",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			rangeCondition: RangeCondition{},
			want:           1000, // 1000 * 1.0
		},
		{
			name: "no stats - returns -1",
			stats: IndexStats{
				NEntry:    0,
				NDistinct: []int64{},
			},
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 10, Inclusive: true},
			},
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.stats.EstimateRangeRows(tt.rangeCondition, 0)
			assert.Equal(t, tt.want, got, "estimateRangeSelectivity() = %v, want %v", got, tt.want)
		})
	}
}

func TestEstimateRangeSelectivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		rangeCondition RangeCondition
		want           float64
	}{
		{
			name: "both bounds",
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 10, Inclusive: true},
				Upper: &RangeBound{Value: 50, Inclusive: true},
			},
			want: 0.3,
		},
		{
			name: "lower bound only",
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 500, Inclusive: true},
			},
			want: 0.5,
		},
		{
			name: "upper bound only",
			rangeCondition: RangeCondition{
				Upper: &RangeBound{Value: 500, Inclusive: false},
			},
			want: 0.5,
		},
		{
			name:           "no bounds",
			rangeCondition: RangeCondition{},
			want:           1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := estimateRangeSelectivity(tt.rangeCondition)
			assert.Equal(t, tt.want, got, "estimateRangeSelectivity() = %v, want %v", got, tt.want)
		})
	}
}

func TestShouldUseIndexForRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		stats          *IndexStats
		rangeCondition RangeCondition
		want           bool
	}{
		{
			name: "selective range - use index (30% < threshold)",
			stats: &IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 10, Inclusive: true},
				Upper: &RangeBound{Value: 50, Inclusive: true},
			},
			want: true, // 30% selectivity < 30% threshold, but it's equal so treated as selective
		},
		{
			name: "non-selective range - don't use index (50% > threshold)",
			stats: &IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 500, Inclusive: true},
			},
			want: false, // 50% selectivity > 30% threshold
		},
		{
			name:  "no stats - default to using index",
			stats: nil,
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 10, Inclusive: true},
			},
			want: true,
		},
		{
			name: "empty stats - default to using index",
			stats: &IndexStats{
				NEntry:    0,
				NDistinct: []int64{},
			},
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 10, Inclusive: true},
			},
			want: true,
		},
		{
			name: "highly selective range - use index",
			stats: &IndexStats{
				NEntry:    10000,
				NDistinct: []int64{5000},
			},
			rangeCondition: RangeCondition{
				Lower: &RangeBound{Value: 100, Inclusive: true},
				Upper: &RangeBound{Value: 200, Inclusive: true},
			},
			want: true, // 30% selectivity <= threshold
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := shouldUseIndexForRange(tt.stats, tt.rangeCondition)
			assert.Equal(t, tt.want, got, "shouldUseIndexForRange() = %v, want %v", got, tt.want)
		})
	}
}
