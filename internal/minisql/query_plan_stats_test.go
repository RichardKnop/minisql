package minisql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseIndexStats tests the stat string parsing
func TestParseIndexStats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		statString  string
		expected    IndexStats
		expectError bool
	}{
		{
			name:       "single column index",
			statString: "1000 500",
			expected: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			expectError: false,
		},
		{
			name:       "composite index",
			statString: "1000 10 50 800",
			expected: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{10, 50, 800},
			},
			expectError: false,
		},
		{
			name:        "invalid format - too few parts",
			statString:  "1000",
			expectError: true,
		},
		{
			name:        "invalid format - non-numeric nEntry",
			statString:  "abc 500",
			expectError: true,
		},
		{
			name:        "invalid format - non-numeric nDistinct",
			statString:  "1000 abc",
			expectError: true,
		},
		{
			name:        "empty string",
			statString:  "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats, err := parseIndexStats(tt.statString)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected.NEntry, stats.NEntry)
			assert.Equal(t, tt.expected.NDistinct, stats.NDistinct)
		})
	}
}

// TestIndexStats_Selectivity tests the selectivity calculation
func TestIndexStats_Selectivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stats     IndexStats
		expected  float64
		tolerance float64
	}{
		{
			name: "single column 50% selectivity",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{500},
			},
			expected:  0.5,
			tolerance: 0.01,
		},
		{
			name: "single column 100% selectivity",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{1000},
			},
			expected:  1.0,
			tolerance: 0.01,
		},
		{
			name: "single column 1% selectivity",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{10},
			},
			expected:  0.01,
			tolerance: 0.001,
		},
		{
			name: "composite index - uses final prefix",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{10, 50, 800},
			},
			expected:  0.8, // Uses last value: 800/1000
			tolerance: 0.01,
		},
		{
			name: "zero entries",
			stats: IndexStats{
				NEntry:    0,
				NDistinct: []int64{0},
			},
			expected:  0.0,
			tolerance: 0.0,
		},
		{
			name: "no distinct values",
			stats: IndexStats{
				NEntry:    1000,
				NDistinct: []int64{},
			},
			expected:  0.0,
			tolerance: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectivity := tt.stats.Selectivity()
			assert.InDelta(t, tt.expected, selectivity, tt.tolerance)
		})
	}
}

func TestIndexStats_EstimateRangeRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rangeCondition RangeCondition
		name           string
		stats          IndexStats
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
		rangeCondition RangeCondition
		name           string
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
		rangeCondition RangeCondition
		stats          *IndexStats
		name           string
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

func TestBuildEquiDepthHistogram(t *testing.T) {
	t.Parallel()

	t.Run("uniform_distribution", func(t *testing.T) {
		t.Parallel()
		sorted := make([]float64, 100)
		for i := range sorted {
			sorted[i] = float64(i + 1)
		}
		h := buildEquiDepthHistogram(sorted, 10)
		require.NotNil(t, h)
		assert.Len(t, h.Bounds, 11) // numBuckets+1
		assert.Equal(t, float64(1), h.Bounds[0])
		assert.Equal(t, float64(100), h.Bounds[10])
	})

	t.Run("fewer_values_than_buckets", func(t *testing.T) {
		t.Parallel()
		sorted := []float64{1, 2, 3}
		h := buildEquiDepthHistogram(sorted, 10)
		require.NotNil(t, h)
		// Capped at n=3 buckets → 4 bounds
		assert.Len(t, h.Bounds, 4)
	})

	t.Run("empty_slice_returns_nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, buildEquiDepthHistogram(nil, 10))
		assert.Nil(t, buildEquiDepthHistogram([]float64{}, 10))
	})

	t.Run("zero_buckets_returns_nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, buildEquiDepthHistogram([]float64{1, 2, 3}, 0))
	})
}

func TestHistogramCDF(t *testing.T) {
	t.Parallel()
	// 4 buckets: [0,25), [25,50), [50,75), [75,100] → bounds = [0,25,50,75,100]
	bounds := []float64{0, 25, 50, 75, 100}

	t.Run("below_min_returns_0", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0.0, histogramCDF(bounds, -1))
	})

	t.Run("at_min_returns_0", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0.0, histogramCDF(bounds, 0))
	})

	t.Run("at_max_returns_1", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 1.0, histogramCDF(bounds, 100))
	})

	t.Run("midpoint_of_first_bucket", func(t *testing.T) {
		t.Parallel()
		// v=12.5 is midpoint of [0,25) → bucket 0, within=0.5 → 0 + 0.5/4 = 0.125
		got := histogramCDF(bounds, 12.5)
		assert.InDelta(t, 0.125, got, 0.001)
	})

	t.Run("midpoint_of_second_bucket", func(t *testing.T) {
		t.Parallel()
		// v=37.5 is midpoint of [25,50) → bucket 1, within=0.5 → 1/4 + 0.5/4 = 0.375
		got := histogramCDF(bounds, 37.5)
		assert.InDelta(t, 0.375, got, 0.001)
	})

	t.Run("at_bucket_boundary", func(t *testing.T) {
		t.Parallel()
		// v=50 is at start of bucket 2: [50,75) → bucket 2, within=0 → 2/4 = 0.5
		got := histogramCDF(bounds, 50)
		assert.InDelta(t, 0.5, got, 0.001)
	})
}

func TestEstimateSelectivityWithHistogram(t *testing.T) {
	t.Parallel()
	// Uniform 0..100, 10 buckets: [0,10,20,30,40,50,60,70,80,90,100]
	bounds := make([]float64, 11)
	for i := range bounds {
		bounds[i] = float64(i * 10)
	}
	hist := &Histogram{Bounds: bounds}

	t.Run("no_histogram_falls_back_to_fixed_constants", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{Lower: &RangeBound{Value: int64(10), Inclusive: true}}
		got := estimateSelectivityWithHistogram(nil, rc)
		assert.Equal(t, 0.5, got) // fixed constant for single bound
	})

	t.Run("lower_bound_only", func(t *testing.T) {
		t.Parallel()
		// value > 50 → 1 - CDF(50) = 1 - 0.5 = 0.5
		rc := RangeCondition{Lower: &RangeBound{Value: int64(50), Inclusive: false}}
		got := estimateSelectivityWithHistogram(hist, rc)
		assert.InDelta(t, 0.5, got, 0.01)
	})

	t.Run("upper_bound_only", func(t *testing.T) {
		t.Parallel()
		// value < 30 → CDF(30) = 0.3
		rc := RangeCondition{Upper: &RangeBound{Value: int64(30), Inclusive: false}}
		got := estimateSelectivityWithHistogram(hist, rc)
		assert.InDelta(t, 0.3, got, 0.01)
	})

	t.Run("both_bounds", func(t *testing.T) {
		t.Parallel()
		// 20 < value < 60 → CDF(60) - CDF(20) = 0.6 - 0.2 = 0.4
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(20), Inclusive: false},
			Upper: &RangeBound{Value: int64(60), Inclusive: false},
		}
		got := estimateSelectivityWithHistogram(hist, rc)
		assert.InDelta(t, 0.4, got, 0.01)
	})

	t.Run("non_convertible_bound_falls_back_to_fixed", func(t *testing.T) {
		t.Parallel()
		// string value can't be converted → lower stays 0, upper stays 1 for unbounded
		rc := RangeCondition{Lower: &RangeBound{Value: "text", Inclusive: true}}
		got := estimateSelectivityWithHistogram(hist, rc)
		// lower=0 (failed conversion), upper=1.0 → sel = 1.0
		assert.Equal(t, 1.0, got)
	})
}

func TestParseIndexStats_WithHistogram(t *testing.T) {
	t.Parallel()

	t.Run("parses_histogram_bounds", func(t *testing.T) {
		t.Parallel()
		s := "100 50|h=1,25,50,75,100"
		stats, err := parseIndexStats(s)
		require.NoError(t, err)
		assert.Equal(t, int64(100), stats.NEntry)
		assert.Equal(t, []int64{50}, stats.NDistinct)
		require.NotNil(t, stats.Hist)
		assert.Equal(t, []float64{1, 25, 50, 75, 100}, stats.Hist.Bounds)
	})

	t.Run("no_histogram_section", func(t *testing.T) {
		t.Parallel()
		stats, err := parseIndexStats("100 50")
		require.NoError(t, err)
		assert.Nil(t, stats.Hist)
	})

	t.Run("invalid_histogram_bound", func(t *testing.T) {
		t.Parallel()
		_, err := parseIndexStats("100 50|h=1,abc,100")
		assert.Error(t, err)
	})
}

func TestSerializeHistogram(t *testing.T) {
	t.Parallel()

	t.Run("nil_histogram_writes_nothing", func(t *testing.T) {
		t.Parallel()
		var b strings.Builder
		serializeHistogram(&b, nil)
		assert.Empty(t, b.String())
	})

	t.Run("serializes_bounds", func(t *testing.T) {
		t.Parallel()
		var b strings.Builder
		h := &Histogram{Bounds: []float64{0, 50, 100}}
		serializeHistogram(&b, h)
		assert.Equal(t, "|h=0,50,100", b.String())
	})

	t.Run("roundtrip", func(t *testing.T) {
		t.Parallel()
		sorted := make([]float64, 100)
		for i := range sorted {
			sorted[i] = float64(i + 1)
		}
		h := buildEquiDepthHistogram(sorted, 16)
		require.NotNil(t, h)

		var b strings.Builder
		fmt.Fprintf(&b, "%d %d", 100, 100)
		serializeHistogram(&b, h)

		parsed, err := parseIndexStats(b.String())
		require.NoError(t, err)
		require.NotNil(t, parsed.Hist)
		assert.Equal(t, len(h.Bounds), len(parsed.Hist.Bounds))
		for i := range h.Bounds {
			assert.InDelta(t, h.Bounds[i], parsed.Hist.Bounds[i], 0.0001)
		}
	})
}

func TestEstimateRangeRows_WithHistogram(t *testing.T) {
	t.Parallel()

	// Build histogram for values 1..100
	sorted := make([]float64, 100)
	for i := range sorted {
		sorted[i] = float64(i + 1)
	}
	hist := buildEquiDepthHistogram(sorted, 10)

	stats := IndexStats{
		NEntry:    100,
		NDistinct: []int64{100},
		Hist:      hist,
	}

	t.Run("narrow_range_uses_histogram", func(t *testing.T) {
		t.Parallel()
		// value between 40 and 60 should be ~20 rows
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(40), Inclusive: true},
			Upper: &RangeBound{Value: int64(60), Inclusive: true},
		}
		got := stats.EstimateRangeRows(rc, 0)
		assert.InDelta(t, 20, float64(got), 5.0)
	})

	t.Run("full_range_returns_all", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(1), Inclusive: true},
			Upper: &RangeBound{Value: int64(100), Inclusive: true},
		}
		got := stats.EstimateRangeRows(rc, 0)
		assert.InDelta(t, 100, float64(got), 5.0)
	})
}
