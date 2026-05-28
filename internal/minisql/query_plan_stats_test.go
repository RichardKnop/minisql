package minisql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildMCV(t *testing.T) {
	t.Parallel()
	freq := map[string]int64{
		"open":    900,
		"closed":  80,
		"pending": 20,
	}
	mcv := buildMCV(freq, 2)
	require.Len(t, mcv, 2)
	assert.Equal(t, "open", mcv[0].Value)
	assert.Equal(t, int64(900), mcv[0].Count)
	assert.Equal(t, "closed", mcv[1].Value)
	assert.Equal(t, int64(80), mcv[1].Count)
}

func TestBuildMCV_EmptyFreq(t *testing.T) {
	t.Parallel()
	assert.Empty(t, buildMCV(nil, 50))
}

func TestBuildMCV_FewerThanN(t *testing.T) {
	t.Parallel()
	freq := map[string]int64{"a": 5, "b": 3}
	mcv := buildMCV(freq, 50)
	assert.Len(t, mcv, 2)
}

func TestSerializeParseMCV_RoundTrip(t *testing.T) {
	t.Parallel()

	input := []MCVEntry{
		{Value: "open", Count: 900},
		{Value: "a,b:c", Count: 50}, // commas and colons must be URL-encoded
		{Value: "42", Count: 10},
	}

	var sb strings.Builder
	serializeMCV(&sb, input)
	s := sb.String()
	require.NotEmpty(t, s)

	const prefix = "|mcv="
	require.True(t, strings.HasPrefix(s, prefix))
	got, err := parseMCV(s[len(prefix):])
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "open", got[0].Value)
	assert.Equal(t, int64(900), got[0].Count)
	assert.Equal(t, "a,b:c", got[1].Value)
	assert.Equal(t, int64(50), got[1].Count)
	assert.Equal(t, "42", got[2].Value)
	assert.Equal(t, int64(10), got[2].Count)
}

func TestSerializeMCV_Empty(t *testing.T) {
	t.Parallel()
	var sb strings.Builder
	serializeMCV(&sb, nil)
	assert.Empty(t, sb.String())
}

func TestParseMCV_Empty(t *testing.T) {
	t.Parallel()
	got, err := parseMCV("")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestParseIndexStats_WithMCV(t *testing.T) {
	t.Parallel()
	stat := "1000 5 |h=1,2,3|mcv=open:900,closed:80"
	s, err := parseIndexStats(stat)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), s.NEntry)
	require.Len(t, s.NDistinct, 1)
	assert.Equal(t, int64(5), s.NDistinct[0])
	require.NotNil(t, s.Hist)
	require.Len(t, s.MCV, 2)
	assert.Equal(t, "open", s.MCV[0].Value)
	assert.Equal(t, int64(900), s.MCV[0].Count)
}

func TestParseIndexStats_MCVOnly(t *testing.T) {
	t.Parallel()
	stat := "500 3|mcv=foo:400,bar:100"
	s, err := parseIndexStats(stat)
	require.NoError(t, err)
	assert.Equal(t, int64(500), s.NEntry)
	assert.Nil(t, s.Hist)
	require.Len(t, s.MCV, 2)
	assert.Equal(t, "foo", s.MCV[0].Value)
}

func TestParseIndexStats_NoMCV(t *testing.T) {
	t.Parallel()
	// Existing format without MCV must still parse.
	stat := "200 10 |h=1,10,100"
	s, err := parseIndexStats(stat)
	require.NoError(t, err)
	assert.Equal(t, int64(200), s.NEntry)
	assert.Nil(t, s.MCV)
}

func TestEstimateEqualityRows(t *testing.T) {
	t.Parallel()

	s := IndexStats{
		NEntry:    1000,
		NDistinct: []int64{5},
		MCV: []MCVEntry{
			{Value: "open", Count: 900},
			{Value: "closed", Count: 80},
		},
	}

	t.Run("mcv_hit_exact", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, int64(900), s.EstimateEqualityRows("open"))
		assert.Equal(t, int64(80), s.EstimateEqualityRows("closed"))
	})

	t.Run("mcv_miss_ndv_fallback", func(t *testing.T) {
		t.Parallel()
		// "pending" not in MCV → NEntry/NDistinct = 1000/5 = 200
		assert.Equal(t, int64(200), s.EstimateEqualityRows("pending"))
	})

	t.Run("no_ndv_returns_nentry", func(t *testing.T) {
		t.Parallel()
		empty := IndexStats{NEntry: 500}
		assert.Equal(t, int64(500), empty.EstimateEqualityRows("anything"))
	})
}

func TestShouldUseIndexForEquality(t *testing.T) {
	t.Parallel()

	stats := &IndexStats{
		NEntry:    1000,
		NDistinct: []int64{5},
		MCV: []MCVEntry{
			{Value: "open", Count: 900}, // 90% → prefer sequential
			{Value: "rare", Count: 10},  // 1%  → use index
		},
	}

	assert.False(t, shouldUseIndexForEquality(stats, "open", 1000),
		"90% selectivity should skip the index")
	assert.True(t, shouldUseIndexForEquality(stats, "rare", 1000),
		"1% selectivity should use index")

	// Missing stats → always use index.
	assert.True(t, shouldUseIndexForEquality(nil, "any", 1000))

	// Unknown table row count → always use index.
	assert.True(t, shouldUseIndexForEquality(stats, "open", 0))
	assert.True(t, shouldUseIndexForEquality(stats, "open", -1))
}

func TestAnyToFloat64(t *testing.T) {
	t.Parallel()

	f, ok := anyToFloat64(int32(7))
	assert.True(t, ok)
	assert.InDelta(t, float64(7), f, 1e-9)

	f, ok = anyToFloat64(int64(42))
	assert.True(t, ok)
	assert.InDelta(t, float64(42), f, 1e-9)

	f, ok = anyToFloat64(float32(3.14))
	assert.True(t, ok)
	assert.InDelta(t, 3.14, f, 0.001)

	f, ok = anyToFloat64(float64(2.718))
	assert.True(t, ok)
	assert.InDelta(t, 2.718, f, 1e-9)

	_, ok = anyToFloat64("not a number")
	assert.False(t, ok)

	_, ok = anyToFloat64(nil)
	assert.False(t, ok)
}

func TestHistogramCDF(t *testing.T) {
	t.Parallel()

	bounds := []float64{0, 25, 50, 75, 100}

	t.Run("empty_bounds_returns_half", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.5, histogramCDF([]float64{}, 50), 1e-9)
		assert.InDelta(t, 0.5, histogramCDF([]float64{10}, 50), 1e-9)
	})

	t.Run("below_min_returns_zero", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.0, histogramCDF(bounds, -1), 1e-9)
		assert.InDelta(t, 0.0, histogramCDF(bounds, 0), 1e-9)
	})

	t.Run("above_max_returns_one", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 1.0, histogramCDF(bounds, 100), 1e-9)
		assert.InDelta(t, 1.0, histogramCDF(bounds, 200), 1e-9)
	})

	t.Run("midpoint_of_first_bucket", func(t *testing.T) {
		t.Parallel()
		// Midpoint of [0, 25] → CDF at 12.5 is 0/4 + 0.5*(1/4) = 0.125
		got := histogramCDF(bounds, 12.5)
		assert.InDelta(t, 0.125, got, 1e-9)
	})

	t.Run("midpoint_of_last_bucket", func(t *testing.T) {
		t.Parallel()
		// Midpoint of [75, 100] → bucket 3 of 4; bucketStart = 3/4 = 0.75
		// withinBucket = (87.5 - 75) / (100 - 75) = 0.5 → 0.75 + 0.5/4 = 0.875
		got := histogramCDF(bounds, 87.5)
		assert.InDelta(t, 0.875, got, 1e-9)
	})

	t.Run("degenerate_bucket_same_bounds", func(t *testing.T) {
		t.Parallel()
		// All values the same: bounds = [5, 5, 5].
		degen := []float64{5, 5, 5}
		// v=5 falls exactly on bounds[0], so returns 0.
		assert.InDelta(t, 0.0, histogramCDF(degen, 5), 1e-9)
		// v=6 > bounds[n=2] so returns 1.
		assert.InDelta(t, 1.0, histogramCDF(degen, 6), 1e-9)
	})
}

func TestEstimateSelectivityWithHistogram(t *testing.T) {
	t.Parallel()

	bounds := []float64{0, 25, 50, 75, 100}
	hist := &Histogram{Bounds: bounds}

	t.Run("nil_hist_falls_back_to_range_selectivity", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(10), Inclusive: true},
			Upper: &RangeBound{Value: int64(90), Inclusive: true},
		}
		sel := estimateSelectivityWithHistogram(nil, rc)
		// Fallback: both bounds → 0.3
		assert.InDelta(t, 0.3, sel, 1e-9)
	})

	t.Run("short_hist_falls_back", func(t *testing.T) {
		t.Parallel()
		shortHist := &Histogram{Bounds: []float64{0}}
		rc := RangeCondition{Lower: &RangeBound{Value: int64(10)}}
		sel := estimateSelectivityWithHistogram(shortHist, rc)
		// Fallback: one bound → 0.5
		assert.InDelta(t, 0.5, sel, 1e-9)
	})

	t.Run("full_range_covered", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(0), Inclusive: true},
			Upper: &RangeBound{Value: int64(100), Inclusive: true},
		}
		sel := estimateSelectivityWithHistogram(hist, rc)
		// CDF(100)=1.0, CDF(0)=0.0 → sel=1.0
		assert.InDelta(t, 1.0, sel, 1e-9)
	})

	t.Run("lower_bound_only", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(50), Inclusive: true},
		}
		sel := estimateSelectivityWithHistogram(hist, rc)
		// upper defaults to 1.0, lower = CDF(50) = 0.5 → sel = 0.5
		assert.InDelta(t, 0.5, sel, 1e-9)
	})

	t.Run("upper_bound_only", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{
			Upper: &RangeBound{Value: int64(25), Inclusive: true},
		}
		sel := estimateSelectivityWithHistogram(hist, rc)
		// lower defaults to 0.0, upper = CDF(25) = 0.25 → sel = 0.25
		assert.InDelta(t, 0.25, sel, 1e-9)
	})

	t.Run("non_numeric_bound_uses_cdf_zero_fallback", func(t *testing.T) {
		t.Parallel()
		// String values are not convertible by anyToFloat64 → CDF not applied.
		rc := RangeCondition{
			Lower: &RangeBound{Value: "abc"},
			Upper: &RangeBound{Value: "xyz"},
		}
		sel := estimateSelectivityWithHistogram(hist, rc)
		// upper stays 1.0, lower stays 0.0 → sel = 1.0
		assert.InDelta(t, 1.0, sel, 1e-9)
	})

	t.Run("impossible_range_clamped_to_zero", func(t *testing.T) {
		t.Parallel()
		// Lower > Upper in histogram terms.
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(90), Inclusive: true},
			Upper: &RangeBound{Value: int64(10), Inclusive: true},
		}
		sel := estimateSelectivityWithHistogram(hist, rc)
		assert.InDelta(t, 0.0, sel, 1e-9)
	})
}

func TestEstimateRangeSelectivity(t *testing.T) {
	t.Parallel()

	t.Run("both_bounds", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{
			Lower: &RangeBound{Value: int64(10)},
			Upper: &RangeBound{Value: int64(90)},
		}
		assert.InDelta(t, 0.3, estimateRangeSelectivity(rc), 1e-9)
	})

	t.Run("lower_only", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{Lower: &RangeBound{Value: int64(10)}}
		assert.InDelta(t, 0.5, estimateRangeSelectivity(rc), 1e-9)
	})

	t.Run("upper_only", func(t *testing.T) {
		t.Parallel()
		rc := RangeCondition{Upper: &RangeBound{Value: int64(90)}}
		assert.InDelta(t, 0.5, estimateRangeSelectivity(rc), 1e-9)
	})

	t.Run("no_bounds_full_scan", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 1.0, estimateRangeSelectivity(RangeCondition{}), 1e-9)
	})
}

func TestSelectivity(t *testing.T) {
	t.Parallel()

	t.Run("zero_entries", func(t *testing.T) {
		t.Parallel()
		s := IndexStats{NEntry: 0, NDistinct: []int64{5}}
		assert.InDelta(t, 0.0, s.Selectivity(), 1e-9)
	})

	t.Run("no_ndistinct", func(t *testing.T) {
		t.Parallel()
		s := IndexStats{NEntry: 1000}
		assert.InDelta(t, 0.0, s.Selectivity(), 1e-9)
	})

	t.Run("normal_case", func(t *testing.T) {
		t.Parallel()
		s := IndexStats{NEntry: 1000, NDistinct: []int64{100}}
		assert.InDelta(t, 0.1, s.Selectivity(), 1e-9)
	})

	t.Run("composite_uses_last_ndistinct", func(t *testing.T) {
		t.Parallel()
		s := IndexStats{NEntry: 1000, NDistinct: []int64{10, 500}}
		assert.InDelta(t, 0.5, s.Selectivity(), 1e-9)
	})
}

func TestEstimateFilteredRows_ZeroDistinct(t *testing.T) {
	t.Parallel()
	// When NDistinct[last] == 0, estimateFilteredRows should return -1.
	stats := &IndexStats{NEntry: 1000, NDistinct: []int64{0}}
	assert.Equal(t, int64(-1), estimateFilteredRows(stats, nil))
}

func TestBuildEquiDepthHistogram(t *testing.T) {
	t.Parallel()

	t.Run("empty_returns_nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, buildEquiDepthHistogram(nil, 10))
	})

	t.Run("zero_buckets_returns_nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, buildEquiDepthHistogram([]float64{1, 2, 3}, 0))
	})

	t.Run("more_buckets_than_values_clamped", func(t *testing.T) {
		t.Parallel()
		// 3 values, 100 buckets → clamped to 3 buckets, 4 bounds.
		h := buildEquiDepthHistogram([]float64{1, 5, 10}, 100)
		require.NotNil(t, h)
		assert.InDelta(t, 1.0, h.Bounds[0], 1e-9)
		assert.InDelta(t, 10.0, h.Bounds[len(h.Bounds)-1], 1e-9)
	})

	t.Run("typical_case", func(t *testing.T) {
		t.Parallel()
		sorted := make([]float64, 100)
		for i := range sorted {
			sorted[i] = float64(i + 1)
		}
		h := buildEquiDepthHistogram(sorted, 4)
		require.NotNil(t, h)
		require.Len(t, h.Bounds, 5)
		assert.InDelta(t, 1.0, h.Bounds[0], 1e-9)
		assert.InDelta(t, 100.0, h.Bounds[4], 1e-9)
	})
}

func TestSerializeParseHistogram_RoundTrip(t *testing.T) {
	t.Parallel()

	h := &Histogram{Bounds: []float64{0, 25.5, 50, 75.25, 100}}
	var sb strings.Builder
	serializeHistogram(&sb, h)
	s := sb.String()
	require.NotEmpty(t, s)

	const prefix = "|h="
	require.True(t, strings.HasPrefix(s, prefix))
	got, err := parseHistogram(s[len(prefix):])
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Bounds, 5)
	assert.InDelta(t, 0.0, got.Bounds[0], 1e-9)
	assert.InDelta(t, 100.0, got.Bounds[4], 1e-9)
}

func TestSerializeHistogram_Nil(t *testing.T) {
	t.Parallel()
	var sb strings.Builder
	serializeHistogram(&sb, nil)
	assert.Empty(t, sb.String())

	serializeHistogram(&sb, &Histogram{})
	assert.Empty(t, sb.String())
}

func TestParseHistogram_TooFewBounds(t *testing.T) {
	t.Parallel()
	// Single bound → parseHistogram returns nil (need at least 2).
	got, err := parseHistogram("42")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestParseHistogram_BadInput(t *testing.T) {
	t.Parallel()
	_, err := parseHistogram("1,bad,3")
	require.Error(t, err)
}
