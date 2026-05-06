package minisql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	// histogramBuckets is the number of equi-depth histogram buckets built
	// during ANALYZE for numeric index columns.
	histogramBuckets = 32

	// Cost thresholds for index vs table scan decision
	indexScanThreshold = 0.3 // If index scan returns >30% of rows, table scan may be faster

	// Cost threshold for ORDER BY optimization
	// If filtered result set is larger than this, prefer ORDER BY index to avoid sorting
	sortCostThreshold = 1000
)

// Histogram is an equi-depth histogram for a numeric index column.
// Bounds contains numBuckets+1 boundary values: Bounds[0] is the minimum
// observed value, Bounds[N] is the maximum. Each of the N buckets between
// consecutive boundaries holds approximately the same number of entries.
type Histogram struct {
	Bounds []float64
}

// IndexStats holds parsed statistics for an index
type IndexStats struct {
	NDistinct []int64
	NEntry    int64
	Hist      *Histogram // nil when not collected or column type is non-numeric
}

// isNumericColumn reports whether c supports histogram collection.
func isNumericColumn(c Column) bool {
	switch c.Kind {
	case Int4, Int8, Real, Double, Timestamp:
		return true
	default:
		return false
	}
}

// anyToFloat64 converts a numeric index key value to float64 for histogram use.
func anyToFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}

// buildEquiDepthHistogram constructs an equi-depth histogram from a sorted
// slice of float64 values. The returned Histogram has numBuckets+1 boundary
// values. Returns nil if sorted is empty or numBuckets <= 0.
func buildEquiDepthHistogram(sorted []float64, numBuckets int) *Histogram {
	n := len(sorted)
	if n == 0 || numBuckets <= 0 {
		return nil
	}
	if numBuckets > n {
		numBuckets = n
	}
	bounds := make([]float64, numBuckets+1)
	bounds[0] = sorted[0]
	bounds[numBuckets] = sorted[n-1]
	for i := 1; i < numBuckets; i++ {
		idx := i * n / numBuckets
		bounds[i] = sorted[idx]
	}
	return &Histogram{Bounds: bounds}
}

// histogramCDF returns the estimated fraction of entries with value <= v,
// using linear interpolation within each bucket.
func histogramCDF(bounds []float64, v float64) float64 {
	n := len(bounds) - 1
	if n <= 0 {
		return 0.5
	}
	if v <= bounds[0] {
		return 0
	}
	if v >= bounds[n] {
		return 1.0
	}
	// Binary search for the largest i such that bounds[i] <= v.
	lo, hi := 0, n-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if bounds[mid] <= v {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	// v is in bucket lo: [bounds[lo], bounds[lo+1]).
	bucketStart := float64(lo) / float64(n)
	if bounds[lo+1] == bounds[lo] {
		return bucketStart + 1.0/float64(n)
	}
	withinBucket := (v - bounds[lo]) / (bounds[lo+1] - bounds[lo])
	return bucketStart + withinBucket/float64(n)
}

// estimateSelectivityWithHistogram estimates the fraction of entries satisfying
// rc. Uses histogram CDF when available; falls back to fixed constants otherwise.
func estimateSelectivityWithHistogram(hist *Histogram, rc RangeCondition) float64 {
	if hist == nil || len(hist.Bounds) < 2 {
		return estimateRangeSelectivity(rc)
	}

	lower := 0.0
	if rc.Lower != nil {
		if f, ok := anyToFloat64(rc.Lower.Value); ok {
			lower = histogramCDF(hist.Bounds, f)
		}
	}

	upper := 1.0
	if rc.Upper != nil {
		if f, ok := anyToFloat64(rc.Upper.Value); ok {
			upper = histogramCDF(hist.Bounds, f)
		}
	}

	sel := upper - lower
	if sel < 0 {
		return 0
	}
	if sel > 1 {
		return 1
	}
	return sel
}

// serializeHistogram appends the histogram to a stat string builder.
func serializeHistogram(b *strings.Builder, h *Histogram) {
	if h == nil || len(h.Bounds) == 0 {
		return
	}
	b.WriteString("|h=")
	for i, v := range h.Bounds {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	}
}

// parseHistogram parses a histogram from the "|h=b0,b1,...,bN" suffix.
func parseHistogram(s string) (*Histogram, error) {
	parts := strings.Split(s, ",")
	bounds := make([]float64, 0, len(parts))
	for _, p := range parts {
		f, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid histogram bound %q: %w", p, err)
		}
		bounds = append(bounds, f)
	}
	if len(bounds) < 2 {
		return nil, nil
	}
	sort.Float64s(bounds)
	return &Histogram{Bounds: bounds}, nil
}

// parseIndexStats parses a stat string: "nEntry nDistinct1 [nDistinct2 ...][|h=b0,b1,...]"
func parseIndexStats(statString string) (IndexStats, error) {
	mainPart := statString
	var hist *Histogram
	if before, histStr, ok := strings.Cut(statString, "|h="); ok {
		mainPart = before
		var err error
		hist, err = parseHistogram(histStr)
		if err != nil {
			return IndexStats{}, err
		}
	}

	parts := strings.Fields(mainPart)
	if len(parts) < 2 {
		return IndexStats{}, fmt.Errorf("invalid stat format: %s", statString)
	}

	nEntry, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return IndexStats{}, fmt.Errorf("invalid nEntry: %w", err)
	}

	nDistinct := make([]int64, 0, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		val, err := strconv.ParseInt(parts[i], 10, 64)
		if err != nil {
			return IndexStats{}, fmt.Errorf("invalid nDistinct value at position %d: %w", i, err)
		}
		nDistinct = append(nDistinct, val)
	}

	return IndexStats{
		NEntry:    nEntry,
		NDistinct: nDistinct,
		Hist:      hist,
	}, nil
}

// Selectivity returns the selectivity of the index (0.0 to 1.0)
// Higher selectivity means more distinct values relative to total entries
// For composite indexes, uses the final column prefix
func (s IndexStats) Selectivity() float64 {
	if s.NEntry == 0 {
		return 0.0
	}
	if len(s.NDistinct) == 0 {
		return 0.0
	}
	// Use the last nDistinct value (most specific prefix)
	finalDistinct := s.NDistinct[len(s.NDistinct)-1]
	return float64(finalDistinct) / float64(s.NEntry)
}

// EstimateRangeRows estimates the number of rows that will match a range condition.
// Returns the estimated row count, or -1 if estimation isn't possible.
// Uses histogram data when available, otherwise assumes uniform distribution.
func (s IndexStats) EstimateRangeRows(rangeCondition RangeCondition, columnIndex int) int64 {
	if s.NEntry == 0 || len(s.NDistinct) == 0 {
		return -1 // Can't estimate
	}

	selectivity := estimateSelectivityWithHistogram(s.Hist, rangeCondition)
	return int64(float64(s.NEntry) * selectivity)
}

// shouldUseIndexForRange decides if an index scan is better than table scan for a range query.
// Returns true if the index scan is estimated to be more efficient.
func shouldUseIndexForRange(stats *IndexStats, rangeCondition RangeCondition) bool {
	if stats == nil {
		// No stats - default to using index for range queries
		return true
	}

	estimatedRows := stats.EstimateRangeRows(rangeCondition, 0)
	if estimatedRows < 0 {
		// Can't estimate - default to using index
		return true
	}

	// Compare estimated selectivity against threshold
	selectivity := float64(estimatedRows) / float64(stats.NEntry)
	return selectivity <= indexScanThreshold
}

// estimateFilteredRows estimates how many rows will remain after applying filters.
// Returns -1 if estimation is not possible.
func estimateFilteredRows(stats *IndexStats, rangeCondition *RangeCondition) int64 {
	if stats == nil {
		return -1
	}

	// For range conditions, use existing range estimation
	if rangeCondition != nil {
		return stats.EstimateRangeRows(*rangeCondition, 0)
	}

	// For equality conditions, estimate based on selectivity
	// This is a simple heuristic: average rows per distinct value
	if len(stats.NDistinct) > 0 && stats.NEntry > 0 {
		distinctValues := stats.NDistinct[len(stats.NDistinct)-1]
		if distinctValues > 0 {
			return stats.NEntry / distinctValues
		}
	}

	return -1
}

// estimateRangeSelectivity estimates the selectivity of a range condition
// using conservative fixed constants (fallback when no histogram is available).
func estimateRangeSelectivity(rangeCondition RangeCondition) float64 {
	if rangeCondition.Lower != nil && rangeCondition.Upper != nil {
		return 0.3 // Both bounds - estimated 30% of rows
	}
	if rangeCondition.Lower != nil || rangeCondition.Upper != nil {
		return 0.5 // One bound - estimated 50% of rows
	}
	return 1.0 // No bounds - full scan
}
