package minisql

import (
	"fmt"
	"strconv"
	"strings"
)

// IndexStats holds parsed statistics for an index
type IndexStats struct {
	NEntry    int64   // Total number of entries in the index
	NDistinct []int64 // Distinct values for each column prefix
}

// parseIndexStats parses a stat string in SQLite format: "nEntry nDistinct1 nDistinct2 ..."
// Example: "100 50" means 100 entries, 50 distinct values
// Example: "100 10 50" means 100 entries, 10 distinct col1, 50 distinct (col1,col2)
func parseIndexStats(statString string) (IndexStats, error) {
	parts := strings.Fields(statString)
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
// Uses uniform distribution assumption for simplicity.
func (s IndexStats) EstimateRangeRows(rangeCondition RangeCondition, columnIndex int) int64 {
	if s.NEntry == 0 || len(s.NDistinct) == 0 {
		return -1 // Can't estimate
	}

	// For simplicity, assume uniform distribution
	// This could be enhanced with histogram data in the future

	// Calculate selectivity based on bounds
	selectivity := estimateRangeSelectivity(rangeCondition)

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

const (
	// Cost thresholds for index vs table scan decision
	indexScanThreshold = 0.3 // If index scan returns >30% of rows, table scan may be faster

	// Cost threshold for ORDER BY optimization
	// If filtered result set is larger than this, prefer ORDER BY index to avoid sorting
	sortCostThreshold = 1000
)

// estimateRangeSelectivity estimates the selectivity of a range condition.
// Uses conservative estimates based on whether bounds are present.
// This assumes uniform distribution and can be enhanced with histogram data.
func estimateRangeSelectivity(rangeCondition RangeCondition) float64 {
	// Default estimates based on presence of bounds:
	// Both bounds: assume 30% selectivity (fairly selective)
	// One bound: assume 50% selectivity (half the data)
	// No bounds: 100% (full scan)
	// This is conservative and can be refined with actual min/max tracking

	if rangeCondition.Lower != nil && rangeCondition.Upper != nil {
		return 0.3 // Both bounds - estimated 30% of rows
	}
	if rangeCondition.Lower != nil || rangeCondition.Upper != nil {
		return 0.5 // One bound - estimated 50% of rows
	}
	return 1.0 // No bounds - full scan
}
