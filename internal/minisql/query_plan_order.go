package minisql

func (p QueryPlan) optimizeOrdering(t *Table, originalConditions OneOrMore) QueryPlan {
	// No ORDER BY clause
	if len(p.OrderBy) == 0 {
		return p
	}

	if len(p.OrderBy) > 1 {
		// TODO - Multiple ORDER BY columns (revisit later)
		// Always need in-memory sort for now
		p.SortInMemory = true
		return p
	}

	// Single column ORDER BY
	var orderCol = p.OrderBy[0].Field.Name
	p.SortReverse = p.OrderBy[0].Direction == Desc

	// If there are no indexes, we must sort in memory
	if t.HasNoIndex() {
		p.SortInMemory = true
		return p
	}

	// Sequential scan - no filters, just ordering
	if len(p.Scans) == 1 && p.Scans[0].Type == ScanTypeSequential && len(p.Scans[0].Filters) == 0 {
		// Use index for ordering if available
		if info, ok := t.IndexInfoByColumnName(orderCol); ok {
			p.Scans[0].Type = ScanTypeIndexAll
			p.Scans[0].IndexName = info.Name
			p.Scans[0].IndexColumns = info.Columns
			p.SortInMemory = false
			return p
		}

		// No index for ORDER BY column - must sort in memory
		p.SortInMemory = true
		return p
	}

	// Index scan (point, range, or full) - check if we should switch to ORDER BY index
	if len(p.Scans) == 1 && p.Scans[0].Type != ScanTypeSequential {
		currentIndexName := p.Scans[0].IndexName

		// If already using the ORDER BY index, check if sort is needed
		if orderByInfo, ok := t.IndexInfoByColumnName(orderCol); ok {
			if currentIndexName == orderByInfo.Name {
				// Index provides ordering for range scans and full scans
				// Point scans with multiple keys need sorting (e.g., IN clause)
				scanType := p.Scans[0].Type
				needsSort := scanType == ScanTypeIndexPoint && len(p.Scans[0].IndexKeys) > 1
				p.SortInMemory = needsSort
				return p
			}

			// We have filters on one index and ORDER BY on another
			// Decide: filter index + sort vs. ORDER BY index + filter
			canUseOrderByIndex := p.canUseOrderByIndexWithFilters(t, orderByInfo)
			if canUseOrderByIndex {
				var filterStats *IndexStats
				if stats, ok := t.indexStats[currentIndexName]; ok {
					filterStats = &stats
				}

				if p.shouldSwitchToOrderByIndex(filterStats) {
					// Switch to ORDER BY index and move ALL filters to post-scan filtering
					p.Scans[0].Type = ScanTypeIndexAll
					p.Scans[0].IndexName = orderByInfo.Name
					p.Scans[0].IndexColumns = orderByInfo.Columns
					p.Scans[0].IndexKeys = nil
					p.Scans[0].RangeCondition = RangeCondition{}
					// Restore original conditions as filters (none are satisfied by ORDER BY index)
					if len(originalConditions) > 0 {
						p.Scans[0].Filters = originalConditions
					}
					p.SortInMemory = false
					return p
				}
			}
		}

		// Keep current scan, sort in memory
		p.SortInMemory = true
		return p
	}

	// Default: sort in memory
	p.SortInMemory = true
	return p
}

// canUseOrderByIndexWithFilters checks if we can use the ORDER BY index
// when we have filters (they'd need to be applied in memory)
func (p QueryPlan) canUseOrderByIndexWithFilters(t *Table, orderByInfo IndexInfo) bool {
	// For now, only support this optimization for simple cases:
	// - Single scan
	// - Filters can be applied in memory (no complex conditions)
	if len(p.Scans) != 1 {
		return false
	}

	// Filters must be applicable as post-scan filters
	// This is always possible for now (all our conditions support in-memory evaluation)
	return true
}

// shouldSwitchToOrderByIndex decides whether to switch from filter index to ORDER BY index
// based on estimated result set size and sorting cost
func (p QueryPlan) shouldSwitchToOrderByIndex(filterStats *IndexStats) bool {
	if filterStats == nil {
		// No stats - use conservative heuristic
		// Don't switch for point lookups (likely small result set)
		if len(p.Scans) == 1 && p.Scans[0].Type == ScanTypeIndexPoint {
			return false
		}
		// For range scans without stats, default to keeping filter index
		return false
	}

	// Estimate how many rows the current filter will return
	var estimatedRows int64 = -1

	scan := p.Scans[0]
	switch scan.Type {
	case ScanTypeIndexPoint:
		// Point lookup - typically returns few rows
		estimatedRows = estimateFilteredRows(filterStats, nil)
	case ScanTypeIndexRange:
		// Range scan - estimate using range condition
		estimatedRows = estimateFilteredRows(filterStats, &scan.RangeCondition)
	case ScanTypeIndexAll:
		// Full index scan - returns all rows (no filtering benefit)
		estimatedRows = filterStats.NEntry
	}

	if estimatedRows < 0 {
		// Can't estimate - be conservative
		return false
	}

	// Decision logic:
	// If filtered result set is large (> threshold), sorting is expensive
	// Better to use ORDER BY index and filter in memory
	return estimatedRows > sortCostThreshold
}
