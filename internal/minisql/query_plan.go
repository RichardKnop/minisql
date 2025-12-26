package minisql

import (
	"context"
	"fmt"
	"strings"
)

type ScanType int

const (
	ScanTypeSequential ScanType = iota + 1 // Full table scan
	ScanTypeIndexAll                       // Full index scan
	ScanTypeIndexPoint                     // Index lookup for specific key(s)
	ScanTypeIndexRange                     // Index range scan
)

func (st ScanType) String() string {
	switch st {
	case ScanTypeSequential:
		return "sequential"
	case ScanTypeIndexAll:
		return "index_all"
	case ScanTypeIndexPoint:
		return "index_point"
	case ScanTypeIndexRange:
		return "index_range"
	default:
		return "unknown"
	}
}

type RangeBound struct {
	Value     any
	Inclusive bool // true for >= or <=, false for > or <
}

type RangeCondition struct {
	Lower *RangeBound // nil = unbounded
	Upper *RangeBound // nil = unbounded
}

// QueryPlan determines how to execute a query
type QueryPlan struct {
	Scans []Scan

	// Ordering
	OrderBy      []OrderBy
	SortInMemory bool
	SortReverse  bool
}

type Scan struct {
	Type            ScanType
	IndexName       string
	IndexColumnName string
	IndexKeys       []any          // Keys to lookup in index
	RangeCondition  RangeCondition // upper/lower bounds for range scan
	Filters         OneOrMore      // Additional filters to apply
}

// FilterRow applies filtering on scanned rows according to filters
func (s Scan) FilterRow(aRow Row) (bool, error) {
	ok, err := aRow.CheckOneOrMore(s.Filters)
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (p QueryPlan) Execute(ctx context.Context, t *Table, selectedFields []Field, filteredPipe chan<- Row) error {
	if len(p.Scans) == 1 {
		switch p.Scans[0].Type {
		case ScanTypeIndexAll:
			return t.indexScanAll(ctx, p, p.Scans[0], selectedFields, filteredPipe)
		case ScanTypeIndexRange:
			return t.indexRangeScan(ctx, p.Scans[0], selectedFields, filteredPipe)
		case ScanTypeIndexPoint:
			return t.indexPointScan(ctx, p.Scans[0], selectedFields, filteredPipe)
		case ScanTypeSequential:
			return t.sequentialScan(ctx, p.Scans[0], selectedFields, filteredPipe)
		default:
			return fmt.Errorf("unhandled scan type in single scan: %d", p.Scans[0].Type)
		}
	}
	for _, aScan := range p.Scans {
		switch aScan.Type {
		case ScanTypeIndexRange:
			if err := t.indexRangeScan(ctx, aScan, selectedFields, filteredPipe); err != nil {
				return err
			}
		case ScanTypeIndexPoint:
			if err := t.indexPointScan(ctx, aScan, selectedFields, filteredPipe); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unhandled scan type in single scan: %d", aScan.Type)
		}
	}
	return nil
}

// PlanQuery creates a query plan based on the statement and table schema
func (t *Table) PlanQuery(ctx context.Context, stmt Statement) (QueryPlan, error) {
	// By default, assume we are doing a single sequential scan
	plan := QueryPlan{
		Scans: []Scan{{
			Type:    ScanTypeSequential,
			Filters: stmt.Conditions,
		}},
		OrderBy: stmt.OrderBy,
	}

	// If there is no where clause, no need to consider index scans
	if len(stmt.Conditions) == 0 {
		// But we might still use index for ordering
		return plan.optimizeOrdering(t), nil
	}

	// If there are no indexes, we cannot do index scans
	if !t.HasPrimaryKey() && len(t.UniqueIndexes) == 0 && len(t.SecondaryIndexes) == 0 {
		// But we might still use index for ordering
		return plan.optimizeOrdering(t), nil
	}

	// Check if we can do an index scans
	if err := plan.setIndexScans(t, stmt.Conditions); err != nil {
		return QueryPlan{}, err
	}

	// But we might still use index for ordering
	return plan.optimizeOrdering(t), nil
}

func (p QueryPlan) optimizeOrdering(t *Table) QueryPlan {
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

	// Sequential scan
	if len(p.Scans) == 1 && p.Scans[0].Type == ScanTypeSequential {
		indexMap := t.IndexMap()

		// Either order ORDER BY indexed column
		if info, ok := indexMap[orderCol]; ok {
			// Use PK index for ordering
			p.Scans[0].Type = ScanTypeIndexAll
			p.Scans[0].IndexName = info.Name
			p.Scans[0].IndexColumnName = info.Column.Name
			p.SortInMemory = false
			return p
		}

		// TODO: Check for secondary indexes on orderCol
		// For now, fall through to in-memory sort
		p.SortInMemory = true
	}

	p.SortInMemory = true

	return p
}

// Check whether we can perform an index scan. Each condition group is separated by OR,
// and within each group conditions are ANDed together. We can only use an index scan
// if each group contains at least one primary key equality condition. We also need to
// keep track of remaining conditions in each group for further filtering. For example:
//
// WHERE (pk = 1 AND a = 'foo') OR (pk = 2 AND b = 'bar')
//
// can be executed as an index scan on for keys 1 and 2 with remaining filters
// (a = 'foo') for 1 and (b = 'bar') for 2.
func (p *QueryPlan) setIndexScans(t *Table, conditions OneOrMore) error {
	var (
		allGroupsHaveIndexCondition = true

		equalityIndex = make(map[int]IndexInfo)
		equalityCond  = make(map[int]int)
		otherIndexes  = make(map[int][]IndexInfo)
		indexKeys     = make(map[int][]any)
		indexMap      = t.IndexMap()
	)

	// Each group is separated by OR, for example:
	// (a = 1 AND b = 2) OR (a = 3 AND b = 4)
	// would be 2 groups with 2 conditions each
	for groupIDx, group := range conditions {
		// Check if this group contains an index condition
		for condIdx, aCondition := range group {
			fieldName, ok := aCondition.Operand1.Value.(string)
			if !ok {
				return fmt.Errorf("invalid field name in condition: %v", aCondition.Operand1.Value)
			}
			_, ok = indexMap[fieldName]
			if !ok {
				continue
			}
			aColumn, ok := t.ColumnByName(fieldName)
			if !ok {
				return fmt.Errorf("invalid field name in condition: %s", fieldName)
			}

			// If group contains conditions on multiple indexes, we must pick only one.
			info, ok := t.IndexInfoByColumnName(fieldName)
			if !ok {
				return fmt.Errorf("could not find index info for field: %s", fieldName)
			}

			// Only consider equality conditions for index scans where right operand is not NULL
			isEquality := isEquality(aCondition) && aCondition.Operand2.Type != OperandNull

			if !isEquality {
				otherIndexes[groupIDx] = append(otherIndexes[groupIDx], info)
				continue
			}

			if _, ok := equalityIndex[groupIDx]; !ok {
				equalityIndex[groupIDx] = info
				equalityCond[groupIDx] = condIdx
				keys, err := equalityKeys(aColumn, aCondition)
				if err != nil {
					return err
				}

				indexKeys[groupIDx] = keys
			} else if aColumn.Name == t.PrimaryKey.Column.Name {
				// Prefer primary key index if available
				equalityIndex[groupIDx] = info
				equalityCond[groupIDx] = condIdx

				keys, err := equalityKeys(aColumn, aCondition)
				if err != nil {
					return err
				}

				indexKeys[groupIDx] = keys
			} else if aColumn.Unique && strings.HasPrefix(equalityIndex[groupIDx].Name, "idx__") {
				// Prefer unique index over secondary index
				equalityIndex[groupIDx] = info
				equalityCond[groupIDx] = condIdx

				keys, err := equalityKeys(aColumn, aCondition)
				if err != nil {
					return err
				}

				indexKeys[groupIDx] = keys
			}
		}
		if _, ok := equalityIndex[groupIDx]; !ok && len(otherIndexes[groupIDx]) == 0 {
			allGroupsHaveIndexCondition = false
			break
		}
	}

	// In case all groups don't contain an index key condition, we cannot do index scan
	// since we would need to do a full table scan anyway for the group without index condition.
	if !allGroupsHaveIndexCondition {
		return nil
	}

	// In case we reach here, we can do index scans..
	// We need to check if we can do range scans instead of point lookups.
	indexScans := make([]Scan, 0, len(conditions))

	for groupIdx, group := range conditions {
		// Try index point scan first
		equalityIndex, ok := equalityIndex[groupIdx]
		if ok {
			filters := make(Conditions, 0, len(group))
			for condIdx, aCondition := range group {
				if condIdx != equalityCond[groupIdx] {
					filters = append(filters, aCondition)
				}
			}
			indexScans = append(indexScans, Scan{
				Type:            ScanTypeIndexPoint,
				IndexName:       equalityIndex.Name,
				IndexColumnName: equalityIndex.Column.Name,
				IndexKeys:       indexKeys[groupIdx],
				Filters:         OneOrMore{filters},
			})
			continue
		}

		// Try range scans on other indexes
		foundRangeScan := false
		for _, idxInfo := range otherIndexes[groupIdx] {
			rangeScan, ok, err := tryRangeScan(idxInfo, group)
			if err != nil {
				return err
			}
			if ok {
				indexScans = append(indexScans, rangeScan)
				foundRangeScan = true
				break
			}
		}

		if foundRangeScan {
			continue
		}

		// Otherwise fall back to sequential scan for this group
		indexScans = append(indexScans, Scan{
			Type:    ScanTypeSequential,
			Filters: OneOrMore{group},
		})
	}

	// We could get here and have no index scans available, in that case do not overwrite existing plan.
	if len(indexScans) > 0 {
		p.Scans = indexScans
	}

	// // Combine all sequential scans into one if possible
	// if len(p.Scans) > 1 {
	// 	sequentialIdxs := make([]int, 0)
	// 	for scanIdx, aScan := range p.Scans {
	// 		if aScan.Type == ScanTypeSequential {
	// 			sequentialIdxs = append(sequentialIdxs, scanIdx)
	// 		}
	// 	}
	// 	if len(sequentialIdxs) > 1 {
	// 		combinedConditions := make(OneOrMore, 0)
	// 		for i := len(sequentialIdxs) - 1; i >= 0; i-- {
	// 			scanIdx := sequentialIdxs[i]
	// 			combinedConditions = append(combinedConditions, p.Scans[scanIdx].Filters...)
	// 			// Remove this scan
	// 			p.Scans = slices.Delete(p.Scans, scanIdx, scanIdx+1)
	// 		}
	// 		// Add combined sequential scan
	// 		p.Scans = append(p.Scans, Scan{
	// 			Type:    ScanTypeSequential,
	// 			Filters: combinedConditions,
	// 		})
	// 	}
	// }

	return nil
}

func isEquality(cond Condition) bool {
	// Check: column_name = literal_value
	// Also consider IN operator for primary key
	if cond.Operator != Eq && cond.Operator != In {
		return false
	}

	if cond.Operand1.Type != OperandField {
		return false
	}

	// Right operand must be a literal (not another field)
	if cond.Operand2.Type == OperandField {
		return false
	}

	return true
}

func equalityKeys(aColumn Column, cond Condition) ([]any, error) {
	if cond.Operator == Eq {
		if cond.Operand2.Type == OperandNull {
			return []any{cond.Operand2.Value}, nil
		}
		keyValue, err := castKeyValue(aColumn, cond.Operand2.Value)
		if err != nil {
			return nil, err
		}
		return []any{keyValue}, nil
	}

	// TODO what if NULL is included in list?
	keyValues := make([]any, 0, len(cond.Operand2.Value.([]any)))
	for _, rawValue := range cond.Operand2.Value.([]any) {
		keyValue, err := castKeyValue(aColumn, rawValue)
		if err != nil {
			return nil, err
		}
		keyValues = append(keyValues, keyValue)
	}

	return keyValues, nil
}

func tryRangeScan(indexInfo IndexInfo, filters Conditions) (Scan, bool, error) {
	var (
		rangeCondition   = RangeCondition{}
		remainingFilters = make(Conditions, 0)
	)

	// Scan conditions to find range predicates on PK
	for _, aCondition := range filters {
		if aCondition.Operand1.Type != OperandField {
			remainingFilters = append(remainingFilters, aCondition)
			continue
		}

		fieldName, ok := aCondition.Operand1.Value.(string)
		if !ok || fieldName != indexInfo.Column.Name {
			remainingFilters = append(remainingFilters, aCondition)
			continue
		}

		// Can't use index if comparing to another field
		if aCondition.Operand2.Type == OperandField {
			remainingFilters = append(remainingFilters, aCondition)
			continue
		}

		if aCondition.Operator == Eq || aCondition.Operator == Ne {
			// id = X, id IN (...) - we will be doing index point scan instead so just return
			return Scan{}, false, nil
		}

		if aCondition.Operator == In || aCondition.Operator == NotIn {
			// id != X , id NOT IN (...) - we will be doing a sequential scan so just return
			return Scan{}, false, nil
		}

		conditionValue, err := castKeyValue(indexInfo.Column, aCondition.Operand2.Value)
		if err != nil {
			return Scan{}, false, err
		}

		switch aCondition.Operator {
		case Gt:
			// id > X
			if rangeCondition.Lower == nil ||
				compareAny(conditionValue, rangeCondition.Lower.Value) > 0 {
				rangeCondition.Lower = &RangeBound{
					Value:     conditionValue,
					Inclusive: false,
				}
			}
		case Gte:
			// id >= X
			if rangeCondition.Lower == nil ||
				compareAny(conditionValue, rangeCondition.Lower.Value) > 0 {
				rangeCondition.Lower = &RangeBound{
					Value:     conditionValue,
					Inclusive: true,
				}
			}
		case Lt:
			// id < X
			if rangeCondition.Upper == nil ||
				compareAny(conditionValue, rangeCondition.Upper.Value) < 0 {
				rangeCondition.Upper = &RangeBound{
					Value:     conditionValue,
					Inclusive: false,
				}
			}
		case Lte:
			// id <= X
			if rangeCondition.Upper == nil ||
				compareAny(conditionValue, rangeCondition.Upper.Value) < 0 {
				rangeCondition.Upper = &RangeBound{
					Value:     conditionValue,
					Inclusive: true,
				}
			}
		default:
			return Scan{}, false, fmt.Errorf("invalid operator for range scan: %d", aCondition.Operator)
		}
	}

	if rangeCondition.Lower == nil && rangeCondition.Upper == nil {
		return Scan{}, false, nil
	}

	// Validate range is sensible
	if rangeCondition.Lower != nil && rangeCondition.Upper != nil {
		cmp := compareAny(rangeCondition.Lower.Value, rangeCondition.Upper.Value)
		if cmp > 0 {
			// Lower > Upper = empty range, no results
			// Could optimize by returning empty result, but for now just don't use index
			return Scan{}, false, nil
		}
	}

	// Create range scan plan
	aScan := Scan{
		Type:            ScanTypeIndexRange,
		IndexName:       indexInfo.Name,
		IndexColumnName: indexInfo.Column.Name,
		RangeCondition:  rangeCondition,
	}
	if len(remainingFilters) > 0 {
		aScan.Filters = OneOrMore{remainingFilters}
	}
	return aScan, true, nil
}
