package minisql

import (
	"context"
	"fmt"
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
		indexKeys                   = make([][]any, 0, 10)
		remaining                   = make([]Conditions, 0, len(conditions))
		groupIndex                  = make(map[int]IndexInfo)
		indexMap                    = t.IndexMap()
	)

	// Each group is separated by OR, for example:
	// (a = 1 AND b = 2) OR (a = 3 AND b = 4)
	// would be 2 groups with 2 conditions each
	for groupIDx, group := range conditions {
		// Check if this group contains an index condition
		var (
			hasIndexCondition = false
			remainingForGroup = make(Conditions, 0, 10)
			indexKeysForGroup = make([]any, 0, 10)
		)
		for _, aCondition := range group {
			fieldName, ok := aCondition.Operand1.Value.(string)
			if !ok {
				return fmt.Errorf("invalid field name in condition: %v", aCondition.Operand1.Value)
			}
			_, ok = indexMap[fieldName]
			if !ok {
				remainingForGroup = append(remainingForGroup, aCondition)
				continue
			}
			aColumn, ok := t.ColumnByName(fieldName)
			if !ok {
				return fmt.Errorf("invalid field name in condition: %s", fieldName)
			}

			// If group contains conditions on multiple indexes, we must pick only one.
			hasIndexCondition = true
			info, ok := pickIndexInfo(t, fieldName)
			if !ok {
				return fmt.Errorf("could not find index info for field: %s", fieldName)
			}
			groupIndex[groupIDx] = info

			if isEquality(aCondition) && aCondition.Operand2.Type != OperandNull {
				keys, err := equalityKeys(aColumn, aCondition)
				if err != nil {
					return err
				}

				indexKeysForGroup = append(indexKeysForGroup, keys...)
			} else {
				remainingForGroup = append(remainingForGroup, aCondition)
				continue
			}
		}
		if !hasIndexCondition {
			allGroupsHaveIndexCondition = false
			break
		}
		remaining = append(remaining, remainingForGroup)
		indexKeys = append(indexKeys, indexKeysForGroup)
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
		rangeScan, ok := tryRangeScan(groupIndex[groupIdx], group)
		if ok {
			indexScans = append(indexScans, rangeScan)
			continue
		}

		// Only choose point scan if there are keys to lookup, for example in case of
		// NOT IN clause, we won't have any keys to lookup even though there is an index condition.
		if len(indexKeys[groupIdx]) > 0 {
			indexScans = append(indexScans, Scan{
				Type:            ScanTypeIndexPoint,
				IndexName:       groupIndex[groupIdx].Name,
				IndexColumnName: groupIndex[groupIdx].Column.Name,
				IndexKeys:       indexKeys[groupIdx],
				Filters:         OneOrMore{remaining[groupIdx]},
			})
		}
	}
	// We could get here and have no index scans available, in that case do not overwrite existing plan.
	if len(indexScans) > 0 {
		p.Scans = indexScans
	}

	return nil
}

// pickIndexInfo chooses index info for a given field name, preferring primary key,
// then unique indexes, then secondary indexes
func pickIndexInfo(t *Table, fieldName string) (IndexInfo, bool) {
	if fieldName == t.PrimaryKey.Column.Name {
		return t.PrimaryKey.IndexInfo, true
	}
	for _, uniqueIndex := range t.UniqueIndexes {
		if uniqueIndex.Column.Name == fieldName {
			return uniqueIndex.IndexInfo, true
		}
	}

	for _, secondaryIndex := range t.SecondaryIndexes {
		if secondaryIndex.Column.Name == fieldName {
			return secondaryIndex.IndexInfo, true
		}
	}
	return IndexInfo{}, false
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

func tryRangeScan(indexInfo IndexInfo, filters Conditions) (Scan, bool) {
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

		conditionValue, err := castKeyValue(indexInfo.Column, aCondition.Operand2.Value)
		if err != nil {
			return Scan{}, false
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
		case Eq, In:
			// id = X, id IN (...) - we will be doing index point scan instead so just return
			return Scan{}, false
		case Ne, NotIn:
			// id != X , id NOT IN (...) - we will be doing a sequential scan so just return
			return Scan{}, false
		default:
			// This should not happen, we cover all operators with switch cases above
			return Scan{}, false
		}
	}

	if rangeCondition.Lower == nil && rangeCondition.Upper == nil {
		return Scan{}, false
	}

	// Validate range is sensible
	if rangeCondition.Lower != nil && rangeCondition.Upper != nil {
		cmp := compareAny(rangeCondition.Lower.Value, rangeCondition.Upper.Value)
		if cmp > 0 {
			// Lower > Upper = empty range, no results
			// Could optimize by returning empty result, but for now just don't use index
			return Scan{}, false
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
	return aScan, true
}
