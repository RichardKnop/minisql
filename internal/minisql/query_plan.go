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
	Type           ScanType
	IndexName      string
	IndexColumns   []Column
	IndexKeys      []any          // Keys to lookup in index
	RangeCondition RangeCondition // upper/lower bounds for range scan
	Filters        OneOrMore      // Additional filters to apply
}

/*
PlanQuery creates a query plan based on the statement and table schema.
This is in no way a sophisticated query planner, but a simple heuristic-based approach
to determine whether an index scan can be used based on the WHERE conditions.

Consider a table with the following schema:
CREATE TABLE users (

	id INTEGER PRIMARY KEY,
	email TEXT UNIQUE,
	non_indexed_col TEXT,
	created TIMESTAMP DEFAULT NOW()

);
CREATE INDEX "idx_created" ON "users" (created);

If we select without a WHERE clause, we default to a sequential scan:
SELECT * from users;

If a WHERE clause cannot be fully satisfied by an index or combination of indexes,
we fall back to a sequential scan.

Remeber that if you have multiple conditions separated by OR, if a single one does not use
an index, we have to do a sequential scan anyway. Also remember that non equality conditions
using != or NOT IN or conditions comparing to NULL cannot use indexes.

SEQUENTIAL SCANS:
-----------------
SELECT * from users WHERE non_indexed_col = 'baz';
SELECT * from users WHERE id = 1 OR non_indexed_col = 'baz';
SELECT * FROM users WHERE id = 1 OR id IS NULL;
SELECT * FROM users WHERE pk NOT IN (1,2,3);

INDEX POINT SCANS:
------------------
When there are only equality conditions on indexed columns (primary key or unique indexes),
we can do index point scans:

SELECT * from users WHERE id = 1; - single point scan on PK index
SELECT * from users WHERE id IN (1, 2, 3); - single point scan on PK index
SELECT * from users WHERE email = 'foo@example.com'; - single point scan on unique index
SELECT * FROM users WHERE id = 1 OR id id = 2 OR email = 'foo@example.com'; - multiple point scans

RANGE SCANS:
------------
For >, >=, <, <= conditions on indexed columns, we can do range scans:

SELECT * FROM users WHERE id > 10 AND id < 20 AND non_indexed_col = 'baz'; - range scan on PK index

For OR conditions, we can do multiple range scans if each condition group has a range condition.
For example, following query can be executed as two range scans, one on PK column and one on secondary index:

SELECT * FROM users WHERE (id >= 10 AND id <= 20) OR created >= '2024-01-01';

When combining range scans with ordering, we currently fall back to in-memory sort:

SELECT * FROM users WHERE id >= 10 AND id <= 20 ORDER BY created DESC;

ORDER BY:
---------
SELECT * from users ORDER BY id DESC; - use PK index for ordering
SELECT * from users ORDER BY created DESC; - use index on created for ordering
SELECT * from users ORDER BY non_indexed_col; - order in memory
*/
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
		// Either order ORDER BY indexed column
		if info, ok := t.IndexInfoByColumnName(orderCol); ok {
			// Use PK index for ordering
			p.Scans[0].Type = ScanTypeIndexAll
			p.Scans[0].IndexName = info.Name
			p.Scans[0].IndexColumns = info.Columns
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

// indexMatch represents a potential index match for equality conditions
type indexMatch struct {
	info                IndexInfo
	matchedConditions   map[int]bool    // tracks which condition indices were matched
	keys                []any           // composite key values in column order
	rangeCondition      *RangeCondition // Set for partial composite index matches
	hasProperUpperBound bool            // False if range scan lacks upper bound (needs filtering)
	isPrimaryKey        bool
	isUnique            bool
}

// findBestEqualityIndexMatch finds the best index that can be used for equality conditions
// in the given group. It tries to match composite indexes by finding conditions on multiple
// columns that form a prefix of the index columns (e.g., for index on (a,b,c), it can match
// conditions on 'a', 'a AND b', or 'a AND b AND c', but not 'b' or 'a AND c').
//
// Priority order: Primary Key > Unique Index > Secondary Index
// For composite indexes, prefer indexes that match more columns.
func findBestEqualityIndexMatch(t *Table, group Conditions) *indexMatch {
	var bestMatch *indexMatch

	// Helper to check if a new match is better than the current best
	isBetterMatch := func(newMatch *indexMatch) bool {
		if bestMatch == nil {
			return true
		}

		// Always prefer primary key
		if newMatch.isPrimaryKey && !bestMatch.isPrimaryKey {
			return true
		}
		if !newMatch.isPrimaryKey && bestMatch.isPrimaryKey {
			return false
		}

		// Both are PK or both are not PK - compare number of matched columns (more is better)
		newMatchedCols := len(newMatch.matchedConditions)
		bestMatchedCols := len(bestMatch.matchedConditions)
		if newMatchedCols != bestMatchedCols {
			return newMatchedCols > bestMatchedCols
		}

		// Same number of columns - prefer by index type (unique over secondary)
		if newMatch.isUnique && !bestMatch.isUnique {
			return true
		}
		return false
	}

	// Collect all available indexes
	var allIndexes []IndexInfo
	if t.HasPrimaryKey() {
		allIndexes = append(allIndexes, t.PrimaryKey.IndexInfo)
	}
	for _, idx := range t.UniqueIndexes {
		allIndexes = append(allIndexes, idx.IndexInfo)
	}
	for _, idx := range t.SecondaryIndexes {
		allIndexes = append(allIndexes, idx.IndexInfo)
	}

	// Try each index
	for _, indexInfo := range allIndexes {
		match := tryMatchIndex(t, indexInfo, group)
		if match != nil && isBetterMatch(match) {
			bestMatch = match
		}
	}

	return bestMatch
}

// tryMatchIndex attempts to match an index against the conditions in a group.
// It returns a match if it can find equality conditions for a prefix of the index columns.
func tryMatchIndex(t *Table, indexInfo IndexInfo, group Conditions) *indexMatch {
	matchedConditions := make(map[int]bool)
	var compositeKey []any

	// For composite indexes, we need to match columns in order from left to right
	for colIdx, indexCol := range indexInfo.Columns {
		foundMatch := false

		// Look for an equality condition on this column
		for condIdx, aCondition := range group {
			// Skip already matched conditions
			if matchedConditions[condIdx] {
				continue
			}

			// Check if this is an equality condition on the current index column
			if !isEquality(aCondition) {
				continue
			}

			if aCondition.Operand1.Type != OperandField {
				continue
			}

			fieldName, ok := aCondition.Operand1.Value.(string)
			if !ok || fieldName != indexCol.Name {
				continue
			}

			// Skip NULL comparisons
			if aCondition.Operand2.Type == OperandNull {
				continue
			}

			// We found a match for this column
			column, ok := t.ColumnByName(fieldName)
			if !ok {
				continue
			}

			keys, err := equalityKeys(column, aCondition)
			if err != nil {
				continue
			}

			// For composite indexes, we currently only support single value equality (not IN)
			// for non-final columns. The final column can use IN.
			if colIdx < len(indexInfo.Columns)-1 && len(keys) > 1 {
				// This is not the last column and we have multiple values (IN clause)
				// Skip this match as we can't handle it yet
				continue
			}

			matchedConditions[condIdx] = true
			compositeKey = append(compositeKey, keys...)
			foundMatch = true
			break
		}

		// If we couldn't match this column, we can't use this index for a composite key
		// However, we can still use what we've matched so far if this is a prefix
		if !foundMatch {
			break
		}
	}

	// No conditions matched this index
	if len(matchedConditions) == 0 {
		return nil
	}

	// Determine index type
	isPK := t.HasPrimaryKey() && indexInfo.Name == t.PrimaryKey.Name
	_, isUnique := t.UniqueIndexes[indexInfo.Name]

	// Determine if this is a partial match (prefix of composite index)
	numMatchedColumns := len(matchedConditions)
	isPartialMatch := numMatchedColumns > 0 && numMatchedColumns < len(indexInfo.Columns)

	var indexKeys []any
	var rangeCondition *RangeCondition
	var hasProperUpperBound bool

	if isPartialMatch {
		// Partial composite index match - use range scan
		// For example, index on (a,b,c) with conditions a=1, b=2 should scan range:
		// Lower: (1, 2) inclusive
		// Upper: (1, 3) exclusive (or (1, 2, MAX) if we can't increment)
		matchedCols := indexInfo.Columns[:numMatchedColumns]
		lowerKey := NewCompositeKey(matchedCols, compositeKey...)

		// Try to create upper bound by incrementing the last matched value
		lastValue := compositeKey[len(compositeKey)-1]
		nextValue := incrementValue(lastValue)

		if nextValue != nil {
			// We can increment - create upper bound with next value
			upperKeyValues := make([]any, len(compositeKey))
			copy(upperKeyValues, compositeKey)
			upperKeyValues[len(upperKeyValues)-1] = nextValue
			upperKey := NewCompositeKey(matchedCols, upperKeyValues...)

			rangeCondition = &RangeCondition{
				Lower: &RangeBound{Value: lowerKey, Inclusive: true},
				Upper: &RangeBound{Value: upperKey, Inclusive: false},
			}
			hasProperUpperBound = true
		} else {
			// Can't increment (e.g., max value or float) - use only lower bound
			// WARNING: Without upper bound, we'll scan more entries than needed
			// and must filter matched conditions during scan
			rangeCondition = &RangeCondition{
				Lower: &RangeBound{Value: lowerKey, Inclusive: true},
			}
			hasProperUpperBound = false
		}
	} else if numMatchedColumns > 1 {
		// Full composite index match - use point scan
		// Create composite key with all matched columns
		matchedCols := indexInfo.Columns[:numMatchedColumns]
		indexKeys = []any{NewCompositeKey(matchedCols, compositeKey...)}
	} else {
		// Single column index - use raw key value(s)
		indexKeys = compositeKey
	}

	return &indexMatch{
		info:                indexInfo,
		matchedConditions:   matchedConditions,
		keys:                indexKeys,
		rangeCondition:      rangeCondition,
		hasProperUpperBound: hasProperUpperBound,
		isPrimaryKey:        isPK,
		isUnique:            isUnique,
	}
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
		equalityMatch               = make(map[int]*indexMatch)
		otherIndexes                = make(map[int][]IndexInfo)
	)

	// Each group is separated by OR, for example:
	// (a = 1 AND b = 2) OR (a = 3 AND b = 4)
	// would be 2 groups with 2 conditions each
	for groupIdx, group := range conditions {
		// Try to find the best index match for this group
		match := findBestEqualityIndexMatch(t, group)
		if match != nil {
			equalityMatch[groupIdx] = match
		}

		// Also collect indexes that could be used for range scans
		for _, aCondition := range group {
			if aCondition.Operand1.Type != OperandField {
				continue
			}
			fieldName, ok := aCondition.Operand1.Value.(string)
			if !ok {
				continue
			}
			if !t.HasIndexOnColumn(fieldName) {
				continue
			}

			info, ok := t.IndexInfoByColumnName(fieldName)
			if !ok {
				continue
			}

			// Skip if this condition was already matched for equality
			if match != nil {
				alreadyMatched := false
				for condIdx := range match.matchedConditions {
					if &group[condIdx] == &aCondition {
						alreadyMatched = true
						break
					}
				}
				if alreadyMatched {
					continue
				}
			}

			// Check if this could be used for range scan
			isEq := isEquality(aCondition) && aCondition.Operand2.Type != OperandNull
			if !isEq {
				// Add to potential range scan indexes
				otherIndexes[groupIdx] = append(otherIndexes[groupIdx], info)
			}
		}

		if equalityMatch[groupIdx] == nil && len(otherIndexes[groupIdx]) == 0 {
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
		match, ok := equalityMatch[groupIdx]
		if ok {
			filters := make(Conditions, 0, len(group))
			for condIdx, aCondition := range group {
				// If we have a range scan without proper upper bound, we must include
				// the matched conditions as filters since the scan will read extra rows
				if match.rangeCondition != nil && !match.hasProperUpperBound {
					// Keep ALL conditions (matched + unmatched) for filtering
					filters = append(filters, aCondition)
				} else if !match.matchedConditions[condIdx] {
					// Only keep unmatched conditions for filtering
					filters = append(filters, aCondition)
				}
			}

			if match.rangeCondition != nil {
				// Partial composite index match - use range scan
				aScan := Scan{
					Type:           ScanTypeIndexRange,
					IndexName:      match.info.Name,
					IndexColumns:   match.info.Columns,
					RangeCondition: *match.rangeCondition,
				}
				if len(filters) > 0 {
					aScan.Filters = OneOrMore{filters}
				}
				indexScans = append(indexScans, aScan)
			} else {
				// Full index match - use point scan
				aScan := Scan{
					Type:         ScanTypeIndexPoint,
					IndexName:    match.info.Name,
					IndexColumns: match.info.Columns,
					IndexKeys:    match.keys,
				}
				if len(filters) > 0 {
					aScan.Filters = OneOrMore{filters}
				}
				indexScans = append(indexScans, aScan)
			}

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

// incrementValue returns the next value after the given value for creating upper bounds in range scans.
// Returns nil if the value cannot be safely incremented (e.g., max value or unsupported type).
func incrementValue(val any) any {
	switch v := val.(type) {
	case int32:
		if v == (1<<31)-1 { // max int32
			return nil
		}
		return v + 1
	case int64:
		if v == (1<<63)-1 { // max int64
			return nil
		}
		return v + 1
	case float32:
		// For floats, we can't simply add 1 as we need the next representable value
		// For range scans on floats, we'll use exclusive upper bound instead
		return nil
	case float64:
		return nil
	case string:
		// For strings, append maximum byte value to get the upper bound for prefix matching
		// This ensures all strings starting with v are included in the range
		// Example: "Ralph" + "\xFF" > "Ralph Darren" because 0xFF > 0x44 ('D')
		return v + "\xFF"
	case bool:
		// Boolean can only be false or true, can't increment
		return nil
	default:
		return nil
	}
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
		if !ok || fieldName != indexInfo.Columns[0].Name {
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

		conditionValue, err := castKeyValue(indexInfo.Columns[0], aCondition.Operand2.Value)
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
		Type:           ScanTypeIndexRange,
		IndexName:      indexInfo.Name,
		IndexColumns:   indexInfo.Columns,
		RangeCondition: rangeCondition,
	}
	if len(remainingFilters) > 0 {
		aScan.Filters = OneOrMore{remainingFilters}
	}
	return aScan, true, nil
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

// FilterRow applies filtering on scanned rows according to filters
func (s Scan) FilterRow(aRow Row) (bool, error) {
	ok, err := aRow.CheckOneOrMore(s.Filters)
	if err != nil {
		return false, err
	}
	return ok, nil
}
