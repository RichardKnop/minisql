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
func (t *Table) PlanQuery(ctx context.Context, stmt Statement) QueryPlan {
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
		return plan.optimizeOrdering(t)
	}

	// If there is no primary key, we cannot do index scans
	// TODO - consider secondary indexes later
	if !t.HasPrimaryKey() {
		// But we might still use index for ordering
		return plan.optimizeOrdering(t)
	}

	// Check if we can do an index scan using the primary key
	plan = plan.setPKIndexScans(
		t.PrimaryKey.Name,
		t.PrimaryKey.Column.Name,
		stmt.Conditions,
	)

	// But we might still use index for ordering
	return plan.optimizeOrdering(t)
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
		if t.HasPrimaryKey() && orderCol == t.PrimaryKey.Column.Name {
			// Use PK index for ordering
			p.Scans[0].Type = ScanTypeIndexAll
			p.Scans[0].IndexName = t.PrimaryKey.Name
			p.Scans[0].IndexColumnName = orderCol
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
func (p QueryPlan) setPKIndexScans(pkName string, pkColumn string, conditions OneOrMore) QueryPlan {
	var (
		allGroupsHavePKCondition = true
		primaryKeys              = make([][]any, 0, 10)
		remaining                = make([]Conditions, 0, len(conditions))
	)

	// Each group is separated by OR, for example:
	// (a = 1 AND b = 2) OR (a = 3 AND b = 4)
	// would be 2 groups with 2 conditions each
	for _, group := range conditions {
		// Check if this group contains a primary key condition
		var (
			hasPKCondition      = false
			remainingForGroup   = make(Conditions, 0, 10)
			primaryKeysForGroup = make([]any, 0, 10)
		)
		for _, aCondition := range group {
			if !isPrimaryKey(aCondition, pkColumn) {
				remainingForGroup = append(remainingForGroup, aCondition)
				continue
			}
			hasPKCondition = true
			keys, ok := isEquality(aCondition)
			if !ok {
				remainingForGroup = append(remainingForGroup, aCondition)
				continue
			}
			primaryKeysForGroup = append(primaryKeysForGroup, keys...)
		}
		if !hasPKCondition {
			allGroupsHavePKCondition = false
			break
		}
		remaining = append(remaining, remainingForGroup)
		primaryKeys = append(primaryKeys, primaryKeysForGroup)
	}

	// In case all groups don't contain a primary key condition, we cannot do index scan
	// since we would need to do a full table scan anyway for the group without PK condition.
	if !allGroupsHavePKCondition {
		return p
	}

	// In case we reach here, we can do index scans on primary key.
	// We need to check if we can do range scans instead of point lookups.
	p.Scans = make([]Scan, 0, len(conditions))
	for groupIdx, group := range conditions {
		rangeScan, ok := tryRangeScan(pkName, pkColumn, group)
		if ok {
			p.Scans = append(p.Scans, rangeScan)
			continue
		}

		p.Scans = append(p.Scans, Scan{
			Type:            ScanTypeIndexPoint,
			IndexName:       pkName,
			IndexColumnName: pkColumn,
			IndexKeys:       primaryKeys[groupIdx],
			Filters:         OneOrMore{remaining[groupIdx]},
		})
	}

	return p
}

func isPrimaryKey(cond Condition, pkColumn string) bool {
	fieldName, ok := cond.Operand1.Value.(string)
	return ok && fieldName == pkColumn
}

func isEquality(cond Condition) ([]any, bool) {
	// Check: column_name = literal_value
	// Also consider IN operator for primary key
	if cond.Operator != Eq && cond.Operator != In {
		return nil, false
	}

	if cond.Operand1.Type != OperandField {
		return nil, false
	}

	// Right operand must be a literal (not another field)
	if cond.Operand2.Type == OperandField {
		return nil, false
	}

	if cond.Operator == Eq {
		return []any{cond.Operand2.Value}, true
	}

	return cond.Operand2.Value.([]any), true
}

func tryRangeScan(pkName string, keyColumn string, filters Conditions) (Scan, bool) {
	var (
		rangeCondition      = RangeCondition{}
		remainingFilters    = make(Conditions, 0)
		foundRangeCondition = false
	)

	// Scan conditions to find range predicates on PK
	for _, aCondition := range filters {
		if aCondition.Operand1.Type != OperandField {
			remainingFilters = append(remainingFilters, aCondition)
			continue
		}

		fieldName, ok := aCondition.Operand1.Value.(string)
		if !ok || fieldName != keyColumn {
			remainingFilters = append(remainingFilters, aCondition)
			continue
		}

		// Can't use index if comparing to another field
		if aCondition.Operand2.Type == OperandField {
			remainingFilters = append(remainingFilters, aCondition)
			continue
		}

		// Key column condition - check if it's a range operator
		foundRangeCondition = true

		switch aCondition.Operator {
		case Gt:
			// id > X
			if rangeCondition.Lower == nil ||
				compareAny(aCondition.Operand2.Value, rangeCondition.Lower.Value) > 0 {
				rangeCondition.Lower = &RangeBound{
					Value:     aCondition.Operand2.Value,
					Inclusive: false,
				}
			}
		case Gte:
			// id >= X
			if rangeCondition.Lower == nil ||
				compareAny(aCondition.Operand2.Value, rangeCondition.Lower.Value) > 0 {
				rangeCondition.Lower = &RangeBound{
					Value:     aCondition.Operand2.Value,
					Inclusive: true,
				}
			}
		case Lt:
			// id < X
			if rangeCondition.Upper == nil ||
				compareAny(aCondition.Operand2.Value, rangeCondition.Upper.Value) < 0 {
				rangeCondition.Upper = &RangeBound{
					Value:     aCondition.Operand2.Value,
					Inclusive: false,
				}
			}
		case Lte:
			// id <= X
			if rangeCondition.Upper == nil ||
				compareAny(aCondition.Operand2.Value, rangeCondition.Upper.Value) < 0 {
				rangeCondition.Upper = &RangeBound{
					Value:     aCondition.Operand2.Value,
					Inclusive: true,
				}
			}
		case Eq, In:
			// id = X (we will be doing index point scan instead so just return)
			return Scan{}, false
		default:
			// Other operators don't support range scan
			remainingFilters = append(remainingFilters, aCondition)
		}
	}

	if !foundRangeCondition {
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
		IndexName:       pkName,
		IndexColumnName: keyColumn,
		RangeCondition:  rangeCondition,
	}
	if len(remainingFilters) > 0 {
		aScan.Filters = OneOrMore{remainingFilters}
	}
	return aScan, true
}
