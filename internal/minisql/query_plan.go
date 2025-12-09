package minisql

import (
	"context"
)

type ScanType int

const (
	ScanTypeSequential ScanType = iota + 1 // Full table scan
	ScanTypeIndexPoint                     // Index lookup for specific key(s)
	ScanTypeIndexRange                     // Index range scan (TODO - not implemented yet)
)

func (st ScanType) String() string {
	switch st {
	case ScanTypeSequential:
		return "sequential"
	case ScanTypeIndexPoint:
		return "index_point"
	case ScanTypeIndexRange:
		return "index_range"
	default:
		return "unknown"
	}
}

// QueryPlan determines how to execute a query
type QueryPlan struct {
	ScanType        ScanType
	IndexName       string
	IndexColumnName string
	IndexKeyGroups  [][]any     // Keys to lookup in index
	Filters         OneOrMore   // Additional filters to apply
	KeyFiltersMap   map[any]int // Map of keys to filter group index
}

func (p QueryPlan) IsIndexScan() bool {
	return p.ScanType == ScanTypeIndexPoint
}

// FilterRow applies the query plan filters to the given row
func (p QueryPlan) FilterRow(aRow Row) (bool, error) {
	if !p.IsIndexScan() && len(p.Filters) == 0 {
		return true, nil
	}
	var (
		ok  bool
		err error
	)
	if p.IsIndexScan() {
		pkValue, _ := aRow.GetValue(p.IndexColumnName)
		ok, err = aRow.CheckConditions(p.Filters[p.KeyFiltersMap[pkValue.Value]])
	} else {
		ok, err = aRow.CheckOneOrMore(p.Filters)
	}
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return true, nil
}

// PlanQuery creates a query plan based on the statement and table schema
func (t *Table) PlanQuery(ctx context.Context, stmt Statement) QueryPlan {
	plan := QueryPlan{
		ScanType: ScanTypeSequential,
		Filters:  stmt.Conditions,
	}

	// No WHERE clause = sequential scan
	if len(stmt.Conditions) == 0 {
		return plan
	}

	// No primary key = sequential scan
	if !t.HasPrimaryKey() {
		return plan
	}

	// Check if we can do an index scan using the primary key
	plan = plan.setPKIndexScan(
		t.PrimaryKey.Name,
		t.PrimaryKey.Column.Name,
		stmt.Conditions,
	)

	return plan
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
func (p QueryPlan) setPKIndexScan(pkName string, pkColumn string, conditions OneOrMore) QueryPlan {
	var (
		primaryKeys              = make([][]any, 0, 10)
		remaining                = make([]Conditions, 0, len(conditions))
		keyFiltersMap            = make(map[any]int)
		allGroupsHavePKCondition = true
	)

	for groupIdx, group := range conditions {
		// Check if this group contains only a primary key equality condition
		var (
			hasPKCondition      = false
			remainingForGroup   = make(Conditions, 0, 10)
			primaryKeysForGroup = make([]any, 0, 10)
		)
		for _, aCondition := range group {
			keys, ok := isPrimaryKeyEquality(aCondition, pkColumn)
			if ok {
				hasPKCondition = true
				primaryKeysForGroup = append(primaryKeysForGroup, keys...)
				for _, aKey := range keys {
					keyFiltersMap[aKey] = groupIdx
				}
				continue
			}
			remainingForGroup = append(remainingForGroup, aCondition)
		}
		if !hasPKCondition {
			allGroupsHavePKCondition = false
			break
		}
		remaining = append(remaining, remainingForGroup)
		primaryKeys = append(primaryKeys, primaryKeysForGroup)
	}

	if allGroupsHavePKCondition {
		// Can use primary key index
		p.IndexName = pkName
		p.IndexColumnName = pkColumn
		p.IndexKeyGroups = primaryKeys
		p.ScanType = ScanTypeIndexPoint
		p.Filters = remaining
		p.KeyFiltersMap = keyFiltersMap
	}

	return p
}

func isPrimaryKeyEquality(cond Condition, pkColumn string) ([]any, bool) {
	// Check: column_name = literal_value
	// Also consider IN operator for primary key
	if cond.Operator != Eq && cond.Operator != In {
		return nil, false
	}

	if cond.Operand1.Type != OperandField {
		return nil, false
	}

	fieldName, ok := cond.Operand1.Value.(string)
	if !ok || fieldName != pkColumn {
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
