package minisql

import (
	"context"
	"fmt"
	"reflect"
)

// ScanType selects the execution strategy the query planner assigns to a Scan.
type ScanType int

// ScanType constants define the available index and table scan strategies.
const (
	// ScanTypeSequential is a full table scan.
	ScanTypeSequential ScanType = iota + 1
	// ScanTypeIndexAll is a full index scan.
	ScanTypeIndexAll
	// ScanTypeIndexPoint is an index lookup for specific key(s).
	ScanTypeIndexPoint
	// ScanTypeIndexRange is an index range scan.
	ScanTypeIndexRange
	// ScanTypeIndexFirst seeks to the first (smallest) key in the index — used for MIN optimisation.
	ScanTypeIndexFirst
	// ScanTypeIndexLast seeks to the last (largest) key in the index — used for MAX optimisation.
	ScanTypeIndexLast
	// ScanTypeIndexIntersect runs each sub-scan to collect RowID sets, intersects them in
	// memory, then fetches only the surviving rows.  Used for AND conditions where two or
	// more independent indexes are available.
	ScanTypeIndexIntersect
	// ScanTypeFullText runs an inverted full-text index lookup for MATCH predicates.
	ScanTypeFullText
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
	case ScanTypeIndexFirst:
		return "index_first"
	case ScanTypeIndexLast:
		return "index_last"
	case ScanTypeIndexIntersect:
		return "index_intersect"
	case ScanTypeFullText:
		return "fulltext"
	default:
		return "unknown"
	}
}

// RangeBound describes one end of a range scan condition, carrying the boundary
// value and whether the bound is inclusive (>= / <=) or exclusive (> / <).
type RangeBound struct {
	Value     any
	Inclusive bool // true for >= or <=, false for > or <
}

// RangeCondition holds the optional lower and upper bounds for a ScanTypeIndexRange scan.
// A nil bound means unbounded on that side.
type RangeCondition struct {
	Lower *RangeBound // nil = unbounded
	Upper *RangeBound // nil = unbounded
}

// JoinColumnPair represents a pair of columns used in a JOIN ON condition
type JoinColumnPair struct {
	BaseTableColumn Field
	JoinTableColumn Field
}

// JoinAlgorithm selects the execution strategy for a single join.
type JoinAlgorithm int

const (
	// JoinAlgorithmNestedLoop uses the existing nested-loop strategy: either an
	// index point-lookup on the inner table (when an index exists) or a full
	// sequential scan of the inner table for every outer row.
	JoinAlgorithmNestedLoop JoinAlgorithm = iota
	// JoinAlgorithmHash builds an in-memory hash table from the inner (build)
	// side once, then probes it for every outer row.  O(N+M) instead of O(N×M).
	// Only chosen when no index exists on the inner join column.
	JoinAlgorithmHash
)

// hashJoinMaxBuildRows is the maximum inner-table row count for which the
// planner chooses hash join.  Beyond this threshold the build-side hash table
// may consume excessive RAM, so we fall back to nested-loop.
const hashJoinMaxBuildRows = int64(1_000_000)

// JoinPlan represents a join operation between two scans
type JoinPlan struct {
	OuterJoinColumn string
	InnerJoinColumn string
	Conditions      Conditions
	JoinColumnPairs []JoinColumnPair
	Type            JoinType
	LeftScanIndex   int
	RightScanIndex  int
	Algorithm       JoinAlgorithm
}

// QueryPlan determines how to execute a query
type QueryPlan struct {
	Scans []Scan
	Joins []JoinPlan // JOIN operations to perform

	// Ordering
	OrderBy      []OrderBy
	SortInMemory bool
	SortReverse  bool
}

// Scan describes a single table or index scan operation within a QueryPlan.
// The Type field determines which execution path is used; the remaining fields
// supply the parameters (index name, key values, range bounds, filters, etc.)
// relevant to that scan type.
type Scan struct {
	RangeCondition RangeCondition
	TableName      string
	TableAlias     string
	IndexName      string
	IndexColumns   []Column
	IndexKeys      []any
	Filters        OneOrMore
	// SubScans holds the child index scans for ScanTypeIndexIntersect.
	// Each child is executed to collect RowIDs; surviving rows are fetched after intersection.
	SubScans      []Scan
	Type          ScanType
	CoveringIndex bool
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

Remember that if you have multiple conditions separated by OR, if a single one does not use
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
	// Handle multi-table queries (JOINs)
	if len(stmt.Joins) > 0 {
		return t.planJoinQuery(ctx, stmt)
	}

	// MIN/MAX index endpoint optimisation:
	// When the query is a single MIN(col) or MAX(col) with no WHERE clause and no GROUP BY,
	// and an index exists on that column, we can satisfy it by reading just the first or
	// last entry in the index — O(log n) instead of O(n).
	if plan, ok := t.tryMinMaxIndexPlan(stmt); ok {
		plan.markCoveringIndexes(stmt)
		return plan, nil
	}

	// Single table query - use existing logic
	plan := QueryPlan{
		Scans: []Scan{{
			TableName: t.Name,
			Type:      ScanTypeSequential,
			Filters:   stmt.Conditions,
		}},
		OrderBy: stmt.OrderBy,
	}

	// If there is no where clause, no need to consider index scans
	if len(stmt.Conditions) == 0 {
		// But we might still use index for ordering
		result := plan.optimizeOrdering(t, nil)
		result.markCoveringIndexes(stmt)
		return result, nil
	}

	// If there are no indexes, we cannot do index scans
	if t.HasNoIndex() {
		return plan.optimizeOrdering(t, stmt.Conditions), nil
	}

	if fullTextScan, ok := t.tryFullTextIndexScan(stmt.Conditions); ok {
		plan.Scans = []Scan{fullTextScan}
		result := plan.optimizeOrdering(t, stmt.Conditions)
		result.markCoveringIndexes(stmt)
		return result, nil
	}

	// Check if we can do an index scans
	if err := plan.setIndexScans(t, stmt.Conditions); err != nil {
		return QueryPlan{}, err
	}

	// But we might still use index for ordering
	// Pass original conditions so we can restore them if we switch indexes
	result := plan.optimizeOrdering(t, stmt.Conditions)
	result.markCoveringIndexes(stmt)
	return result, nil
}

// indexMatch represents a potential index match for equality conditions
type indexMatch struct {
	matchedConditions   map[int]bool
	rangeCondition      *RangeCondition
	stats               *IndexStats
	info                IndexInfo
	keys                []any
	hasProperUpperBound bool
	isPrimaryKey        bool
	isUnique            bool
}

// findBestEqualityIndexMatch finds the best index that can be used for equality conditions
// in the given group. It tries to match composite indexes by finding conditions on multiple
// columns that form a prefix of the index columns (e.g., for index on (a,b,c), it can match
// conditions on 'a', 'a AND b', or 'a AND b AND c', but not 'b' or 'a AND c').
//
// When statistics are available (from ANALYZE), uses selectivity to choose the best index.
// Otherwise falls back to: Primary Key > Unique Index > Secondary Index
// For composite indexes, prefer indexes that match more columns.
func (t *Table) findBestEqualityIndexMatch(group Conditions) *indexMatch {
	var bestMatch *indexMatch

	// Helper to check if a new match is better than the current best
	isBetterMatch := func(newMatch *indexMatch) bool {
		if bestMatch == nil {
			return true
		}

		// If both have stats, use selectivity-based comparison
		if newMatch.stats != nil && bestMatch.stats != nil {
			newSelectivity := newMatch.stats.Selectivity()
			bestSelectivity := bestMatch.stats.Selectivity()

			// Higher selectivity is better (more distinct values = more selective)
			if newSelectivity != bestSelectivity {
				return newSelectivity > bestSelectivity
			}

			// Same selectivity - prefer more matched columns
			newMatchedCols := len(newMatch.matchedConditions)
			bestMatchedCols := len(bestMatch.matchedConditions)
			if newMatchedCols != bestMatchedCols {
				return newMatchedCols > bestMatchedCols
			}

			// Same matched columns - prefer primary key, then unique
			if newMatch.isPrimaryKey && !bestMatch.isPrimaryKey {
				return true
			}
			if newMatch.isUnique && !bestMatch.isUnique {
				return true
			}
			return false
		}

		// Fall back to heuristic-based comparison when stats not available
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
	indexCap := len(t.UniqueIndexes) + len(t.SecondaryIndexes)
	if t.HasPrimaryKey() {
		indexCap += 1
	}
	allIndexes := make([]IndexInfo, 0, indexCap)
	if t.HasPrimaryKey() {
		allIndexes = append(allIndexes, t.PrimaryKey.IndexInfo)
	}
	for _, idx := range t.UniqueIndexes {
		allIndexes = append(allIndexes, idx.IndexInfo)
	}
	for _, idx := range t.SecondaryIndexes {
		if !idx.IsBTree() {
			continue
		}
		allIndexes = append(allIndexes, idx.IndexInfo)
	}

	// Try each index
	for _, indexInfo := range allIndexes {
		// Partial index: only use when the query's AND group syntactically implies
		// every condition in the index's WHERE predicate.
		if !partialIndexImplied(indexInfo.WhereCond, group) {
			continue
		}
		match := t.tryMatchIndex(indexInfo, group)
		if match != nil && isBetterMatch(match) {
			bestMatch = match
		}
	}

	// Try expression indexes for OperandExpr conditions (e.g. LOWER(email) = ?).
	for condIdx, cond := range group {
		if cond.Operand1.Type != OperandExpr {
			continue
		}
		if (cond.Operator != Eq && cond.Operator != In) || cond.Operand2.Type == OperandNull {
			continue
		}
		condExpr, ok := cond.Operand1.Value.(*Expr)
		if !ok {
			continue
		}
		si, found := t.FindExpressionIndex(condExpr)
		if !found {
			continue
		}
		if !partialIndexImplied(si.WhereCond, group) {
			continue
		}
		var keys []any
		if cond.Operator == Eq {
			key, err := castKeyValue(si.Columns[0], cond.Operand2.Value)
			if err != nil {
				continue
			}
			keys = []any{key}
		} else {
			rawKeys, ok2 := cond.Operand2.Value.([]any)
			if !ok2 {
				continue
			}
			for _, raw := range rawKeys {
				key, err := castKeyValue(si.Columns[0], raw)
				if err != nil {
					continue
				}
				keys = append(keys, key)
			}
			if len(keys) == 0 {
				continue
			}
		}
		exprMatch := &indexMatch{
			info:              si.IndexInfo,
			matchedConditions: map[int]bool{condIdx: true},
			keys:              keys,
		}
		if isBetterMatch(exprMatch) {
			bestMatch = exprMatch
		}
	}

	return bestMatch
}

func (t *Table) tryFullTextIndexScan(filters OneOrMore) (Scan, bool) {
	if len(filters) != 1 {
		return Scan{}, false
	}

	var matchCondIdx = -1
	var matchExpr *Expr
	for i, cond := range filters[0] {
		if cond.Operator != Eq || cond.Operand2.Type != OperandBoolean || cond.Operand2.Value != true {
			continue
		}
		if cond.Operand1.Type != OperandExpr {
			continue
		}
		expr, ok := cond.Operand1.Value.(*Expr)
		if !ok || expr.FuncName != "MATCH" || len(expr.Args) != 2 {
			continue
		}
		matchCondIdx = i
		matchExpr = expr
		break
	}
	if matchExpr == nil {
		return Scan{}, false
	}

	columnName := matchExpr.Args[0].Column
	if columnName == "" {
		return Scan{}, false
	}
	query, ok := literalText(matchExpr.Args[1])
	if !ok {
		return Scan{}, false
	}
	tokens := uniqueTextSearchTokens(query)
	if len(tokens) == 0 {
		return Scan{}, false
	}

	var matchedIndex SecondaryIndex
	foundIndex := false
	for _, idx := range t.SecondaryIndexes {
		if idx.Method != IndexMethodFullText || len(idx.Columns) != 1 {
			continue
		}
		if idx.Columns[0].Name == columnName {
			matchedIndex = idx
			foundIndex = true
			break
		}
	}
	if !foundIndex || !partialIndexImplied(matchedIndex.WhereCond, filters[0]) {
		return Scan{}, false
	}

	remaining := make(Conditions, 0, len(filters[0]))
	for i, cond := range filters[0] {
		if i == matchCondIdx {
			continue
		}
		remaining = append(remaining, cond)
	}
	scanFilters := OneOrMore{remaining}
	if len(remaining) == 0 {
		scanFilters = nil
	}

	indexKeys := make([]any, len(tokens))
	for i, token := range tokens {
		indexKeys[i] = token
	}
	return Scan{
		TableName:    t.Name,
		Type:         ScanTypeFullText,
		IndexName:    matchedIndex.Name,
		IndexColumns: []Column{fullTextTokenColumn()},
		IndexKeys:    indexKeys,
		Filters:      scanFilters,
	}, true
}

func literalText(expr *Expr) (string, bool) {
	if expr == nil {
		return "", false
	}
	tp, ok := expr.Literal.(TextPointer)
	if !ok {
		return "", false
	}
	return tp.String(), true
}

// tryMatchIndex attempts to match an index against the conditions in a group.
// It returns a match if it can find equality conditions for a prefix of the index columns.
func (t *Table) tryMatchIndex(indexInfo IndexInfo, group Conditions) *indexMatch {
	matchedConditions := make(map[int]bool)
	var compositeKey []any

	// For composite indexes, we need to match columns in order from left to right
	for colIdx, indexCol := range indexInfo.Columns {
		foundMatch := false

		// Look for an equality condition on this column
		for condIdx, cond := range group {
			// Skip already matched conditions
			if matchedConditions[condIdx] {
				continue
			}

			// Check if this is an equality condition on the current index column
			if !isEquality(cond) {
				continue
			}

			if cond.Operand1.Type != OperandField {
				continue
			}

			field, ok := cond.Operand1.Value.(Field)
			if !ok || field.Name != indexCol.Name {
				continue
			}

			// Skip NULL comparisons
			if cond.Operand2.Type == OperandNull {
				continue
			}

			// We found a match for this column
			column, ok := t.ColumnByName(field.Name)
			if !ok {
				continue
			}

			keys, err := equalityKeys(column, cond)
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

	switch {
	case isPartialMatch:
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
	case numMatchedColumns > 1:
		// Full composite index match - use point scan
		// Create composite key with all matched columns
		matchedCols := indexInfo.Columns[:numMatchedColumns]
		indexKeys = []any{NewCompositeKey(matchedCols, compositeKey...)}
	default:
		// Single column index - use raw key value(s)
		indexKeys = compositeKey
	}

	match := &indexMatch{
		info:                indexInfo,
		matchedConditions:   matchedConditions,
		keys:                indexKeys,
		rangeCondition:      rangeCondition,
		hasProperUpperBound: hasProperUpperBound,
		isPrimaryKey:        isPK,
		isUnique:            isUnique,
	}

	// Load statistics for this index if available
	stats, hasStats := t.indexStats[indexInfo.Name]
	if hasStats {
		match.stats = &stats
	}

	return match
}

// shouldUseIntersection reports whether multi-index intersection is worth attempting for the
// given primary match.  Unique / PK single-key point lookups already return ≤ 1 row, so
// paying the intersection overhead yields no benefit.
func shouldUseIntersection(match *indexMatch) bool {
	if match.rangeCondition != nil {
		// Range scan as primary — intersection can reduce the result set further.
		return true
	}
	// Single-key PK or unique point lookup returns at most one row.
	if (match.isUnique || match.isPrimaryKey) && len(match.keys) == 1 {
		return false
	}
	return true
}

// buildSubScanFromMatch constructs the Scan for the primary match (no post-filters; those
// are handled at the parent intersection level).
func buildSubScanFromMatch(tableName string, match *indexMatch) Scan {
	if match.rangeCondition != nil {
		return Scan{
			TableName:      tableName,
			Type:           ScanTypeIndexRange,
			IndexName:      match.info.Name,
			IndexColumns:   match.info.Columns,
			RangeCondition: *match.rangeCondition,
		}
	}
	return Scan{
		TableName:    tableName,
		Type:         ScanTypeIndexPoint,
		IndexName:    match.info.Name,
		IndexColumns: match.info.Columns,
		IndexKeys:    match.keys,
	}
}

// conditionsForColumn returns conditions whose left operand is the named field.
func conditionsForColumn(group Conditions, colName string) Conditions {
	var result Conditions
	for _, cond := range group {
		if cond.Operand1.Type != OperandField {
			continue
		}
		f, ok := cond.Operand1.Value.(Field)
		if !ok || f.Name != colName {
			continue
		}
		result = append(result, cond)
	}
	return result
}

// findAdditionalIndexScans looks for index scans for conditions in group that are NOT
// already covered by primaryMatch.  It returns the extra sub-scans and a set of covered
// condition indices (indices into group).  Each additional index is used at most once.
func (t *Table) findAdditionalIndexScans(group Conditions, primaryMatch *indexMatch) ([]Scan, map[int]bool) {
	var scans []Scan
	covered := make(map[int]bool)
	usedIndexes := map[string]bool{primaryMatch.info.Name: true}

	for condIdx, cond := range group {
		if primaryMatch.matchedConditions[condIdx] {
			continue
		}
		if cond.Operand1.Type != OperandField {
			continue
		}
		field, ok := cond.Operand1.Value.(Field)
		if !ok {
			continue
		}
		if !t.HasIndexOnColumn(field.Name) {
			continue
		}
		idxInfo, ok := t.IndexInfoByColumnName(field.Name)
		if !ok || usedIndexes[idxInfo.Name] {
			continue
		}

		if isEquality(cond) && cond.Operand2.Type != OperandNull {
			col, ok := t.ColumnByName(field.Name)
			if !ok {
				continue
			}
			keys, err := equalityKeys(col, cond)
			if err != nil {
				continue
			}
			scans = append(scans, Scan{
				TableName:    t.Name,
				Type:         ScanTypeIndexPoint,
				IndexName:    idxInfo.Name,
				IndexColumns: idxInfo.Columns,
				IndexKeys:    keys,
			})
			covered[condIdx] = true
			usedIndexes[idxInfo.Name] = true
			continue
		}

		// Try a range scan using only the conditions for this column.
		colConds := conditionsForColumn(group, field.Name)
		if len(colConds) == 0 {
			continue
		}
		var stats *IndexStats
		if s, ok := t.indexStats[idxInfo.Name]; ok {
			stats = &s
		}
		rangeScan, built, err := tryRangeScan(t.Name, idxInfo, colConds, stats)
		if err != nil || !built {
			continue
		}
		rangeScan.Filters = nil // The intersection parent handles remaining filters.
		scans = append(scans, rangeScan)
		// Mark all conditions for this column as covered.
		for ci, c := range group {
			if c.Operand1.Type == OperandField {
				if f, ok2 := c.Operand1.Value.(Field); ok2 && f.Name == field.Name {
					covered[ci] = true
				}
			}
		}
		usedIndexes[idxInfo.Name] = true
	}

	return scans, covered
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
		match := t.findBestEqualityIndexMatch(group)
		if match != nil {
			equalityMatch[groupIdx] = match
		}

		// Also collect indexes that could be used for range scans
		for _, cond := range group {
			if cond.Operand1.Type != OperandField {
				continue
			}
			field, ok := cond.Operand1.Value.(Field)
			if !ok {
				continue
			}
			if !t.HasIndexOnColumn(field.Name) {
				continue
			}

			info, ok := t.IndexInfoByColumnName(field.Name)
			if !ok {
				continue
			}

			// Skip if this condition was already matched for equality
			if match != nil {
				alreadyMatched := false
				for condIdx := range match.matchedConditions {
					if &group[condIdx] == &cond {
						alreadyMatched = true
						break
					}
				}
				if alreadyMatched {
					continue
				}
			}

			// Check if this could be used for range scan
			isEq := isEquality(cond) && cond.Operand2.Type != OperandNull
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
		// Try equality-based index first (point scan or partial composite range scan).
		match, ok := equalityMatch[groupIdx]
		if ok {
			// Try multi-index intersection when the primary match is not already a
			// single-row lookup (unique / PK with one key).
			if shouldUseIntersection(match) {
				additionalScans, additionalCovered := t.findAdditionalIndexScans(group, match)
				if len(additionalScans) > 0 {
					primarySubScan := buildSubScanFromMatch(t.Name, match)
					subScans := append([]Scan{primarySubScan}, additionalScans...)

					// Collect the complete set of covered condition indices.
					allCovered := make(map[int]bool)
					for k := range match.matchedConditions {
						allCovered[k] = true
					}
					for k := range additionalCovered {
						allCovered[k] = true
					}

					// Any condition not covered by any sub-scan becomes a post-filter.
					var remaining Conditions
					for condIdx, cond := range group {
						if !allCovered[condIdx] {
							remaining = append(remaining, cond)
						}
					}

					intersectScan := Scan{
						TableName: t.Name,
						Type:      ScanTypeIndexIntersect,
						SubScans:  subScans,
					}
					if len(remaining) > 0 {
						intersectScan.Filters = OneOrMore{remaining}
					}
					indexScans = append(indexScans, intersectScan)
					continue
				}
			}

			// Single-index path (existing behaviour).
			filters := make(Conditions, 0, len(group))
			for condIdx, cond := range group {
				// If we have a range scan without proper upper bound, we must include
				// the matched conditions as filters since the scan will read extra rows.
				if match.rangeCondition != nil && !match.hasProperUpperBound {
					filters = append(filters, cond)
				} else if !match.matchedConditions[condIdx] {
					filters = append(filters, cond)
				}
			}

			if match.rangeCondition != nil {
				scan := Scan{
					TableName:      t.Name,
					Type:           ScanTypeIndexRange,
					IndexName:      match.info.Name,
					IndexColumns:   match.info.Columns,
					RangeCondition: *match.rangeCondition,
				}
				if len(filters) > 0 {
					scan.Filters = OneOrMore{filters}
				}
				indexScans = append(indexScans, scan)
			} else {
				scan := Scan{
					TableName:    t.Name,
					Type:         ScanTypeIndexPoint,
					IndexName:    match.info.Name,
					IndexColumns: match.info.Columns,
					IndexKeys:    match.keys,
				}
				if len(filters) > 0 {
					scan.Filters = OneOrMore{filters}
				}
				indexScans = append(indexScans, scan)
			}

			continue
		}

		// No equality match — try range scans.  When two or more indexes are
		// available for different columns, build an intersection scan.
		foundRangeScan := false
		var (
			rangeSubScans    []Scan
			usedRangeIndexes = make(map[string]bool)
		)
		for _, idxInfo := range otherIndexes[groupIdx] {
			if usedRangeIndexes[idxInfo.Name] {
				continue
			}
			var stats *IndexStats
			if indexStats, hasStats := t.indexStats[idxInfo.Name]; hasStats {
				stats = &indexStats
			}
			colConds := conditionsForColumn(group, idxInfo.Columns[0].Name)
			if len(colConds) == 0 {
				continue
			}
			rangeScan, built, err := tryRangeScan(t.Name, idxInfo, colConds, stats)
			if err != nil {
				return err
			}
			if built {
				rangeScan.Filters = nil // Intersection parent handles remaining filters.
				rangeSubScans = append(rangeSubScans, rangeScan)
				usedRangeIndexes[idxInfo.Name] = true
			}
		}

		switch {
		case len(rangeSubScans) >= 2:
			// Multi-range intersection.
			covered := make(map[int]bool)
			for _, rs := range rangeSubScans {
				colName := rs.IndexColumns[0].Name
				for condIdx, cond := range group {
					if cond.Operand1.Type == OperandField {
						if f, ok2 := cond.Operand1.Value.(Field); ok2 && f.Name == colName {
							covered[condIdx] = true
						}
					}
				}
			}
			var remaining Conditions
			for condIdx, cond := range group {
				if !covered[condIdx] {
					remaining = append(remaining, cond)
				}
			}
			intersectScan := Scan{
				TableName: t.Name,
				Type:      ScanTypeIndexIntersect,
				SubScans:  rangeSubScans,
			}
			if len(remaining) > 0 {
				intersectScan.Filters = OneOrMore{remaining}
			}
			indexScans = append(indexScans, intersectScan)
			foundRangeScan = true

		case len(rangeSubScans) == 1:
			// Single range scan — re-run with the full group so remaining conditions
			// are captured as post-filters (original behaviour).
			idxInfo := otherIndexes[groupIdx][0]
			var stats *IndexStats
			if indexStats, hasStats := t.indexStats[idxInfo.Name]; hasStats {
				stats = &indexStats
			}
			rangeScan, built, err := tryRangeScan(t.Name, idxInfo, group, stats)
			if err != nil {
				return err
			}
			if built {
				indexScans = append(indexScans, rangeScan)
				foundRangeScan = true
			}
		}

		if foundRangeScan {
			continue
		}

		// Otherwise fall back to sequential scan for this group.
		indexScans = append(indexScans, Scan{
			TableName: t.Name,
			Type:      ScanTypeSequential,
			Filters:   OneOrMore{group},
		})
	}

	// Only override the default plan if at least one scan uses a real index.
	// When all groups fall back to sequential scan (no index usable), keep the
	// default single sequential scan which already holds the full DNF filter.
	hasRealIndexScan := false
	for _, scan := range indexScans {
		if scan.Type != ScanTypeSequential {
			hasRealIndexScan = true
			break
		}
	}
	if len(indexScans) > 0 && hasRealIndexScan {
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

func equalityKeys(col Column, cond Condition) ([]any, error) {
	if cond.Operator == Eq {
		if cond.Operand2.Type == OperandNull {
			return []any{cond.Operand2.Value}, nil
		}
		keyValue, err := castKeyValue(col, cond.Operand2.Value)
		if err != nil {
			return nil, err
		}
		return []any{keyValue}, nil
	}

	// TODO what if NULL is included in list?
	keyValues := make([]any, 0, len(cond.Operand2.Value.([]any)))
	for _, rawValue := range cond.Operand2.Value.([]any) {
		keyValue, err := castKeyValue(col, rawValue)
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

func tryRangeScan(tableName string, indexInfo IndexInfo, filters Conditions, stats *IndexStats) (Scan, bool, error) {
	var (
		rangeCondition   = RangeCondition{}
		remainingFilters = make(Conditions, 0)
	)

	// Scan conditions to find range predicates on PK
	for _, cond := range filters {
		// Left side operand must be a field
		if cond.Operand1.Type != OperandField {
			remainingFilters = append(remainingFilters, cond)
			continue
		}

		field, ok := cond.Operand1.Value.(Field)
		if !ok || field.Name != indexInfo.Columns[0].Name {
			remainingFilters = append(remainingFilters, cond)
			continue
		}

		// Can't use index if comparing to another field
		if cond.Operand2.Type == OperandField {
			remainingFilters = append(remainingFilters, cond)
			continue
		}

		if cond.Operator == Eq || cond.Operator == Ne {
			// id = X, id IN (...) - we will be doing index point scan instead so just return
			return Scan{}, false, nil
		}

		if cond.Operator == In || cond.Operator == NotIn {
			// id != X , id NOT IN (...) - we will be doing a sequential scan so just return
			return Scan{}, false, nil
		}

		if cond.Operator == Like || cond.Operator == NotLike {
			// LIKE / NOT LIKE requires a full sequential scan — no range bound possible
			return Scan{}, false, nil
		}

		if cond.Operator == Between || cond.Operator == NotBetween {
			// BETWEEN / NOT BETWEEN uses sequential scan for now
			return Scan{}, false, nil
		}

		conditionValue, err := castKeyValue(indexInfo.Columns[0], cond.Operand2.Value)
		if err != nil {
			return Scan{}, false, err
		}

		switch cond.Operator {
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
			return Scan{}, false, fmt.Errorf("invalid operator for range scan: %d", cond.Operator)
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

	// Check if index scan is cost-effective based on statistics
	if !shouldUseIndexForRange(stats, rangeCondition) {
		// Table scan would be more efficient - don't use this index
		return Scan{}, false, nil
	}

	// Create range scan plan
	scan := Scan{
		TableName:      tableName,
		Type:           ScanTypeIndexRange,
		IndexName:      indexInfo.Name,
		IndexColumns:   indexInfo.Columns,
		RangeCondition: rangeCondition,
	}
	if len(remainingFilters) > 0 {
		scan.Filters = OneOrMore{remainingFilters}
	}
	return scan, true, nil
}

// Execute runs the query plan, calling out for every row produced.
// out receives each row synchronously on the caller's goroutine; no internal
// goroutines or channels are created.  JOIN queries are the exception: they
// still drive executeNestedLoopJoin in a helper goroutine internally so that
// the nested-loop logic can use a channel, but the rows are forwarded to out
// before Execute returns.
func (p QueryPlan) Execute(ctx context.Context, provider TableProvider, selectedFields []Field, out func(Row) error) error {
	// Handle JOIN queries: executeNestedLoopJoin still uses chan<- Row internally,
	// so we bridge it with a goroutine + channel and forward rows to the callback.
	if len(p.Joins) > 0 {
		ch := make(chan Row, 128)
		var joinErr error
		go func() {
			defer close(ch)
			joinErr = p.executeNestedLoopJoin(ctx, provider, selectedFields, ch)
		}()
		for row := range ch {
			if err := out(row); err != nil {
				return err
			}
		}
		return joinErr
	}

	if len(p.Scans) == 1 {
		t, ok := provider.GetTable(ctx, p.Scans[0].TableName)
		if !ok {
			return fmt.Errorf("%w: %s", errTableDoesNotExist, p.Scans[0].TableName)
		}

		switch p.Scans[0].Type {
		case ScanTypeIndexAll:
			return t.indexScanAll(ctx, p, p.Scans[0], selectedFields, out)
		case ScanTypeIndexRange:
			return t.indexRangeScan(ctx, p, p.Scans[0], selectedFields, out)
		case ScanTypeIndexPoint:
			return t.indexPointScan(ctx, p.Scans[0], selectedFields, out)
		case ScanTypeIndexFirst:
			return t.indexEndpointScan(ctx, p.Scans[0], selectedFields, out, false)
		case ScanTypeIndexLast:
			return t.indexEndpointScan(ctx, p.Scans[0], selectedFields, out, true)
		case ScanTypeIndexIntersect:
			return t.indexIntersectScan(ctx, p.Scans[0], selectedFields, out)
		case ScanTypeFullText:
			return t.fullTextIndexScan(ctx, p.Scans[0], selectedFields, out)
		case ScanTypeSequential:
			return t.sequentialScan(ctx, p.Scans[0], selectedFields, out)
		default:
			return fmt.Errorf("unhandled scan type in single scan: %d", p.Scans[0].Type)
		}
	}
	for _, scan := range p.Scans {
		t, ok := provider.GetTable(ctx, scan.TableName)
		if !ok {
			return fmt.Errorf("%w: %s", errTableDoesNotExist, scan.TableName)
		}

		switch scan.Type {
		case ScanTypeIndexRange:
			if err := t.indexRangeScan(ctx, p, scan, selectedFields, out); err != nil {
				return err
			}
		case ScanTypeIndexPoint:
			if err := t.indexPointScan(ctx, scan, selectedFields, out); err != nil {
				return err
			}
		case ScanTypeIndexIntersect:
			if err := t.indexIntersectScan(ctx, scan, selectedFields, out); err != nil {
				return err
			}
		case ScanTypeFullText:
			if err := t.fullTextIndexScan(ctx, scan, selectedFields, out); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unhandled scan type in multi scan: %d", scan.Type)
		}
	}
	return nil
}

// tryMinMaxIndexPlan attempts to use an index endpoint scan for a query of the
// form SELECT MIN(col) FROM t  or  SELECT MAX(col) FROM t  with no WHERE clause
// and no GROUP BY.  Returns the plan and true if the optimisation applies.
func (t *Table) tryMinMaxIndexPlan(stmt Statement) (QueryPlan, bool) {
	// Must be a single aggregate, MIN or MAX, with no WHERE clause and no GROUP BY.
	if len(stmt.GroupBy) != 0 {
		return QueryPlan{}, false
	}
	if len(stmt.Aggregates) != 1 {
		return QueryPlan{}, false
	}
	agg := stmt.Aggregates[0]
	if agg.Kind != AggregateMin && agg.Kind != AggregateMax {
		return QueryPlan{}, false
	}
	if len(stmt.Conditions) != 0 {
		return QueryPlan{}, false
	}
	if agg.Column == "" {
		return QueryPlan{}, false
	}

	info, ok := t.IndexInfoByColumnName(agg.Column)
	if !ok {
		return QueryPlan{}, false
	}

	scanType := ScanTypeIndexFirst
	if agg.Kind == AggregateMax {
		scanType = ScanTypeIndexLast
	}

	return QueryPlan{
		Scans: []Scan{{
			TableName:    t.Name,
			Type:         scanType,
			IndexName:    info.Name,
			IndexColumns: info.Columns,
		}},
	}, true
}

// partialIndexImplied returns true when the query's AND group syntactically
// implies the partial index predicate. Uses SQLite-style conservative check:
// the index WHERE must be a single AND group and every term of that group must
// appear verbatim in the query's condition group. Full indexes always return true.
func partialIndexImplied(indexWhereCond OneOrMore, queryGroup Conditions) bool {
	if len(indexWhereCond) == 0 {
		return true
	}
	// Only handle single-conjunct (no OR) index predicates conservatively.
	if len(indexWhereCond) > 1 {
		return false
	}
	for _, indexCond := range indexWhereCond[0] {
		found := false
		for _, queryCond := range queryGroup {
			if conditionEqual(indexCond, queryCond) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func conditionEqual(a, b Condition) bool {
	return a.Operator == b.Operator &&
		operandEqual(a.Operand1, b.Operand1) &&
		operandEqual(a.Operand2, b.Operand2)
}

func operandEqual(a, b Operand) bool {
	return a.Type == b.Type && reflect.DeepEqual(a.Value, b.Value)
}
