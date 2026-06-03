package minisql

import (
	"context"
	"errors"
	"fmt"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

// joinMemo holds precomputed, per-join-level state that is constant across all
// outer rows. Computing these once in executeNestedLoopJoin instead of per outer
// row eliminates repeated slice allocations in the hot join path.
type joinMemo struct {
	innerTable      *Table
	innerFields     []Field
	innerAllMask    []bool   // all-true mask for full inner-row materialisation via RowView
	combinedColumns []Column // alias-prefixed combined columns for this join level
	joinFilter      func(Row) (bool, error)
	nullInnerCount  int // len(innerTable.Columns), for LEFT/FULL OUTER null padding

	// Pre-computed for index point lookup hot path (avoids per-outer-row allocations).
	singleKeySlice      []any                   // reusable 1-element key slice for single-column joins
	innerIndex          BTreeIndex              // resolved index for point lookups (avoids per-row IndexByName)
	innerSelectedMask   []bool                  // selectedColumnsMask for inner table
	innerNSelected      int                     // len(innerFields)
	innerTableFilter    func(Row) (bool, error) // compileScanFilter for inner table rows
	innerCoveringFilter func(Row) (bool, error) // compileScanFilter for covering index rows
	innerCovering       bool                    // whether inner scan is a covering index
	innerIndexCols      []Column                // index columns for inner covering scan
}

// buildCombinedColumns returns the alias-prefixed column list for the first join
// level (joinIndex == 0).
func buildCombinedColumns(outerCols []Column, outerAlias string, innerCols []Column, innerAlias string) []Column {
	combined := make([]Column, 0, len(outerCols)+len(innerCols))
	for _, col := range outerCols {
		c := col
		if outerAlias != "" {
			c.Name = outerAlias + "." + col.Name
		}
		combined = append(combined, c)
	}
	for _, col := range innerCols {
		c := col
		c.Name = innerAlias + "." + col.Name
		combined = append(combined, c)
	}
	return combined
}

// buildCombinedColumnsProgressive appends alias-prefixed inner columns to an
// already-combined outer column list (joinIndex > 0).
func buildCombinedColumnsProgressive(existingCols, innerCols []Column, innerAlias string) []Column {
	combined := make([]Column, 0, len(existingCols)+len(innerCols))
	combined = append(combined, existingCols...)
	for _, col := range innerCols {
		c := col
		c.Name = innerAlias + "." + col.Name
		combined = append(combined, c)
	}
	return combined
}

// combineRowsWithSchema concatenates outer and inner row values using a
// precomputed combined column slice. Avoids the per-row Column slice allocation
// that combineRows performs.
func combineRowsWithSchema(outerRow, innerRow Row, combinedColumns []Column) Row {
	combined := make([]OptionalValue, len(outerRow.Values)+len(innerRow.Values))
	copy(combined, outerRow.Values)
	copy(combined[len(outerRow.Values):], innerRow.Values)
	return NewRowWithValues(combinedColumns, combined)
}

// combineRowWithNullInner combines outerRow with nullCount NULL-valued inner
// columns using a precomputed combined column schema. Avoids a separate
// nullRowForColumns allocation for the LEFT/FULL OUTER JOIN miss path.
func combineRowWithNullInner(outerRow Row, nullCount int, combinedColumns []Column) Row {
	combined := make([]OptionalValue, len(outerRow.Values)+nullCount)
	copy(combined, outerRow.Values)
	return NewRowWithValues(combinedColumns, combined)
}

// chanRowCallback wraps a buffered channel as a row callback.
// The goroutine sending to the channel must close it when done.
func chanRowCallback(ctx context.Context, ch chan<- Row) func(Row) error {
	return func(row Row) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- row:
			return nil
		}
	}
}

// planJoinQuery creates an optimized query plan for JOINs.
// Supports arbitrary join topologies (star schema, chain joins, and mixed).
// Optimizations:
//  1. Use index on inner table join column when available (index nested loop join).
//  2. Push single-table WHERE conditions into individual table scans.
//  3. Use index scans for pushed-down conditions when a matching index exists.
//  4. Greedy join reordering: when all tables have ANALYZE statistics and all
//     joins are INNER, reorders the join sequence so the smallest tables are
//     processed first, minimising intermediate result sizes.
func (t *Table) planJoinQuery(ctx context.Context, stmt Statement) (QueryPlan, error) {
	// Attempt greedy join reordering when statistics are available.
	if plan, ok, err := t.planJoinQueryGreedy(ctx, stmt); err != nil {
		return QueryPlan{}, err
	} else if ok {
		return plan, nil
	}

	// Fallback: user-specified order.
	baseTableFilters, joinTableFilters := pushDownFilters(stmt.Conditions, stmt.TableAlias, stmt.Joins)

	plan := QueryPlan{
		Scans:        []Scan{planJoinTableScan(t, t.Name, stmt.TableAlias, baseTableFilters)},
		Joins:        make([]JoinPlan, 0),
		OrderBy:      stmt.OrderBy,
		SortInMemory: len(stmt.OrderBy) > 0,
	}

	// scanIndexByAlias maps each table alias to its scan slot index.
	// Used to resolve LeftScanIndex for arbitrary join topologies.
	scanIndexByAlias := map[string]int{stmt.TableAlias: 0}

	if err := t.flattenJoinTree(ctx, &plan, stmt.Joins, scanIndexByAlias, joinTableFilters); err != nil {
		return QueryPlan{}, err
	}

	return plan, nil
}

// joinGraphNode is one participant (table) in the join graph used for greedy reordering.
type joinGraphNode struct {
	tableName  string
	tableAlias string
	table      *Table
	rows       int64 // estimated row count from ANALYZE; -1 if unknown
	// indexPartners maps a partner table alias to true when this node has an
	// index on its join column for that partner.  A non-empty map means this
	// node is a good candidate for the inner (build/lookup) side of an
	// index-NL join.  Populated by collectJoinGraph.
	indexPartners map[string]bool
}

// joinGraphEdge is an undirected edge in the join graph: the ON conditions
// connecting two aliases, plus the join type.  Conditions are stored verbatim
// from the parser — they identify both endpoints by alias prefix so they
// remain correct regardless of which end is treated as "left" vs "right".
type joinGraphEdge struct {
	alias1     string
	alias2     string
	conditions Conditions
	joinType   JoinType
}

// collectJoinGraph walks the join tree rooted at stmt and returns the set of
// joinGraphNodes (one per table, including the base) and joinGraphEdges (one
// per Join clause).  Returns (nil, nil, false) when the tree contains a join
// whose FromTableAlias cannot be resolved — indicating a topology too complex
// for greedy reordering.
func (t *Table) collectJoinGraph(ctx context.Context, stmt Statement) ([]joinGraphNode, []joinGraphEdge, bool) {
	nodes := []joinGraphNode{{
		tableName:  t.Name,
		tableAlias: stmt.TableAlias,
		table:      t,
		rows:       t.estimatedRowCount(),
	}}
	aliasSet := map[string]struct{}{stmt.TableAlias: {}}
	var edges []joinGraphEdge

	var walk func(joins []Join) bool
	walk = func(joins []Join) bool {
		for _, j := range joins {
			fromAlias := j.FromTableAlias()
			if fromAlias == "" {
				return false // cannot determine connectivity — abort
			}
			jTable, ok := t.provider.GetTable(ctx, j.TableName)
			if !ok {
				return false
			}
			nodes = append(nodes, joinGraphNode{
				tableName:  j.TableName,
				tableAlias: j.TableAlias,
				table:      jTable,
				rows:       jTable.estimatedRowCount(),
			})
			aliasSet[j.TableAlias] = struct{}{}
			edges = append(edges, joinGraphEdge{
				alias1:     fromAlias,
				alias2:     j.TableAlias,
				conditions: j.Conditions,
				joinType:   j.Type,
			})
			if !walk(j.Joins) {
				return false
			}
		}
		return true
	}
	if !walk(stmt.Joins) {
		return nil, nil, false
	}

	// Precompute index eligibility: for each edge, check whether each endpoint
	// has an index on its join column.  This guides greedy reordering to prefer
	// index-eligible nodes as the inner (lookup) side, enabling indexed NL joins.
	nodeByAlias := make(map[string]*joinGraphNode, len(nodes))
	for i := range nodes {
		nodeByAlias[nodes[i].tableAlias] = &nodes[i]
	}
	for _, e := range edges {
		for _, pair := range [2]struct{ self, partner string }{{e.alias1, e.alias2}, {e.alias2, e.alias1}} {
			n := nodeByAlias[pair.self]
			if n == nil || n.table == nil {
				continue
			}
			cols := joinColumnsForAlias(e.conditions, pair.self)
			if len(cols) > 0 && n.table.findIndexOnColumns(cols) != nil {
				if n.indexPartners == nil {
					n.indexPartners = make(map[string]bool)
				}
				n.indexPartners[pair.partner] = true
			}
		}
	}

	return nodes, edges, true
}

// joinColumnsForAlias extracts the column names that belong to targetAlias from
// a set of equi-join conditions.  Used to determine which column of a node
// participates in the join so we can check index availability.
func joinColumnsForAlias(conditions Conditions, targetAlias string) []string {
	var cols []string
	for _, cond := range conditions {
		if cond.Operator != Eq || cond.Operand1.Type != OperandField || cond.Operand2.Type != OperandField {
			continue
		}
		f1, ok1 := cond.Operand1.Value.(Field)
		f2, ok2 := cond.Operand2.Value.(Field)
		if !ok1 || !ok2 {
			continue
		}
		switch targetAlias {
		case f1.AliasPrefix:
			cols = append(cols, f1.Name)
		case f2.AliasPrefix:
			cols = append(cols, f2.Name)
		}
	}
	return cols
}

// greedyJoinOrder returns a reordered sequence of joinGraphNodes and, for each
// node beyond the first (the new base), the edge that connects it to the
// already-processed set.  The algorithm is greedy nearest-neighbour: start with
// the node with the fewest estimated rows, then at each step extend with the
// cheapest reachable node.
//
// Returns (nil, nil, false) when:
//   - any estimated row count is -1 (no ANALYZE data),
//   - any join is not INNER (outer joins are order-sensitive), or
//   - the graph is disconnected (should never happen for a valid SQL JOIN).
func greedyJoinOrder(nodes []joinGraphNode, edges []joinGraphEdge) ([]joinGraphNode, []joinGraphEdge, bool) {
	// Eligibility: all INNER, all rows known.
	for _, e := range edges {
		if e.joinType != Inner {
			return nil, nil, false
		}
	}
	for _, n := range nodes {
		if n.rows < 0 {
			return nil, nil, false
		}
	}

	// Build adjacency: alias → list of incident edges.
	adj := make(map[string][]int, len(nodes)) // alias → edge indices
	for i, e := range edges {
		adj[e.alias1] = append(adj[e.alias1], i)
		adj[e.alias2] = append(adj[e.alias2], i)
	}

	// Find the start (base/probe) node.
	//
	// A node with an index on a join column is best kept as the inner (lookup)
	// side so the planner can use an index NL join instead of a hash join.
	// Therefore we prefer to start with a node that has NO such index.
	// Among equally index-ineligible nodes, prefer the one with the most rows
	// (larger probe table is fine; it keeps the smaller table as the inner
	// build/lookup side, minimising hash-table memory when no index exists).
	// When all nodes have index-eligible joins, or none do, fall back to the
	// original fewest-rows heuristic (ties broken by row count).
	anyHasIndex := false
	for _, n := range nodes {
		if len(n.indexPartners) > 0 {
			anyHasIndex = true
			break
		}
	}

	startIdx := 0
	for i, n := range nodes {
		cur := nodes[startIdx]
		var preferI bool
		if anyHasIndex {
			curHasIdx := len(cur.indexPartners) > 0
			nHasIdx := len(n.indexPartners) > 0
			switch {
			case !nHasIdx && curHasIdx:
				preferI = true // n has no index: better as base/probe
			case nHasIdx && !curHasIdx:
				preferI = false // cur has no index: keep it
			case !nHasIdx && !curHasIdx:
				preferI = n.rows > cur.rows // both non-indexed: larger as probe
			default:
				preferI = n.rows < cur.rows // both indexed: fewest rows (original)
			}
		} else {
			preferI = n.rows < cur.rows // no indexes anywhere: original heuristic
		}
		if preferI {
			startIdx = i
		}
	}

	done := make(map[string]struct{}, len(nodes))
	remaining := make(map[string]int, len(nodes)) // alias → nodes index
	for i, n := range nodes {
		remaining[n.tableAlias] = i
	}

	orderedNodes := make([]joinGraphNode, 0, len(nodes))
	orderedEdges := make([]joinGraphEdge, 0, len(nodes)-1) // one per join (excludes base)

	start := nodes[startIdx]
	orderedNodes = append(orderedNodes, start)
	done[start.tableAlias] = struct{}{}
	delete(remaining, start.tableAlias)

	for len(remaining) > 0 {
		// Find the cheapest node reachable from any done node.
		bestNodeIdx := -1
		bestEdgeIdx := -1
		bestRows := int64(-1)

		for doneAlias := range done {
			for _, edgeIdx := range adj[doneAlias] {
				e := edges[edgeIdx]
				// Determine the other endpoint.
				var otherAlias string
				if e.alias1 == doneAlias {
					otherAlias = e.alias2
				} else {
					otherAlias = e.alias1
				}
				nodeIdx, stillPending := remaining[otherAlias]
				if !stillPending {
					continue
				}
				n := nodes[nodeIdx]
				// Prefer candidates with an index on the join column for any
				// already-placed (done) alias — they enable an index NL join as
				// the inner side and are always better than a hash-build node.
				// Among equally index-eligible candidates, prefer fewer rows.
				nHasIdx := false
				for doneA := range done {
					if n.indexPartners[doneA] {
						nHasIdx = true
						break
					}
				}
				bHasIdx := false
				if bestNodeIdx != -1 {
					for doneA := range done {
						if nodes[bestNodeIdx].indexPartners[doneA] {
							bHasIdx = true
							break
						}
					}
				}
				if bestNodeIdx == -1 || (nHasIdx && !bHasIdx) || (nHasIdx == bHasIdx && n.rows < bestRows) {
					bestNodeIdx = nodeIdx
					bestEdgeIdx = edgeIdx
					bestRows = n.rows
				}
			}
		}

		if bestNodeIdx == -1 {
			// Graph is disconnected — cannot reorder safely.
			return nil, nil, false
		}

		chosen := nodes[bestNodeIdx]
		orderedNodes = append(orderedNodes, chosen)
		orderedEdges = append(orderedEdges, edges[bestEdgeIdx])
		done[chosen.tableAlias] = struct{}{}
		delete(remaining, chosen.tableAlias)
	}

	return orderedNodes, orderedEdges, true
}

// planJoinQueryGreedy attempts greedy join reordering. Returns (plan, true, nil)
// when reordering is applied; (QueryPlan{}, false, nil) when the plan falls
// back to user order; (QueryPlan{}, false, err) on a hard error.
func (t *Table) planJoinQueryGreedy(ctx context.Context, stmt Statement) (QueryPlan, bool, error) {
	nodes, edges, ok := t.collectJoinGraph(ctx, stmt)
	if !ok {
		return QueryPlan{}, false, nil
	}

	orderedNodes, orderedEdges, ok := greedyJoinOrder(nodes, edges)
	if !ok {
		return QueryPlan{}, false, nil
	}

	// If the greedy order matches the user order exactly, fall through to the
	// normal path so existing test expectations are not perturbed.
	if orderedNodes[0].tableAlias == stmt.TableAlias {
		userOrderPreserved := true
		for i, j := range stmt.Joins {
			if i+1 >= len(orderedNodes) || orderedNodes[i+1].tableAlias != j.TableAlias {
				userOrderPreserved = false
				break
			}
		}
		if userOrderPreserved {
			return QueryPlan{}, false, nil
		}
	}

	// Build per-alias filters (push-down regardless of which table is base).
	allFilters := make(map[string]OneOrMore, len(nodes))
	{
		baseFilters, joinFilters := pushDownFilters(stmt.Conditions, stmt.TableAlias, stmt.Joins)
		allFilters[stmt.TableAlias] = baseFilters
		for alias, f := range joinFilters {
			allFilters[alias] = f
		}
	}

	plan := QueryPlan{
		OrderBy:      stmt.OrderBy,
		SortInMemory: len(stmt.OrderBy) > 0,
		Joins:        make([]JoinPlan, 0, len(orderedNodes)-1),
		Scans:        make([]Scan, 0, len(orderedNodes)),
	}

	newBase := orderedNodes[0]
	plan.Scans = append(plan.Scans, planJoinTableScan(
		newBase.table, newBase.tableName, newBase.tableAlias,
		allFilters[newBase.tableAlias],
	))
	scanIndexByAlias := map[string]int{newBase.tableAlias: 0}

	for i, node := range orderedNodes[1:] {
		edge := orderedEdges[i]

		// Determine from-alias: the endpoint of this edge that is already done.
		fromAlias := edge.alias1
		if _, done := scanIndexByAlias[fromAlias]; !done {
			fromAlias = edge.alias2
		}
		leftScanIndex := scanIndexByAlias[fromAlias]

		var fromTable *Table
		if leftScanIndex == 0 {
			fromTable = newBase.table
		} else {
			fromTableName := plan.Scans[leftScanIndex].TableName
			ft, ftOk := t.provider.GetTable(ctx, fromTableName)
			if !ftOk {
				return QueryPlan{}, false, minisqlErrors.ErrNoSuchTable{Name: fromTableName}
			}
			fromTable = ft
		}

		joinColumnPairs, err := extractJoinColumnPairs(
			edge.conditions, fromAlias, node.tableAlias,
			fromTable, node.table,
		)
		if err != nil {
			return QueryPlan{}, false, err
		}

		joinTableColumns := make([]string, len(joinColumnPairs))
		for j, pair := range joinColumnPairs {
			joinTableColumns[j] = pair.JoinTableColumn.Name
		}
		joinTableIndex := node.table.findIndexOnColumns(joinTableColumns)

		var (
			innerScanType  ScanType
			innerIndexInfo *IndexInfo
			algorithm      JoinAlgorithm
		)
		if joinTableIndex != nil {
			innerScanType = ScanTypeIndexPoint
			innerIndexInfo = joinTableIndex
			algorithm = JoinAlgorithmNestedLoop
		} else {
			innerScanType = ScanTypeSequential
			buildRows := node.rows
			if buildRows < 0 || buildRows <= hashJoinMaxBuildRows {
				algorithm = JoinAlgorithmHash
			} else {
				algorithm = JoinAlgorithmNestedLoop
			}
		}

		var joinScan Scan
		if innerScanType == ScanTypeSequential {
			joinScan = planJoinTableScan(node.table, node.tableName, node.tableAlias, allFilters[node.tableAlias])
		} else {
			joinScan = Scan{
				TableName:  node.tableName,
				TableAlias: node.tableAlias,
				Type:       innerScanType,
				Filters:    allFilters[node.tableAlias],
			}
			if innerIndexInfo != nil {
				joinScan.IndexName = innerIndexInfo.Name
				joinScan.IndexColumns = innerIndexInfo.Columns
			}
		}

		plan.Scans = append(plan.Scans, joinScan)
		rightScanIndex := len(plan.Scans) - 1
		scanIndexByAlias[node.tableAlias] = rightScanIndex

		plan.Joins = append(plan.Joins, JoinPlan{
			Type:            Inner,
			LeftScanIndex:   leftScanIndex,
			RightScanIndex:  rightScanIndex,
			Conditions:      edge.conditions,
			OuterJoinColumn: joinColumnPairs[0].BaseTableColumn.Name,
			InnerJoinColumn: joinColumnPairs[0].JoinTableColumn.Name,
			JoinColumnPairs: joinColumnPairs,
			Algorithm:       algorithm,
		})
	}

	return plan, true, nil
}

// flattenJoinTree recursively walks the join tree in DFS order and appends one
// Scan and one JoinPlan entry per join node. LeftScanIndex is set to the scan
// slot of the table the join is FROM (resolved via scanIndexByAlias), so chain
// joins (a→b→c) produce correct LeftScanIndex values instead of always 0.
func (t *Table) flattenJoinTree(
	ctx context.Context,
	plan *QueryPlan,
	joins []Join,
	scanIndexByAlias map[string]int,
	joinTableFilters map[string]OneOrMore,
) error {
	for _, join := range joins {
		fromAlias := join.FromTableAlias()
		leftScanIndex, ok := scanIndexByAlias[fromAlias]
		if !ok {
			return fmt.Errorf("join references unknown table alias %q", fromAlias)
		}

		// Resolve the table on the left (from) side for column validation.
		var fromTable *Table
		fromScan := plan.Scans[leftScanIndex]
		if leftScanIndex == 0 {
			fromTable = t
		} else {
			fromTable, ok = t.provider.GetTable(ctx, fromScan.TableName)
			if !ok {
				return minisqlErrors.ErrNoSuchTable{Name: fromScan.TableName}
			}
		}

		joinedTable, ok := t.provider.GetTable(ctx, join.TableName)
		if !ok {
			return minisqlErrors.ErrNoSuchTable{Name: join.TableName}
		}

		joinColumnPairs, err := extractJoinColumnPairs(
			join.Conditions,
			fromAlias,
			join.TableAlias,
			fromTable,
			joinedTable,
		)
		if err != nil {
			return err
		}

		joinTableColumns := make([]string, len(joinColumnPairs))
		for i, pair := range joinColumnPairs {
			joinTableColumns[i] = pair.JoinTableColumn.Name
		}

		joinTableIndex := joinedTable.findIndexOnColumns(joinTableColumns)

		var (
			innerScanType  ScanType
			innerIndexInfo *IndexInfo
			algorithm      JoinAlgorithm
		)
		if joinTableIndex != nil {
			// Index exists — use indexed nested-loop join.
			innerScanType = ScanTypeIndexPoint
			innerIndexInfo = joinTableIndex
			algorithm = JoinAlgorithmNestedLoop
		} else {
			innerScanType = ScanTypeSequential
			// No index on the inner join column.  Hash join is O(N+M) vs O(N×M)
			// for nested-loop, so prefer it unless the build side is too large to
			// materialise in memory or the join type requires nested-loop (RIGHT JOIN
			// and FULL OUTER JOIN need an unmatched-row pass that is harder to do with
			// a hash table).
			if join.Type != Right && join.Type != FullOuter {
				buildRows := joinedTable.estimatedRowCount()
				if buildRows < 0 || buildRows <= hashJoinMaxBuildRows {
					algorithm = JoinAlgorithmHash
				} else {
					algorithm = JoinAlgorithmNestedLoop
				}
			} else {
				algorithm = JoinAlgorithmNestedLoop
			}
		}

		var joinScan Scan
		if innerScanType == ScanTypeSequential {
			// No index on the join column: try to accelerate the scan using an index
			// on any pushed-down WHERE condition (e.g. salary > 80000 on the build
			// side of a hash join).
			joinScan = planJoinTableScan(joinedTable, join.TableName, join.TableAlias, joinTableFilters[join.TableAlias])
		} else {
			// Index nested-loop join: the join key drives the index point scan;
			// pushed-down filters are applied as post-lookup row filters.
			joinScan = Scan{
				TableName:  join.TableName,
				TableAlias: join.TableAlias,
				Type:       innerScanType,
				Filters:    joinTableFilters[join.TableAlias],
			}
			if innerScanType == ScanTypeIndexPoint && innerIndexInfo != nil {
				joinScan.IndexName = innerIndexInfo.Name
				joinScan.IndexColumns = innerIndexInfo.Columns
			}
		}

		plan.Scans = append(plan.Scans, joinScan)
		rightScanIndex := len(plan.Scans) - 1
		scanIndexByAlias[join.TableAlias] = rightScanIndex

		plan.Joins = append(plan.Joins, JoinPlan{
			Type:            join.Type,
			LeftScanIndex:   leftScanIndex,
			RightScanIndex:  rightScanIndex,
			Conditions:      join.Conditions,
			OuterJoinColumn: joinColumnPairs[0].BaseTableColumn.Name,
			InnerJoinColumn: joinColumnPairs[0].JoinTableColumn.Name,
			JoinColumnPairs: joinColumnPairs,
			Algorithm:       algorithm,
		})

		// Recurse for joins that hang off this join (chain joins).
		if len(join.Joins) > 0 {
			if err := t.flattenJoinTree(ctx, plan, join.Joins, scanIndexByAlias, joinTableFilters); err != nil {
				return err
			}
		}
	}
	return nil
}

// extractJoinColumnPairs extracts all column pairs from JOIN ON conditions
// and validates that the columns exist in both tables
// Supports multiple conditions like: ON b.a_id = a.id AND b.other_id = a.other_id
func extractJoinColumnPairs(conditions Conditions, baseAlias, joinAlias string, baseTable, joinTable *Table) ([]JoinColumnPair, error) {
	var pairs []JoinColumnPair

	for _, cond := range conditions {
		if cond.Operator != Eq {
			continue
		}

		// Check if both operands are fields
		if cond.Operand1.Type != OperandField || cond.Operand2.Type != OperandField {
			continue
		}

		field1, ok1 := cond.Operand1.Value.(Field)
		field2, ok2 := cond.Operand2.Value.(Field)
		if !ok1 || !ok2 {
			continue
		}

		// Match aliases to determine which field belongs to which table
		var baseField, joinField Field
		switch {
		case field1.AliasPrefix == baseAlias && field2.AliasPrefix == joinAlias:
			baseField, joinField = field1, field2
		case field1.AliasPrefix == joinAlias && field2.AliasPrefix == baseAlias:
			baseField, joinField = field2, field1
		default:
			// Not a valid join condition between these two tables
			continue
		}

		// Validate that columns exist in both tables
		if _, ok := baseTable.ColumnByName(baseField.Name); !ok {
			return nil, fmt.Errorf("column %s does not exist in base table %s", baseField.Name, baseTable.Name)
		}
		if _, ok := joinTable.ColumnByName(joinField.Name); !ok {
			return nil, fmt.Errorf("column %s does not exist in join table %s", joinField.Name, joinTable.Name)
		}

		pairs = append(pairs, JoinColumnPair{
			BaseTableColumn: baseField,
			JoinTableColumn: joinField,
		})
	}

	if len(pairs) == 0 {
		return nil, errors.New("could not extract valid join columns from conditions")
	}

	return pairs, nil
}

// findIndexOnColumns finds an index (primary key, unique, or secondary) that matches the given columns
// This supports both single column and composite indexes
// The columns must match the index prefix (first N columns of the index)
func (t *Table) findIndexOnColumns(columnNames []string) *IndexInfo {
	if len(columnNames) == 0 {
		return nil
	}

	// Helper function to check if index columns match the requested columns
	matchesColumns := func(indexCols []Column) bool {
		// Index must have at least as many columns as requested
		if len(indexCols) < len(columnNames) {
			return false
		}
		// Check if the first N columns of the index match the requested columns
		for i, colName := range columnNames {
			if indexCols[i].Name != colName {
				return false
			}
		}
		return true
	}

	// Check primary key
	if matchesColumns(t.PrimaryKey.Columns) {
		return &IndexInfo{
			Name:    t.PrimaryKey.Name,
			Columns: t.PrimaryKey.Columns,
		}
	}

	// Check unique indexes
	for name, idx := range t.UniqueIndexes {
		if matchesColumns(idx.Columns) {
			return &IndexInfo{
				Name:    name,
				Columns: idx.Columns,
			}
		}
	}

	// Check secondary indexes (skip partial indexes — they omit rows)
	for name, idx := range t.SecondaryIndexes {
		if !idx.IsBTree() {
			continue
		}
		if idx.WhereClause != "" {
			continue
		}
		if matchesColumns(idx.Columns) {
			return &IndexInfo{
				Name:    name,
				Columns: idx.Columns,
			}
		}
	}

	return nil
}

// collectJoinAliases recursively gathers every table alias from a join tree into dst.
func collectJoinAliases(joins []Join, dst map[string]struct{}) {
	for _, j := range joins {
		dst[j.TableAlias] = struct{}{}
		collectJoinAliases(j.Joins, dst)
	}
}

// planJoinTableScan returns an optimized Scan for a table participating in a JOIN.
// It tries index selection on the pushed-down conditions. When index selection
// produces exactly one scan (the common case: single AND group in WHERE), that
// index scan is returned. Otherwise the conditions are kept as sequential-scan
// post-filters to avoid the complexity of unioning multiple OR-group index scans
// inside the JOIN execution path.
func planJoinTableScan(t *Table, tableName, tableAlias string, conditions OneOrMore) Scan {
	defaultScan := Scan{
		TableName:  tableName,
		TableAlias: tableAlias,
		Type:       ScanTypeSequential,
		Filters:    conditions,
	}
	if len(conditions) == 0 || t.HasNoIndex() {
		return defaultScan
	}
	tempPlan := &QueryPlan{Scans: []Scan{defaultScan}}
	if err := tempPlan.setIndexScans(t, conditions); err != nil {
		return defaultScan
	}
	if len(tempPlan.Scans) != 1 || tempPlan.Scans[0].Type == ScanTypeIndexUnion {
		// Multiple OR groups each with an index — keep sequential for simplicity in JOIN path.
		return defaultScan
	}
	scan := tempPlan.Scans[0]
	scan.TableAlias = tableAlias
	return scan
}

// neededOuterFields returns the subset of base-table columns that the join
// actually references.  Decoding only needed columns avoids boxing TextPointer
// values for columns that appear in the schema but are not selected or used as
// join keys — saving one heap allocation per unused text column per outer row.
//
// When there are scan filters the function falls back to all columns because
// the filter evaluation reads column values by index and must see decoded data.
func neededOuterFields(p QueryPlan, baseTable *Table, baseScan Scan, selectedFields []Field) []Field {
	// With filters we cannot safely skip columns (filter may read any column).
	if len(baseScan.Filters) > 0 {
		return fieldsFromColumns(baseTable.Columns...)
	}

	needed := make(map[string]struct{}, len(baseTable.Columns))

	// Join key columns for every join that has the base table on its left side.
	for _, join := range p.Joins {
		if join.LeftScanIndex != 0 {
			continue
		}
		for _, pair := range join.JoinColumnPairs {
			needed[pair.BaseTableColumn.Name] = struct{}{}
		}
		if join.OuterJoinColumn != "" {
			needed[join.OuterJoinColumn] = struct{}{}
		}
	}

	// SELECT fields that are explicitly scoped to the base table's alias.
	// Fields with no alias prefix are ambiguous in a join query — include all.
	baseAlias := baseScan.TableAlias
	for _, f := range selectedFields {
		if f.AliasPrefix == "" {
			// Unaliased field: can't determine ownership — fall back to all cols.
			return fieldsFromColumns(baseTable.Columns...)
		}
		if f.AliasPrefix == baseAlias {
			needed[f.Name] = struct{}{}
		}
	}

	if len(needed) == 0 || len(needed) >= len(baseTable.Columns) {
		return fieldsFromColumns(baseTable.Columns...)
	}

	// Build reduced field list preserving schema order.
	fields := make([]Field, 0, len(needed))
	for _, col := range baseTable.Columns {
		if _, ok := needed[col.Name]; ok {
			fields = append(fields, Field{Name: col.Name})
		}
	}
	return fields
}

// runTableScan dispatches a single-table scan to the appropriate method based on
// the scan type. Used in the JOIN execution path where the scan type may have been
// optimized to an index scan by planJoinTableScan.
// plan is forwarded to index range scans (which need the sort-direction hint).
func runTableScan(ctx context.Context, plan QueryPlan, t *Table, scan Scan, fields []Field, out func(Row) error) error {
	switch scan.Type {
	case ScanTypeIndexPoint:
		return t.indexPointScan(ctx, scan, fields, out)
	case ScanTypeIndexRange:
		return t.indexRangeScan(ctx, plan, scan, fields, out)
	case ScanTypeIndexIntersect:
		return t.indexIntersectScan(ctx, scan, fields, out)
	default:
		return t.sequentialScan(ctx, scan, fields, out)
	}
}

// pushDownFilters separates WHERE conditions by table alias, pushing filters to
// the appropriate per-table scan. Handles arbitrary join topologies by collecting
// all join aliases recursively before distributing conditions.
func pushDownFilters(conditions OneOrMore, baseTableAlias string, joins []Join) (OneOrMore, map[string]OneOrMore) {
	allJoinAliases := make(map[string]struct{})
	collectJoinAliases(joins, allJoinAliases)

	baseFilters := OneOrMore{}
	joinFilters := make(map[string]OneOrMore, len(allJoinAliases))
	for alias := range allJoinAliases {
		joinFilters[alias] = OneOrMore{}
	}

	for _, group := range conditions {
		baseGroup := Conditions{}
		joinGroups := make(map[string]Conditions, len(allJoinAliases))
		for alias := range allJoinAliases {
			joinGroups[alias] = Conditions{}
		}

		for _, condition := range group {
			tableAlias := getConditionTableAlias(condition, baseTableAlias, allJoinAliases)
			if tableAlias == baseTableAlias {
				baseGroup = append(baseGroup, condition)
			} else if _, exists := joinGroups[tableAlias]; exists {
				joinGroups[tableAlias] = append(joinGroups[tableAlias], condition)
			} else {
				baseGroup = append(baseGroup, condition)
			}
		}

		if len(baseGroup) > 0 {
			baseFilters = append(baseFilters, baseGroup)
		}
		for alias, grp := range joinGroups {
			if len(grp) > 0 {
				joinFilters[alias] = append(joinFilters[alias], grp)
			}
		}
	}

	return baseFilters, joinFilters
}

// getConditionTableAlias determines which table alias a condition belongs to.
// allJoinAliases is the complete set of join table aliases (all depths).
func getConditionTableAlias(condition Condition, baseTableAlias string, allJoinAliases map[string]struct{}) string {
	checkAlias := func(prefix string) (string, bool) {
		if prefix == "" {
			return "", false
		}
		if prefix == baseTableAlias {
			return baseTableAlias, true
		}
		if _, ok := allJoinAliases[prefix]; ok {
			return prefix, true
		}
		return "", false
	}

	if condition.Operand1.Type == OperandField {
		if field, ok := condition.Operand1.Value.(Field); ok {
			if alias, found := checkAlias(field.AliasPrefix); found {
				return alias
			}
		}
	}
	if condition.Operand2.Type == OperandField {
		if field, ok := condition.Operand2.Value.(Field); ok {
			if alias, found := checkAlias(field.AliasPrefix); found {
				return alias
			}
		}
	}
	return baseTableAlias
}

// executeNestedLoopJoin performs join execution for multi-table queries.
// Hash joins build their hash table once here; nested-loop joins probe the
// inner table per outer row inside executeJoinsForRow.
func (p QueryPlan) executeNestedLoopJoin(ctx context.Context, provider TableProvider, selectedFields []Field, filteredPipe chan<- Row) error {
	if len(p.Joins) == 0 {
		return errors.New("no joins to execute")
	}

	// Build hash tables for all hash-join entries (O(inner) each, done once).
	hashTables, err := buildHashBuckets(ctx, p, provider)
	if err != nil {
		return err
	}

	baseScan := p.Scans[0]
	baseTable, ok := provider.GetTable(ctx, baseScan.TableName)
	if !ok {
		return minisqlErrors.ErrNoSuchTable{Name: baseScan.TableName}
	}

	// Precompute per-join-level state that is constant across all outer rows.
	// This eliminates repeated table lookups, fieldsFromColumns calls, column
	// schema allocations, and filter compilation in the hot per-row path.
	joinMemos := make([]joinMemo, len(p.Joins))
	for i, join := range p.Joins {
		innerScan := p.Scans[join.RightScanIndex]
		innerTable, innerOK := provider.GetTable(ctx, innerScan.TableName)
		if !innerOK {
			return minisqlErrors.ErrNoSuchTable{Name: innerScan.TableName}
		}

		var combinedCols []Column
		if i == 0 {
			combinedCols = buildCombinedColumns(baseTable.Columns, baseScan.TableAlias, innerTable.Columns, innerScan.TableAlias)
		} else {
			combinedCols = buildCombinedColumnsProgressive(joinMemos[i-1].combinedColumns, innerTable.Columns, innerScan.TableAlias)
		}

		joinConditions := OneOrMore{}
		if len(join.Conditions) > 0 {
			joinConditions = append(joinConditions, join.Conditions)
		}

		allMask := make([]bool, len(innerTable.Columns))
		for k := range allMask {
			allMask[k] = true
		}
		innerFields := fieldsFromColumns(innerTable.Columns...)
		innerScanForMemo := p.Scans[join.RightScanIndex]
		innerIdx, _ := innerTable.IndexByName(innerScanForMemo.IndexName)
		joinMemos[i] = joinMemo{
			innerTable:          innerTable,
			innerFields:         innerFields,
			innerAllMask:        allMask,
			combinedColumns:     combinedCols,
			joinFilter:          compileRowFilterForColumns(combinedCols, joinConditions),
			nullInnerCount:      len(innerTable.Columns),
			singleKeySlice:      make([]any, 1),
			innerIndex:          innerIdx,
			innerSelectedMask:   selectedColumnsMask(innerTable.Columns, innerFields),
			innerNSelected:      len(innerFields),
			innerTableFilter:    compileScanFilter(innerTable.Columns, innerScanForMemo.Filters),
			innerCoveringFilter: compileScanFilter(innerScanForMemo.IndexColumns, innerScanForMemo.Filters),
			innerCovering:       innerScanForMemo.CoveringIndex,
			innerIndexCols:      innerScanForMemo.IndexColumns,
		}
	}

	// RowView outer scan: for sequential non-virtual hash joins, iterate the base
	// table as RowViews to avoid materialising unmatched outer rows.
	if canUseOuterRowViewScan(p, baseTable, baseScan) {
		if err := p.executeNestedLoopJoinOuterRowView(ctx, provider, baseTable, baseScan, filteredPipe, hashTables, joinMemos); err != nil {
			return err
		}
	} else {
		baseRowChan := make(chan Row, 100)
		baseErrChan := make(chan error, 1)
		baseFields := neededOuterFields(p, baseTable, baseScan, selectedFields)

		go func() {
			defer close(baseRowChan)
			if err := runTableScan(ctx, p, baseTable, baseScan, baseFields, chanRowCallback(ctx, baseRowChan)); err != nil {
				baseErrChan <- err
			}
		}()

		for baseRow := range baseRowChan {
			select {
			case err := <-baseErrChan:
				return err
			default:
			}
			if err := p.executeJoinsForRow(ctx, provider, baseRow, 0, filteredPipe, hashTables, joinMemos); err != nil {
				return err
			}
		}

		select {
		case err := <-baseErrChan:
			return err
		default:
		}
	}

	// RIGHT JOIN / FULL OUTER JOIN: emit right-table rows that had no matching base row.
	hasRightOrFullJoin := false
	for _, j := range p.Joins {
		if j.Type == Right || j.Type == FullOuter {
			hasRightOrFullJoin = true
			break
		}
	}
	if hasRightOrFullJoin {
		if err := p.executeRightJoinPass(ctx, provider, filteredPipe); err != nil {
			return err
		}
	}

	return nil
}

// canUseOuterRowViewScan reports whether the outer (base) table scan in a hash
// join can be driven as a RowView iteration rather than a fully materialised Row
// scan. When true, executeNestedLoopJoinOuterRowView can be called instead of the
// channel-based fallback path.
func canUseOuterRowViewScan(p QueryPlan, baseTable *Table, baseScan Scan) bool {
	return baseScan.Type == ScanTypeSequential &&
		baseTable.virtualRows == nil &&
		!baseTable.parallelScan &&
		len(p.Joins) > 0 &&
		p.Joins[0].Algorithm == JoinAlgorithmHash &&
		len(p.Joins[0].JoinColumnPairs) > 0 &&
		rowViewFilterSupports(baseTable.Columns, baseScan.Filters)
}

// executeNestedLoopJoinOuterRowView iterates the base table as RowViews and
// materialises each outer row only when the hash probe succeeds (or when the join
// type requires an outer row even on a miss, e.g. LEFT JOIN). For INNER JOINs
// where selectivity is low this eliminates make([]OptionalValue) for every
// unmatched outer row — the dominant allocation in a wide-table join scan.
//
// Caller must have verified canUseOuterRowViewScan before calling.
func (p QueryPlan) executeNestedLoopJoinOuterRowView(
	ctx context.Context,
	provider TableProvider,
	baseTable *Table,
	baseScan Scan,
	filteredPipe chan<- Row,
	hashTables map[int]*hashJoinBucket,
	joinMemos []joinMemo,
) error {
	firstJoin := p.Joins[0]

	// Pre-compute the base-table column indices for the first join's key columns.
	// These are used by appendHashKeyFromView to extract the probe key without
	// materialising the full row.
	colIdxMap := make(map[string]int, len(baseTable.Columns))
	for i, col := range baseTable.Columns {
		colIdxMap[col.Name] = i
	}
	colIdxs := make([]int, len(firstJoin.JoinColumnPairs))
	colKinds := make([]ColumnKind, len(firstJoin.JoinColumnPairs))
	for i, pair := range firstJoin.JoinColumnPairs {
		idx, ok := colIdxMap[pair.BaseTableColumn.Name]
		if !ok {
			return fmt.Errorf("outer RowView scan: join column %q not found in base table %s",
				pair.BaseTableColumn.Name, baseTable.Name)
		}
		colIdxs[i] = idx
		colKinds[i] = baseTable.Columns[idx].Kind
	}

	// All-true mask: materialise every base-table column (same as the existing path).
	allMask := make([]bool, len(baseTable.Columns))
	for i := range allMask {
		allMask[i] = true
	}

	// Compile a RowView filter for any post-scan base-table conditions.
	var baseFilter func(context.Context, RowView) (bool, error)
	if len(baseScan.Filters) > 0 {
		baseFilter = compileRowViewFilterForColumns(baseTable.Columns, baseTable.pager, baseScan.Filters)
	}

	bucket := hashTables[0]
	innerJoin := firstJoin.Type == Inner

	cursor, err := baseTable.SeekFirst(ctx)
	if err != nil {
		return err
	}
	page, err := baseTable.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return fmt.Errorf("outer RowView scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	var keyBuf [128]byte

	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return err
		}
		if page.Index != cursor.PageIdx {
			page, err = baseTable.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return fmt.Errorf("outer RowView scan: %w", err)
			}
		}

		cell := page.LeafNode.Cells[cursor.CellIdx]
		advanceLeafCursor(cursor, page)

		view := NewRowView(baseTable.Columns, cell)

		// Apply post-scan base-table filters via typed RowView accessors.
		if baseFilter != nil {
			pass, filterErr := baseFilter(ctx, view)
			if filterErr != nil {
				return filterErr
			}
			if !pass {
				continue
			}
		}

		// Extract probe key via typed RowView accessors — no make([]OptionalValue).
		probeKey, valid, err := appendHashKeyFromView(keyBuf[:0], view, colIdxs, colKinds)
		if err != nil {
			return err
		}

		// INNER JOIN fast-skip: if the key is NULL or the Bloom filter reports a
		// definite absence, this outer row has no match — skip without materialising.
		if innerJoin && (!valid || bucket == nil || !bucket.filter.MayContain(probeKey)) {
			continue
		}

		// Materialise the base-table row now that we know it may be needed.
		baseRow, err := view.MaterializeWithOverflow(ctx, baseTable.pager, allMask)
		if err != nil {
			return err
		}

		if err := p.executeJoinsForRow(ctx, provider, baseRow, 0, filteredPipe, hashTables, joinMemos); err != nil {
			return err
		}
	}

	return nil
}

// executeJoinsForRow recursively executes joins for a given row.
// joinIndex indicates which join in p.Joins is being processed.
// At joinIndex == 0, currentRow is the raw base-table row (column names not yet
// alias-prefixed). For joinIndex > 0 it is an already-combined row whose column
// names carry alias prefixes (e.g. "a.id", "b.name"). The from-side alias for
// each join is resolved from p.Scans[join.LeftScanIndex].TableAlias, enabling
// correct key lookup for both star-schema and chain joins.
// hashTables holds the pre-built hash buckets for JoinAlgorithmHash joins.
// memos holds per-join-level state precomputed once by executeNestedLoopJoin.
func (p QueryPlan) executeJoinsForRow(ctx context.Context, provider TableProvider, currentRow Row, joinIndex int, filteredPipe chan<- Row, hashTables map[int]*hashJoinBucket, memos []joinMemo) error {
	if joinIndex >= len(p.Joins) {
		select {
		case filteredPipe <- currentRow:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	join := p.Joins[joinIndex]

	// Hash join: probe the pre-built hash table instead of scanning the inner table.
	if join.Algorithm == JoinAlgorithmHash {
		return p.executeHashJoinForRow(ctx, currentRow, joinIndex, filteredPipe, hashTables, memos)
	}

	// Nested-loop join (index point or sequential).
	// Use precomputed per-join-level state from memos to avoid repeated allocations.
	// Take a pointer into the slice (not a copy) so the 216-byte struct never
	// escapes to the heap due to closure capture in the non-unique index path.
	memo := &memos[joinIndex]
	innerScan := p.Scans[join.RightScanIndex]
	fromAlias := p.Scans[join.LeftScanIndex].TableAlias

	matched := false

	if innerScan.Type == ScanTypeIndexPoint && join.InnerJoinColumn != "" {
		// Synchronous index point-scan: no goroutine or channel needed.
		// An index point lookup returns a small number of rows (0 or 1 for unique
		// indexes); spawning a goroutine + 100-slot buffered channel per outer row
		// is far more expensive than the lookup itself.
		var joinKeyValues []any
		if len(join.JoinColumnPairs) > 0 {
			if len(join.JoinColumnPairs) == 1 {
				// Single-pair fast path: reuse the pre-allocated singleKeySlice to
				// avoid make([]any, 1) on every outer row (~70MB saved per benchmark).
				pair := join.JoinColumnPairs[0]
				var (
					keyValue OptionalValue
					ok       bool
				)
				if joinIndex == 0 {
					keyValue, ok = currentRow.GetValue(pair.BaseTableColumn.Name)
				} else {
					keyValue, ok = currentRow.GetValue(fromAlias + "." + pair.BaseTableColumn.Name)
				}
				if ok && keyValue.Valid {
					memo.singleKeySlice[0] = keyValue.Value
					joinKeyValues = memo.singleKeySlice
				}
			} else {
				joinKeyValues = make([]any, len(join.JoinColumnPairs))
				for i, pair := range join.JoinColumnPairs {
					var (
						keyValue OptionalValue
						ok       bool
					)
					if joinIndex == 0 {
						keyValue, ok = currentRow.GetValue(pair.BaseTableColumn.Name)
					} else {
						keyValue, ok = currentRow.GetValue(fromAlias + "." + pair.BaseTableColumn.Name)
					}
					if !ok || !keyValue.Valid {
						joinKeyValues = nil
						break
					}
					joinKeyValues[i] = keyValue.Value
				}
			}
		} else {
			var (
				keyValue OptionalValue
				ok       bool
			)
			if joinIndex == 0 {
				keyValue, ok = currentRow.GetValue(join.OuterJoinColumn)
			} else {
				keyValue, ok = currentRow.GetValue(fromAlias + "." + join.OuterJoinColumn)
			}
			if ok && keyValue.Valid {
				// Reuse the pre-allocated single-key slice from joinMemo to avoid
				// allocating a new []any{...} on every outer row.
				memo.singleKeySlice[0] = keyValue.Value
				joinKeyValues = memo.singleKeySlice
			}
		}

		if len(joinKeyValues) > 0 {
			indexScan := innerScan
			indexScan.IndexKeys = joinKeyValues
			switch {
			case join.Type == Semi || join.Type == AntiSemi:
				exists, err := memo.innerTable.indexPointExists(ctx, indexScan, memo.innerFields)
				if err != nil {
					return err
				}
				matched = exists
			case !memo.innerTable.isUniquePointIndex(indexScan.IndexName):
				err := memo.innerTable.indexPointScan(ctx, indexScan, memo.innerFields, func(innerRow Row) error {
					combinedRow := combineRowsWithSchema(currentRow, innerRow, memo.combinedColumns)
					matches := true
					if memo.joinFilter != nil {
						var filterErr error
						matches, filterErr = memo.joinFilter(combinedRow)
						if filterErr != nil {
							return filterErr
						}
					}
					if matches {
						matched = true
						if err := p.executeJoinsForRow(ctx, provider, combinedRow, joinIndex+1, filteredPipe, hashTables, memos); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					return err
				}
			default:
				// Unique index fast path: PointUniqueRowID avoids allocating a closure +
				// rows slice on every outer row (including the 99%-miss case).
				for _, indexKey := range joinKeyValues {
					rowID, ptErr := memo.innerIndex.PointUniqueRowID(ctx, indexKey)
					if errors.Is(ptErr, ErrNotFound) {
						continue
					}
					if ptErr != nil {
						return ptErr
					}
					row, ok, rowErr := memo.innerTable.indexedScanRow(
						ctx, indexKey, rowID,
						memo.innerCovering, memo.innerIndexCols,
						memo.innerSelectedMask, memo.innerNSelected,
						memo.innerTableFilter, memo.innerCoveringFilter,
					)
					if rowErr != nil {
						return rowErr
					}
					if !ok {
						continue
					}
					combinedRow := combineRowsWithSchema(currentRow, row, memo.combinedColumns)
					matches := true
					if memo.joinFilter != nil {
						var filterErr error
						if matches, filterErr = memo.joinFilter(combinedRow); filterErr != nil {
							return filterErr
						}
					}
					if matches {
						matched = true
						if err := p.executeJoinsForRow(ctx, provider, combinedRow, joinIndex+1, filteredPipe, hashTables, memos); err != nil {
							return err
						}
					}
				}
			}
		}
	} else {
		// Async sequential scan: run in a goroutine so the outer loop and inner
		// scan can pipeline. Use a modest buffer to limit memory per join.
		innerCtx := ctx
		var innerCancel context.CancelFunc
		if join.Type == Semi || join.Type == AntiSemi {
			innerCtx, innerCancel = context.WithCancel(ctx)
			defer innerCancel()
		}

		innerRowChan := make(chan Row, 16)
		innerErrChan := make(chan error, 1)

		// Pass innerScan by value as a goroutine argument rather than capturing
		// it in the closure.  Capture causes the Scan struct (~200 B) to escape
		// to the heap for every outer row — even rows that take the index-scan
		// branch never reach this goroutine.  Argument-passing keeps innerScan
		// on the stack for the calling frame.
		go func(scan Scan) {
			defer close(innerRowChan)
			if err := memo.innerTable.sequentialScan(innerCtx, scan, memo.innerFields, chanRowCallback(innerCtx, innerRowChan)); err != nil {
				innerErrChan <- err
			}
		}(innerScan)

		for innerRow := range innerRowChan {
			select {
			case err := <-innerErrChan:
				return err
			default:
			}

			if join.Type == Semi || join.Type == AntiSemi {
				matched = true
				innerCancel()
				drainRowCh(innerRowChan)
				break
			}

			combinedRow := combineRowsWithSchema(currentRow, innerRow, memo.combinedColumns)
			matches := true
			if memo.joinFilter != nil {
				var err error
				matches, err = memo.joinFilter(combinedRow)
				if err != nil {
					return err
				}
			}
			if matches {
				matched = true
				if err := p.executeJoinsForRow(ctx, provider, combinedRow, joinIndex+1, filteredPipe, hashTables, memos); err != nil {
					return err
				}
			}
		}

		select {
		case err := <-innerErrChan:
			if innerCancel != nil && errors.Is(err, context.Canceled) {
				break // we triggered the cancellation ourselves
			}
			return err
		default:
		}
	}

	// Semi-join: emit the outer row when the inner had at least one match.
	if join.Type == Semi {
		if matched {
			outerRow := currentRow
			if joinIndex == 0 {
				outerRow = prefixRowAlias(currentRow, fromAlias)
			}
			return p.executeJoinsForRow(ctx, provider, outerRow, joinIndex+1, filteredPipe, hashTables, memos)
		}
		return nil
	}

	// Anti-semi-join: emit the outer row only when there was no inner match.
	if join.Type == AntiSemi {
		outerRow := currentRow
		if joinIndex == 0 {
			outerRow = prefixRowAlias(currentRow, fromAlias)
		}
		return p.executeJoinsForRow(ctx, provider, outerRow, joinIndex+1, filteredPipe, hashTables, memos)
	}

	// LEFT JOIN / FULL OUTER JOIN: emit the outer row with NULL-filled inner columns when nothing matched.
	if !matched && (join.Type == Left || join.Type == FullOuter) {
		combinedRow := combineRowWithNullInner(currentRow, memo.nullInnerCount, memo.combinedColumns)
		if err := p.executeJoinsForRow(ctx, provider, combinedRow, joinIndex+1, filteredPipe, hashTables, memos); err != nil {
			return err
		}
	}

	return nil
}

// executeHashJoinForRow probes the pre-built hash table for joinIndex and
// recurses for matching inner rows.  Handles LEFT JOIN miss by emitting a
// NULL-padded combined row when the probe finds no matches.
func (p QueryPlan) executeHashJoinForRow(ctx context.Context, currentRow Row, joinIndex int, filteredPipe chan<- Row, hashTables map[int]*hashJoinBucket, memos []joinMemo) error {
	join := p.Joins[joinIndex]
	memo := memos[joinIndex]
	fromAlias := p.Scans[join.LeftScanIndex].TableAlias

	bucket := hashTables[joinIndex]

	// Build the probe key into a stack-local buffer; nil means a NULL join column.
	var probeKeyBuf [128]byte
	probeKey := appendHashKey(probeKeyBuf[:0], join, currentRow, fromAlias, joinIndex, false)

	// Semi-join / anti-semi-join: check existence only — no need to load inner
	// rows.  The build phase populated bucket.present (not bucket.rows).
	if join.Type == Semi || join.Type == AntiSemi {
		var found bool
		if probeKey != nil && bucket != nil && bucket.filter.MayContain(probeKey) {
			// Go compiler optimises map[string]struct{}[string([]byte)] to avoid
			// allocation when the key is already present.
			_, found = bucket.present[string(probeKey)]
		}
		emit := (join.Type == Semi && found) || (join.Type == AntiSemi && !found)
		if emit {
			outerRow := currentRow
			if joinIndex == 0 {
				outerRow = prefixRowAlias(currentRow, fromAlias)
			}
			return p.executeJoinsForRow(ctx, nil, outerRow, joinIndex+1, filteredPipe, hashTables, memos)
		}
		return nil
	}

	// Regular join: retrieve matching inner rows from the bucket.
	matched := false
	if probeKey != nil && bucket != nil && bucket.filter.MayContain(probeKey) {
		if bucket.cells != nil {
			// Compact path: decode cell bytes lazily on probe hit, avoiding
			// the build-phase []OptionalValue allocation for unmatched rows.
			// Use string(probeKey) directly in the index expression — the Go compiler
			// elides the string allocation for []byte→string conversions used only
			// as a map key, avoiding one heap alloc per matching outer row.
			for _, cc := range bucket.cells[string(probeKey)] {
				innerRow, err := NewRowView(memo.innerTable.Columns, cc.toCell()).
					MaterializeWithOverflow(ctx, memo.innerTable.pager, memo.innerAllMask)
				if err != nil {
					return err
				}
				combinedRow := combineRowsWithSchema(currentRow, innerRow, memo.combinedColumns)
				matched = true
				if err := p.executeJoinsForRow(ctx, nil, combinedRow, joinIndex+1, filteredPipe, hashTables, memos); err != nil {
					return err
				}
			}
		} else {
			// Legacy path: pre-materialised rows (index scans or non-RowView-compatible filters).
			for _, innerRow := range bucket.rows[string(probeKey)] {
				combinedRow := combineRowsWithSchema(currentRow, innerRow, memo.combinedColumns)
				matched = true
				if err := p.executeJoinsForRow(ctx, nil, combinedRow, joinIndex+1, filteredPipe, hashTables, memos); err != nil {
					return err
				}
			}
		}
	}

	// LEFT JOIN / FULL OUTER JOIN: no matching inner row — emit outer row with NULL inner columns.
	if !matched && (join.Type == Left || join.Type == FullOuter) {
		combinedRow := combineRowWithNullInner(currentRow, memo.nullInnerCount, memo.combinedColumns)
		if err := p.executeJoinsForRow(ctx, nil, combinedRow, joinIndex+1, filteredPipe, hashTables, memos); err != nil {
			return err
		}
	}

	return nil
}

// executeRightJoinPass scans each RIGHT JOIN's inner table and emits rows that
// had no matching base-table row. The combined row has NULL values for the base
// table and all other joined tables, and the actual values only for the right
// (unmatched) table.
func (p QueryPlan) executeRightJoinPass(ctx context.Context, provider TableProvider, filteredPipe chan<- Row) error {
	baseScan := p.Scans[0]
	baseTable, ok := provider.GetTable(ctx, baseScan.TableName)
	if !ok {
		return minisqlErrors.ErrNoSuchTable{Name: baseScan.TableName}
	}
	baseFields := fieldsFromColumns(baseTable.Columns...)

	for joinIndex, join := range p.Joins {
		if join.Type != Right && join.Type != FullOuter {
			continue
		}

		innerScan := p.Scans[join.RightScanIndex]
		innerTable, ok := provider.GetTable(ctx, innerScan.TableName)
		if !ok {
			return minisqlErrors.ErrNoSuchTable{Name: innerScan.TableName}
		}
		innerFields := fieldsFromColumns(innerTable.Columns...)

		// Scan right (inner) table as outer loop
		rightRowChan := make(chan Row, 100)
		rightErrChan := make(chan error, 1)
		go func() {
			defer close(rightRowChan)
			if err := innerTable.sequentialScan(ctx, innerScan, innerFields, chanRowCallback(ctx, rightRowChan)); err != nil {
				rightErrChan <- err
			}
		}()

		joinConditions := OneOrMore{}
		if len(join.Conditions) > 0 {
			joinConditions = append(joinConditions, join.Conditions)
		}
		var joinFilter func(Row) (bool, error)

		for rightRow := range rightRowChan {
			select {
			case err := <-rightErrChan:
				return err
			default:
			}

			// Check whether any base row matches this right row.
			// We build a temporary combined row to evaluate join conditions.
			anyMatch := false
			baseRowChan := make(chan Row, 100)
			baseErrChan := make(chan error, 1)
			go func() {
				defer close(baseRowChan)
				if err := baseTable.sequentialScan(ctx, baseScan, baseFields, chanRowCallback(ctx, baseRowChan)); err != nil {
					baseErrChan <- err
				}
			}()

			for baseRow := range baseRowChan {
				select {
				case err := <-baseErrChan:
					return err
				default:
				}

				// After finding match, drain the channel to let the goroutine finish
				if anyMatch {
					continue
				}

				probe := combineRows(baseRow, rightRow, baseScan.TableAlias, innerScan.TableAlias)
				if joinFilter == nil {
					joinFilter = compileRowFilterForColumns(probe.Columns, joinConditions)
				}
				matches := true
				if joinFilter != nil {
					var err error
					matches, err = joinFilter(probe)
					if err != nil {
						return err
					}
				}
				if matches {
					anyMatch = true
				}
			}
			select {
			case err := <-baseErrChan:
				return err
			default:
			}

			if anyMatch {
				continue
			}

			// No base row matched — build combined row with NULLs for all tables
			// except the current right-join table which gets the actual row values.
			combined, err := p.buildRightJoinNullRow(ctx, provider, rightRow, joinIndex)
			if err != nil {
				return err
			}

			select {
			case filteredPipe <- combined:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		select {
		case err := <-rightErrChan:
			return err
		default:
		}
	}

	return nil
}

// buildRightJoinNullRow constructs a combined row for an unmatched RIGHT JOIN row.
// All tables except the one at joinIndex are NULL-filled; the table at joinIndex
// contains the actual rightRow values. Column order matches what the main join
// loop would produce so that selected fields resolve correctly.
func (p QueryPlan) buildRightJoinNullRow(ctx context.Context, provider TableProvider, rightRow Row, joinIndex int) (Row, error) {
	baseScan := p.Scans[0]
	baseTable, ok := provider.GetTable(ctx, baseScan.TableName)
	if !ok {
		return Row{}, minisqlErrors.ErrNoSuchTable{Name: baseScan.TableName}
	}

	// Build the combined row scan-by-scan (base first, then each join in order).
	// joinIndex==0 is special: first combine is combineRows, subsequent ones use combineRowsProgressive.

	// Determine the row for the first join slot
	join0 := p.Joins[0]
	join0Scan := p.Scans[join0.RightScanIndex]

	var firstInnerRow Row
	if joinIndex == 0 {
		firstInnerRow = rightRow
	} else {
		join0Table, ok := provider.GetTable(ctx, join0Scan.TableName)
		if !ok {
			return Row{}, minisqlErrors.ErrNoSuchTable{Name: join0Scan.TableName}
		}
		firstInnerRow = nullRowForColumns(join0Table.Columns)
	}

	combined := combineRows(nullRowForColumns(baseTable.Columns), firstInnerRow, baseScan.TableAlias, join0Scan.TableAlias)

	for i := 1; i < len(p.Joins); i++ {
		ji := p.Joins[i]
		jiScan := p.Scans[ji.RightScanIndex]

		var innerRow Row
		if i == joinIndex {
			innerRow = rightRow
		} else {
			jiTable, ok := provider.GetTable(ctx, jiScan.TableName)
			if !ok {
				return Row{}, minisqlErrors.ErrNoSuchTable{Name: jiScan.TableName}
			}
			innerRow = nullRowForColumns(jiTable.Columns)
		}

		combined = combineRowsProgressive(combined, innerRow, jiScan.TableAlias)
	}

	return combined, nil
}

// nullRowForColumns returns a Row where every value is NULL (Valid: false).
// Used to pad the outer side when a LEFT/RIGHT JOIN finds no matching row.
func nullRowForColumns(columns []Column) Row {
	values := make([]OptionalValue, len(columns))
	return NewRowWithValues(columns, values)
}

// combineRows concatenates columns and values from outer and inner rows (first join)
// Column names are prefixed with table aliases to avoid conflicts (e.g., "u.id", "o.id")
// prefixRowAlias returns a copy of row with each column name prefixed by
// "alias.".  When alias is empty the original row is returned unchanged.
func prefixRowAlias(row Row, alias string) Row {
	if alias == "" {
		return row
	}
	result := Row{
		Key:     row.Key,
		Columns: make([]Column, len(row.Columns)),
		Values:  make([]OptionalValue, len(row.Values)),
	}
	copy(result.Values, row.Values)
	for i, col := range row.Columns {
		result.Columns[i] = col
		result.Columns[i].Name = alias + "." + col.Name
	}
	return result
}

func combineRows(outerRow, innerRow Row, outerTableAlias, innerTableAlias string) Row {
	combinedColumns := make([]Column, 0, len(outerRow.Columns)+len(innerRow.Columns))

	// Add outer table columns, optionally prefixed with the table alias.
	for _, col := range outerRow.Columns {
		prefixedCol := col
		if outerTableAlias != "" {
			prefixedCol.Name = outerTableAlias + "." + col.Name
		}
		combinedColumns = append(combinedColumns, prefixedCol)
	}

	// Add inner table columns with table alias prefix
	for _, col := range innerRow.Columns {
		prefixedCol := col
		prefixedCol.Name = innerTableAlias + "." + col.Name
		combinedColumns = append(combinedColumns, prefixedCol)
	}

	combinedValues := make([]OptionalValue, 0, len(outerRow.Values)+len(innerRow.Values))
	combinedValues = append(combinedValues, outerRow.Values...)
	combinedValues = append(combinedValues, innerRow.Values...)

	return NewRowWithValues(combinedColumns, combinedValues)
}

// combineRowsProgressive adds a new table's row to an already-combined row (subsequent joins)
// The existing row already has prefixed column names from previous joins
func combineRowsProgressive(existingRow, newRow Row, newTableAlias string) Row {
	combinedColumns := make([]Column, 0, len(existingRow.Columns)+len(newRow.Columns))

	// Keep existing columns as-is (already prefixed)
	combinedColumns = append(combinedColumns, existingRow.Columns...)

	// Add new table columns with table alias prefix
	for _, col := range newRow.Columns {
		prefixedCol := col
		prefixedCol.Name = newTableAlias + "." + col.Name
		combinedColumns = append(combinedColumns, prefixedCol)
	}

	combinedValues := make([]OptionalValue, 0, len(existingRow.Values)+len(newRow.Values))
	combinedValues = append(combinedValues, existingRow.Values...)
	combinedValues = append(combinedValues, newRow.Values...)

	return NewRowWithValues(combinedColumns, combinedValues)
}
