package minisql

import (
	"context"
	"errors"
	"fmt"
)

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
// 1. Use index on inner table join column when available (index nested loop join).
// 2. Push single-table WHERE conditions into individual table scans.
func (t *Table) planJoinQuery(ctx context.Context, stmt Statement) (QueryPlan, error) {
	baseTableFilters, joinTableFilters := pushDownFilters(stmt.Conditions, stmt.TableAlias, stmt.Joins)

	plan := QueryPlan{
		Scans: []Scan{{
			TableName:  t.Name,
			TableAlias: stmt.TableAlias,
			Type:       ScanTypeSequential,
			Filters:    baseTableFilters,
		}},
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
				return fmt.Errorf("%w: %s", errTableDoesNotExist, fromScan.TableName)
			}
		}

		joinedTable, ok := t.provider.GetTable(ctx, join.TableName)
		if !ok {
			return fmt.Errorf("%w: %s", errTableDoesNotExist, join.TableName)
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
			// needs an unmatched-row pass that is harder to do with a hash table).
			if join.Type != Right {
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

		joinScan := Scan{
			TableName:  join.TableName,
			TableAlias: join.TableAlias,
			Type:       innerScanType,
			Filters:    joinTableFilters[join.TableAlias],
		}
		if innerScanType == ScanTypeIndexPoint && innerIndexInfo != nil {
			joinScan.IndexName = innerIndexInfo.Name
			joinScan.IndexColumns = innerIndexInfo.Columns
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

	// Check secondary indexes
	for name, idx := range t.SecondaryIndexes {
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
		return fmt.Errorf("%w: %s", errTableDoesNotExist, baseScan.TableName)
	}

	baseRowChan := make(chan Row, 100)
	baseErrChan := make(chan error, 1)
	baseFields := fieldsFromColumns(baseTable.Columns...)

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
		if err := p.executeJoinsForRow(ctx, provider, baseRow, 0, filteredPipe, hashTables); err != nil {
			return err
		}
	}

	select {
	case err := <-baseErrChan:
		return err
	default:
	}

	// RIGHT JOIN: emit right-table rows that had no matching base row.
	hasRightJoin := false
	for _, j := range p.Joins {
		if j.Type == Right {
			hasRightJoin = true
			break
		}
	}
	if hasRightJoin {
		if err := p.executeRightJoinPass(ctx, provider, filteredPipe); err != nil {
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
func (p QueryPlan) executeJoinsForRow(ctx context.Context, provider TableProvider, currentRow Row, joinIndex int, filteredPipe chan<- Row, hashTables map[int]*hashJoinBucket) error {
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
		return p.executeHashJoinForRow(ctx, currentRow, joinIndex, filteredPipe, hashTables)
	}

	// Nested-loop join (index point or sequential).
	innerScan := p.Scans[join.RightScanIndex]
	fromAlias := p.Scans[join.LeftScanIndex].TableAlias

	innerTable, ok := provider.GetTable(ctx, innerScan.TableName)
	if !ok {
		return fmt.Errorf("%w: %s", errTableDoesNotExist, innerScan.TableName)
	}

	innerFields := fieldsFromColumns(innerTable.Columns...)
	innerRowChan := make(chan Row, 100)
	innerErrChan := make(chan error, 1)

	if innerScan.Type == ScanTypeIndexPoint && join.InnerJoinColumn != "" {
		go func() {
			defer close(innerRowChan)

			var joinKeyValues []any

			if len(join.JoinColumnPairs) > 0 {
				joinKeyValues = make([]any, len(join.JoinColumnPairs))
				for i, pair := range join.JoinColumnPairs {
					var (
						keyValue OptionalValue
						ok       bool
					)
					if joinIndex == 0 {
						// currentRow is the unmodified base-table row — no alias prefix yet.
						keyValue, ok = currentRow.GetValue(pair.BaseTableColumn.Name)
					} else {
						keyValue, ok = currentRow.GetValue(fromAlias + "." + pair.BaseTableColumn.Name)
					}
					if !ok || !keyValue.Valid {
						return
					}
					joinKeyValues[i] = keyValue.Value
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
				if !ok || !keyValue.Valid {
					return
				}
				joinKeyValues = []any{keyValue.Value}
			}

			indexScan := innerScan
			indexScan.IndexKeys = joinKeyValues
			if err := innerTable.indexPointScan(ctx, indexScan, innerFields, chanRowCallback(ctx, innerRowChan)); err != nil {
				innerErrChan <- err
			}
		}()
	} else {
		go func() {
			defer close(innerRowChan)
			if err := innerTable.sequentialScan(ctx, innerScan, innerFields, chanRowCallback(ctx, innerRowChan)); err != nil {
				innerErrChan <- err
			}
		}()
	}

	joinConditions := OneOrMore{}
	if len(join.Conditions) > 0 {
		joinConditions = append(joinConditions, join.Conditions)
	}
	var joinFilter func(Row) (bool, error)

	matched := false
	for innerRow := range innerRowChan {
		select {
		case err := <-innerErrChan:
			return err
		default:
		}

		var combinedRow Row
		if joinIndex == 0 {
			combinedRow = combineRows(currentRow, innerRow, fromAlias, innerScan.TableAlias)
		} else {
			combinedRow = combineRowsProgressive(currentRow, innerRow, innerScan.TableAlias)
		}

		if joinFilter == nil {
			joinFilter = compileRowFilterForColumns(combinedRow.Columns, joinConditions)
		}

		matches := true
		if joinFilter != nil {
			var err error
			matches, err = joinFilter(combinedRow)
			if err != nil {
				return err
			}
		}

		if matches {
			matched = true
			if err := p.executeJoinsForRow(ctx, provider, combinedRow, joinIndex+1, filteredPipe, hashTables); err != nil {
				return err
			}
		}
	}

	select {
	case err := <-innerErrChan:
		return err
	default:
	}

	// LEFT JOIN: emit the outer row with NULL-filled inner columns when nothing matched.
	if !matched && join.Type == Left {
		nullInner := nullRowForColumns(innerTable.Columns)
		var combinedRow Row
		if joinIndex == 0 {
			combinedRow = combineRows(currentRow, nullInner, fromAlias, innerScan.TableAlias)
		} else {
			combinedRow = combineRowsProgressive(currentRow, nullInner, innerScan.TableAlias)
		}
		if err := p.executeJoinsForRow(ctx, provider, combinedRow, joinIndex+1, filteredPipe, hashTables); err != nil {
			return err
		}
	}

	return nil
}

// executeHashJoinForRow probes the pre-built hash table for joinIndex and
// recurses for matching inner rows.  Handles LEFT JOIN miss by emitting a
// NULL-padded combined row when the probe finds no matches.
func (p QueryPlan) executeHashJoinForRow(ctx context.Context, currentRow Row, joinIndex int, filteredPipe chan<- Row, hashTables map[int]*hashJoinBucket) error {
	join := p.Joins[joinIndex]
	innerScan := p.Scans[join.RightScanIndex]
	fromAlias := p.Scans[join.LeftScanIndex].TableAlias

	bucket := hashTables[joinIndex]

	probeKey := probeSideHashKey(join, currentRow, fromAlias, joinIndex)
	var matchingRows []Row
	if probeKey != "" && bucket != nil {
		matchingRows = bucket.rows[probeKey]
	}

	matched := false
	for _, innerRow := range matchingRows {
		var combinedRow Row
		if joinIndex == 0 {
			combinedRow = combineRows(currentRow, innerRow, fromAlias, innerScan.TableAlias)
		} else {
			combinedRow = combineRowsProgressive(currentRow, innerRow, innerScan.TableAlias)
		}
		matched = true
		if err := p.executeJoinsForRow(ctx, nil, combinedRow, joinIndex+1, filteredPipe, hashTables); err != nil {
			return err
		}
	}

	// LEFT JOIN: no matching inner row — emit outer row with NULL inner columns.
	if !matched && join.Type == Left {
		var innerColumns []Column
		if bucket != nil {
			innerColumns = bucket.innerColumns
		}
		nullInner := nullRowForColumns(innerColumns)
		var combinedRow Row
		if joinIndex == 0 {
			combinedRow = combineRows(currentRow, nullInner, fromAlias, innerScan.TableAlias)
		} else {
			combinedRow = combineRowsProgressive(currentRow, nullInner, innerScan.TableAlias)
		}
		if err := p.executeJoinsForRow(ctx, nil, combinedRow, joinIndex+1, filteredPipe, hashTables); err != nil {
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
		return fmt.Errorf("%w: %s", errTableDoesNotExist, baseScan.TableName)
	}
	baseFields := fieldsFromColumns(baseTable.Columns...)

	for joinIndex, join := range p.Joins {
		if join.Type != Right {
			continue
		}

		innerScan := p.Scans[join.RightScanIndex]
		innerTable, ok := provider.GetTable(ctx, innerScan.TableName)
		if !ok {
			return fmt.Errorf("%w: %s", errTableDoesNotExist, innerScan.TableName)
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
		return Row{}, fmt.Errorf("%w: %s", errTableDoesNotExist, baseScan.TableName)
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
			return Row{}, fmt.Errorf("%w: %s", errTableDoesNotExist, join0Scan.TableName)
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
				return Row{}, fmt.Errorf("%w: %s", errTableDoesNotExist, jiScan.TableName)
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
func combineRows(outerRow, innerRow Row, outerTableAlias, innerTableAlias string) Row {
	combinedColumns := make([]Column, 0, len(outerRow.Columns)+len(innerRow.Columns))

	// Add outer table columns with table alias prefix
	for _, col := range outerRow.Columns {
		prefixedCol := col
		prefixedCol.Name = outerTableAlias + "." + col.Name
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
