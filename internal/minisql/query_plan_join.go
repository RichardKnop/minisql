package minisql

import (
	"context"
	"fmt"
)

// planJoinQuery creates an optimized query plan for JOINs
// (supports star schema: multiple tables joining to base table)
// Optimizations:
// 1. Choose smaller table as outer table to minimize inner scans
// 2. Use index on inner table join column when available (index nested loop join)
// For star schema, the base table is scanned once, and each joined table is optimized independently
func (t *Table) planJoinQuery(ctx context.Context, stmt Statement) (QueryPlan, error) {
	// Push down WHERE conditions to individual table scans
	baseTableFilters, joinTableFilters := pushDownFilters(stmt.Conditions, stmt.TableAlias, stmt.Joins)

	// Initialize plan with base table scan
	plan := QueryPlan{
		Scans: []Scan{{
			TableName:  t.Name,
			TableAlias: stmt.TableAlias,
			Type:       ScanTypeSequential, // Base table is always scanned sequentially
			Filters:    baseTableFilters,
		}},
		Joins:        make([]JoinPlan, 0, len(stmt.Joins)),
		OrderBy:      stmt.OrderBy,
		SortInMemory: len(stmt.OrderBy) > 0, // Always sort in memory for JOINs with ORDER BY
	}

	// Process each join independently (star schema: all join to base table)
	for _, join := range stmt.Joins {
		// Get the joined table
		joinedTable, ok := t.provider.GetTable(ctx, join.TableName)
		if !ok {
			return QueryPlan{}, fmt.Errorf("%w: %s", errTableDoesNotExist, join.TableName)
		}

		// Extract join column names from ON conditions
		baseJoinCol, joinJoinCol := extractJoinColumns(join.Conditions, stmt.TableAlias, join.TableAlias)

		// Check for index on joined table's join column
		joinTableIndex := joinedTable.findIndexOnColumn(joinJoinCol)

		// Determine scan strategy for this join
		// For star schema, base table is always outer (left), joined tables are inner (right)
		// We can optimize by using index on joined table if available
		var (
			innerScanType  ScanType
			innerIndexInfo *IndexInfo
		)

		if joinTableIndex != nil {
			// Joined table has index on join column - use index nested loop join
			innerScanType = ScanTypeIndexPoint
			innerIndexInfo = joinTableIndex
		} else {
			// No index available - use sequential scan
			innerScanType = ScanTypeSequential
		}

		// Create scan for joined table
		joinScan := Scan{
			TableName:  join.TableName,
			TableAlias: join.TableAlias,
			Type:       innerScanType,
			Filters:    joinTableFilters[join.TableAlias],
		}

		// For index scan, set up the index info
		if innerScanType == ScanTypeIndexPoint && innerIndexInfo != nil {
			joinScan.IndexName = innerIndexInfo.Name
			joinScan.IndexColumns = innerIndexInfo.Columns
			// The actual key value will be set per base row during execution
		}

		// Add scan to plan
		plan.Scans = append(plan.Scans, joinScan)

		// Create join plan entry
		// In star schema, left side is always the base table (scan index 0)
		// Right side is the current joined table
		plan.Joins = append(plan.Joins, JoinPlan{
			Type:            join.Type,
			LeftScanIndex:   0, // Base table is always scan index 0
			RightScanIndex:  len(plan.Scans) - 1,
			Conditions:      join.Conditions,
			OuterJoinColumn: baseJoinCol,
			InnerJoinColumn: joinJoinCol,
		})
	}

	return plan, nil
}

// extractJoinColumns extracts the column names from JOIN ON conditions
// Returns (baseTableColumn, joinTableColumn)
func extractJoinColumns(conditions Conditions, baseAlias, joinAlias string) (string, string) {
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
		if field1.AliasPrefix == baseAlias && field2.AliasPrefix == joinAlias {
			return field1.Name, field2.Name
		}
		if field1.AliasPrefix == joinAlias && field2.AliasPrefix == baseAlias {
			return field2.Name, field1.Name
		}
	}

	return "", ""
}

// findIndexOnColumn finds an index (primary key, unique, or secondary) on the given column
func (t *Table) findIndexOnColumn(columnName string) *IndexInfo {
	if columnName == "" {
		return nil
	}

	// Check primary key
	if len(t.PrimaryKey.Columns) == 1 && t.PrimaryKey.Columns[0].Name == columnName {
		return &IndexInfo{
			Name:    t.PrimaryKey.Name,
			Columns: t.PrimaryKey.Columns,
		}
	}

	// Check unique indexes
	for name, idx := range t.UniqueIndexes {
		if len(idx.Columns) == 1 && idx.Columns[0].Name == columnName {
			return &IndexInfo{
				Name:    name,
				Columns: idx.Columns,
			}
		}
	}

	// Check secondary indexes
	for name, idx := range t.SecondaryIndexes {
		if len(idx.Columns) == 1 && idx.Columns[0].Name == columnName {
			return &IndexInfo{
				Name:    name,
				Columns: idx.Columns,
			}
		}
	}

	return nil
}

// pushDownFilters separates WHERE conditions by table alias, pushing filters to appropriate scans
func pushDownFilters(conditions OneOrMore, baseTableAlias string, joins []Join) (OneOrMore, map[string]OneOrMore) {
	var (
		baseFilters = OneOrMore{}
		joinFilters = make(map[string]OneOrMore)
	)

	// Initialize map for each joined table
	for _, join := range joins {
		joinFilters[join.TableAlias] = OneOrMore{}
	}

	// Distribute conditions to appropriate tables, preserving OR group structure
	for _, group := range conditions {
		// Create new groups for each table
		baseGroup := Conditions{}
		joinGroups := make(map[string]Conditions)
		for _, join := range joins {
			joinGroups[join.TableAlias] = Conditions{}
		}

		// Distribute conditions within this group
		for _, condition := range group {
			// Check which table this condition belongs to
			tableAlias := getConditionTableAlias(condition, baseTableAlias, joins)

			if tableAlias == baseTableAlias {
				baseGroup = append(baseGroup, condition)
			} else if _, exists := joinGroups[tableAlias]; exists {
				joinGroups[tableAlias] = append(joinGroups[tableAlias], condition)
			} else {
				// Cross-table condition or unknown - keep in base for now
				baseGroup = append(baseGroup, condition)
			}
		}

		// Add non-empty groups to the respective filters
		if len(baseGroup) > 0 {
			baseFilters = append(baseFilters, baseGroup)
		}
		for alias, group := range joinGroups {
			if len(group) > 0 {
				joinFilters[alias] = append(joinFilters[alias], group)
			}
		}
	}

	return baseFilters, joinFilters
}

// getConditionTableAlias determines which table a condition belongs to based on field prefix
func getConditionTableAlias(condition Condition, baseTableAlias string, joins []Join) string {
	// Check operand1
	if condition.Operand1.Type == OperandField {
		if field, ok := condition.Operand1.Value.(Field); ok {
			if field.AliasPrefix != "" {
				if field.AliasPrefix == baseTableAlias {
					return baseTableAlias
				}
				for _, join := range joins {
					if field.AliasPrefix == join.TableAlias {
						return join.TableAlias
					}
				}
			}
		}
	}

	// Check operand2 as well (for field-to-field comparisons)
	if condition.Operand2.Type == OperandField {
		if field, ok := condition.Operand2.Value.(Field); ok {
			if field.AliasPrefix != "" {
				if field.AliasPrefix == baseTableAlias {
					return baseTableAlias
				}
				for _, join := range joins {
					if field.AliasPrefix == join.TableAlias {
						return join.TableAlias
					}
				}
			}
		}
	}

	// If no alias prefix found, assume base table
	return baseTableAlias
}

// executeNestedLoopJoin performs nested loop join execution for multi-table queries
func (p QueryPlan) executeNestedLoopJoin(ctx context.Context, provider TableProvider, selectedFields []Field, filteredPipe chan<- Row) error {
	if len(p.Joins) == 0 {
		return fmt.Errorf("no joins to execute")
	}

	// For star schema, we process joins sequentially
	// Start with base table (scan 0), then join each additional table

	// Get base table
	baseScan := p.Scans[0]
	baseTable, ok := provider.GetTable(ctx, baseScan.TableName)
	if !ok {
		return fmt.Errorf("%w: %s", errTableDoesNotExist, baseScan.TableName)
	}

	// Scan base table
	baseRowChan := make(chan Row, 100)
	baseErrChan := make(chan error, 1)
	baseFields := fieldsFromColumns(baseTable.Columns...)

	go func() {
		defer close(baseRowChan)
		if err := baseTable.sequentialScan(ctx, baseScan, baseFields, baseRowChan); err != nil {
			baseErrChan <- err
		}
	}()

	// Process each base row through all joins
	for baseRow := range baseRowChan {
		// Check for errors
		select {
		case err := <-baseErrChan:
			return err
		default:
		}

		// Execute all joins for this base row
		if err := p.executeJoinsForRow(ctx, provider, baseRow, baseScan.TableAlias, 0, filteredPipe); err != nil {
			return err
		}
	}

	// Check for final errors
	select {
	case err := <-baseErrChan:
		return err
	default:
	}

	return nil
}

// executeJoinsForRow recursively executes joins for a given row
// For star schema, all joins are against the base table
// joinIndex indicates which join we're currently processing
func (p QueryPlan) executeJoinsForRow(ctx context.Context, provider TableProvider, currentRow Row, baseTableAlias string, joinIndex int, filteredPipe chan<- Row) error {
	// Base case: all joins processed, send the row
	if joinIndex >= len(p.Joins) {
		select {
		case filteredPipe <- currentRow:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	// Get current join
	join := p.Joins[joinIndex]
	innerScan := p.Scans[join.RightScanIndex]

	// Get inner table
	innerTable, ok := provider.GetTable(ctx, innerScan.TableName)
	if !ok {
		return fmt.Errorf("%w: %s", errTableDoesNotExist, innerScan.TableName)
	}

	// Get fields for inner table
	innerFields := fieldsFromColumns(innerTable.Columns...)

	// Create channel for inner rows
	innerRowChan := make(chan Row, 100)
	innerErrChan := make(chan error, 1)

	// Scan inner table - use index if available
	if innerScan.Type == ScanTypeIndexPoint && join.InnerJoinColumn != "" {
		// Index nested loop join: look up matching rows using index
		go func() {
			defer close(innerRowChan)

			// Get join key value from current row
			// For first join, column is not prefixed; for subsequent joins, it's prefixed with base table alias
			var joinKeyValue OptionalValue
			var ok bool

			if joinIndex == 0 {
				// First join - column not yet prefixed
				joinKeyValue, ok = currentRow.GetValue(join.OuterJoinColumn)
			} else {
				// Subsequent join - column is prefixed with base table alias
				prefixedCol := baseTableAlias + "." + join.OuterJoinColumn
				joinKeyValue, ok = currentRow.GetValue(prefixedCol)
			}

			if !ok || !joinKeyValue.Valid {
				return // No match possible if join key is NULL or missing
			}

			// Create a modified scan with the specific key value
			indexScan := innerScan
			indexScan.IndexKeys = []any{joinKeyValue.Value}

			// Use index scan to find matching rows
			if err := innerTable.indexPointScan(ctx, indexScan, innerFields, innerRowChan); err != nil {
				innerErrChan <- err
			}
		}()
	} else {
		// Standard nested loop: sequential scan of inner table
		go func() {
			defer close(innerRowChan)
			if err := innerTable.sequentialScan(ctx, innerScan, innerFields, innerRowChan); err != nil {
				innerErrChan <- err
			}
		}()
	}

	// For each matching inner row, combine and continue with next join
	for innerRow := range innerRowChan {
		select {
		case err := <-innerErrChan:
			return err
		default:
		}

		// Combine current row with inner row
		var combinedRow Row
		if joinIndex == 0 {
			// First join - combine base row with inner row
			combinedRow = combineRows(currentRow, innerRow, baseTableAlias, innerScan.TableAlias)
		} else {
			// Subsequent join - current row is already combined, add inner row
			combinedRow = combineRowsProgressive(currentRow, innerRow, innerScan.TableAlias)
		}

		// Evaluate JOIN conditions
		joinConditions := OneOrMore{}
		if len(join.Conditions) > 0 {
			joinConditions = append(joinConditions, join.Conditions)
		}

		matches, err := evaluateJoinConditions(combinedRow, joinConditions)
		if err != nil {
			return err
		}

		if matches {
			// Continue with next join (or output if this was the last join)
			if err := p.executeJoinsForRow(ctx, provider, combinedRow, baseTableAlias, joinIndex+1, filteredPipe); err != nil {
				return err
			}
		}
	}

	// Check for errors from inner scan
	select {
	case err := <-innerErrChan:
		return err
	default:
	}

	return nil
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

// evaluateJoinConditions checks if a combined row satisfies all JOIN conditions
func evaluateJoinConditions(row Row, conditions OneOrMore) (bool, error) {
	if len(conditions) == 0 {
		return true, nil // No conditions means match all (cross product)
	}

	// Use existing CheckOneOrMore logic
	return row.CheckOneOrMore(conditions)
}
