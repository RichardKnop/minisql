package minisql

// markCoveringIndexes iterates the plan's scans and sets CoveringIndex = true
// on every index scan whose columns fully cover the query's column requirements.
// Only SELECT queries are eligible: DELETE and UPDATE always need the full table
// row for index maintenance, even when using an index scan to locate rows.
// Sequential scans are never marked; CoveringIndex is only meaningful on index
// scan types (ScanTypeIndexAll, ScanTypeIndexPoint, ScanTypeIndexRange,
// ScanTypeIndexFirst, ScanTypeIndexLast).
func (p *QueryPlan) markCoveringIndexes(stmt Statement) {
	if stmt.Kind != Select {
		return
	}
	for i, scan := range p.Scans {
		if scan.Type == ScanTypeSequential || scan.Type == ScanTypeIndexIntersect || scan.Type == ScanTypeFullText || scan.Type == ScanTypeInverted {
			continue
		}
		if coveringIndexEligible(stmt, scan.IndexColumns) {
			p.Scans[i].CoveringIndex = true
		}
	}
}

// coveringIndexEligible reports whether the index described by indexColumns
// covers every column the query needs, making a table-row fetch unnecessary.
//
// A query is eligible when:
//   - It is not SELECT * (needs all columns).
//   - Every column referenced in SELECT fields, WHERE conditions, ORDER BY,
//     GROUP BY, and aggregate functions is present in indexColumns.
//   - No WHERE condition involves a NULL check (IS NULL / IS NOT NULL), because
//     rows with NULL keys may not be present in the index.
//
// SELECT COUNT(*) with no column references is always eligible when an index
// scan has already been chosen — the executor only needs to count entries.
func coveringIndexEligible(stmt Statement, indexColumns []Column) bool {
	// SELECT * is never covered (all table columns needed).
	if stmt.IsSelectAll() {
		return false
	}

	// SELECT COUNT(*) — only needs row count, no column values.
	if stmt.IsSelectCountAll() {
		return true
	}

	// If no specific columns are listed and this isn't a recognised aggregate form,
	// we can't determine coverage — default to false.
	if len(stmt.Fields) == 0 && !stmt.IsSelectAggregate() {
		return false
	}

	// Build a set of index column names for O(1) lookup.
	covered := make(map[string]struct{}, len(indexColumns))
	for _, c := range indexColumns {
		covered[c.Name] = struct{}{}
	}

	// SELECT fields (resolve expression source columns, not output aliases).
	for _, f := range exprSourceFields(stmt.Fields) {
		if _, ok := covered[f.Name]; !ok {
			return false
		}
	}

	// Aggregate source columns (SUM(col), AVG(col), MIN(col), MAX(col)).
	for _, agg := range stmt.Aggregates {
		if agg.Column == "" {
			continue // COUNT(*) etc.
		}
		if _, ok := covered[agg.Column]; !ok {
			return false
		}
	}

	// GROUP BY columns.
	for _, gb := range stmt.GroupBy {
		if _, ok := covered[gb.Name]; !ok {
			return false
		}
	}

	// ORDER BY columns.
	for _, ob := range stmt.OrderBy {
		if _, ok := covered[ob.Field.Name]; !ok {
			return false
		}
	}

	// WHERE conditions: check for NULL checks and unreferenced columns.
	for _, group := range stmt.Conditions {
		for _, cond := range group {
			// IS NULL / IS NOT NULL — index may not contain NULL-keyed entries.
			if cond.Operand2.Type == OperandNull {
				return false
			}
			if cond.Operand1.Type == OperandField {
				if f, ok := cond.Operand1.Value.(Field); ok {
					if _, inIndex := covered[f.Name]; !inIndex {
						return false
					}
				}
			}
			if cond.Operand2.Type == OperandField {
				if f, ok := cond.Operand2.Value.(Field); ok {
					if _, inIndex := covered[f.Name]; !inIndex {
						return false
					}
				}
			}
		}
	}

	return true
}

// rowFromIndexKey constructs a Row directly from an index key without reading
// the table page.  For a single-column index the key is the raw column value;
// for a composite index it is a CompositeKey that already carries column
// definitions and values.  rowID is stored as Row.Key so downstream code
// (filters, projections) can identify the row.
func rowFromIndexKey(key any, indexColumns []Column, rowID RowID) Row {
	if ck, ok := key.(CompositeKey); ok {
		vals := make([]OptionalValue, len(ck.Columns))
		for i := range ck.Columns {
			vals[i] = OptionalValue{Value: ck.Values[i], Valid: true}
		}
		row := NewRowWithValues(ck.Columns, vals)
		row.Key = rowID
		return row
	}

	// Single-column index.
	col := indexColumns[0]
	vals := []OptionalValue{{Value: key, Valid: true}}
	row := NewRowWithValues([]Column{col}, vals)
	row.Key = rowID
	return row
}
