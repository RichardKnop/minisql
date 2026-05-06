package minisql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// ErrNoMoreRows ...
var ErrNoMoreRows = errors.New("no more rows")

// Select ...
func (t *Table) Select(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if stmt.Kind != Select {
		return StatementResult{}, fmt.Errorf("invalid statement kind for SELECT: %v", stmt.Kind)
	}

	// Fast path: COUNT(*) with no WHERE clause and no JOIN — walk leaf page
	// headers without deserialising any row data.
	// Virtual tables (derived FROM subqueries) must skip this and go through
	// the normal materialising path so the in-memory rows are counted instead.
	if stmt.IsSelectCountAll() && len(stmt.Conditions) == 0 && len(stmt.Joins) == 0 && t.virtualRows == nil {
		return t.countAllLeafWalk(ctx)
	}

	// Create query plan
	plan, err := t.PlanQuery(ctx, stmt)
	if err != nil {
		return StatementResult{}, err
	}

	t.logger.Debug("query plan", zap.String("query type", "SELECT"), zap.Any("plan", plan))

	// Only fetch fields included in the SELECT query or fields needed for WHERE conditions
	// TODO - handle * plus other fields, for example SELECT *, a, b FROM table WHERE c = 1
	var (
		requestedFields []Field
		selectedFields  []Field
	)
	switch {
	case stmt.IsSelectAll():
		requestedFields = fieldsFromColumns(t.Columns...)
		selectedFields = requestedFields
	case stmt.IsSelectAggregate():
		// For aggregate queries, fetch the actual source columns (not the synthetic output names).
		// For GROUP BY queries also include the GROUP BY columns.
		colSet := make(map[string]struct{})
		for _, f := range stmt.GroupBy {
			colSet[f.Name] = struct{}{}
		}
		for _, agg := range stmt.Aggregates {
			if agg.Column != "" {
				colSet[agg.Column] = struct{}{}
			}
		}
		for colName := range colSet {
			requestedFields = append(requestedFields, Field{Name: colName})
		}
		selectedFields = requestedFields
	default:
		if !stmt.IsSelectCountAll() {
			requestedFields = stmt.Fields
			// For selectedFields, replace computed expression fields with the
			// underlying column references they read from (so the scan fetches
			// the right data from disk).  Plain column fields pass through
			// unchanged.
			selectedFields = exprSourceFields(requestedFields)
		}

		// Pre-allocate for WHERE condition fields (estimate: 2 operands per condition)
		conditionFieldsEstimate := 0
		for _, conditions := range stmt.Conditions {
			conditionFieldsEstimate += len(conditions) * 2
		}
		if cap(selectedFields) == 0 && conditionFieldsEstimate > 0 {
			selectedFields = make([]Field, 0, conditionFieldsEstimate)
		}

		for _, conditions := range stmt.Conditions {
			for _, cond := range conditions {
				if cond.Operand1.Type == OperandField {
					selectedFields = append(selectedFields, cond.Operand1.Value.(Field))
				}
				if cond.Operand2.Type == OperandField {
					selectedFields = append(selectedFields, cond.Operand2.Value.(Field))
				}
			}
		}
	}

	// Fast path: streaming queries that require no sort, no GROUP BY, and no
	// aggregates, and have no JOINs. This avoids materializing an intermediate
	// []Row buffer from the plan scan.
	//
	// With LIMIT, scanning can stop early once enough rows are projected.
	// Without LIMIT, we still benefit by eliminating one full intermediate
	// allocation (rows), even though projected rows keep growing dynamically.
	//
	// JOIN queries are excluded because plan.Execute launches a goroutine internally;
	// returning errLimitReached from the callback while that goroutine is still
	// writing to the channel would leak the goroutine.
	if !stmt.IsSelectCountAll() &&
		!stmt.IsSelectGroupBy() &&
		!stmt.IsSelectAggregate() &&
		!plan.SortInMemory &&
		len(plan.Joins) == 0 {
		return t.selectStreamingDirect(ctx, stmt, plan, selectedFields, requestedFields)
	}

	// Materialising path: buffer every matching row, then dispatch.
	// Required for COUNT (needs a total), GROUP BY, aggregates, ORDER BY (sort),
	// and JOIN (goroutine-based execution inside plan.Execute).
	var rows []Row
	if err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		rows = append(rows, row)
		return nil
	}); err != nil {
		return StatementResult{}, err
	}

	if stmt.IsSelectCountAll() {
		return t.selectCount(rows)
	}

	// Grouped aggregate queries (GROUP BY) materialise all rows and group them.
	if stmt.IsSelectGroupBy() {
		return t.selectGroupBy(ctx, stmt, rows)
	}

	// Aggregate queries (SUM, AVG, MIN, MAX) always materialise all rows.
	if stmt.IsSelectAggregate() {
		return t.selectAggregate(ctx, stmt, rows)
	}

	// If we are sorting in memory, it means we are doing sequential scan as we need to
	// gather all rows first to be able to determine correct order.
	if plan.SortInMemory {
		return t.selectWithSort(stmt, plan, rows, requestedFields)
	}

	// JOIN streaming: already buffered above, project during iteration.
	return t.selectStreaming(stmt, rows, requestedFields)
}

func (t *Table) selectCount(rows []Row) (StatementResult, error) {
	count := int64(len(rows))
	return StatementResult{
		Columns: []Column{{Name: "COUNT(*)"}},
		Rows: NewSingleRowIterator(NewRowWithValues(
			[]Column{{Name: "COUNT(*)"}},
			[]OptionalValue{{Valid: true, Value: count}},
		)),
	}, nil
}

// countAllLeafWalk counts every row in the table.
//
// Fast path: if a row-count getter has been registered (set by the Database
// after loading the table), returns the cached count in O(1) without any I/O.
//
// Fallback: walks the B+ tree leaf page chain and sums Header.Cells on each
// page — O(leaf pages), no row data read or deserialised.
//
// This is only valid for COUNT(*) with no WHERE clause and no JOIN.
func (t *Table) countAllLeafWalk(ctx context.Context) (StatementResult, error) {
	var count int64
	if t.getRowCount != nil {
		count = t.getRowCount()
	} else {
		cursor, err := t.SeekFirst(ctx)
		if err != nil {
			return StatementResult{}, fmt.Errorf("count all: %w", err)
		}

		pageIdx := cursor.PageIdx
		for {
			select {
			case <-ctx.Done():
				return StatementResult{}, ctx.Err()
			default:
			}

			page, err := t.pager.ReadPage(ctx, pageIdx)
			if err != nil {
				return StatementResult{}, fmt.Errorf("count all: read page %d: %w", pageIdx, err)
			}
			count += int64(page.LeafNode.Header.Cells)

			if page.LeafNode.Header.NextLeaf == 0 {
				break
			}
			pageIdx = page.LeafNode.Header.NextLeaf
		}
	}

	return StatementResult{
		Columns: []Column{
			{Name: "COUNT(*)"},
		},
		Rows: NewSingleRowIterator(NewRowWithValues(
			[]Column{{Name: "COUNT(*)"}},
			[]OptionalValue{{Valid: true, Value: count}},
		)),
	}, nil
}

// aggState holds the running accumulator for a single aggregate expression.
type aggState struct {
	min       OptionalValue
	max       OptionalValue
	count     int64
	sumI      int64
	sumF      float64
	useIntSum bool
	hasValue  bool
}

func (t *Table) selectAggregate(ctx context.Context, stmt Statement, rows []Row) (StatementResult, error) {
	states := make([]aggState, len(stmt.Aggregates))

	// Determine whether integer or float accumulation should be used for SUM/AVG.
	for i, agg := range stmt.Aggregates {
		if agg.Kind != AggregateSum && agg.Kind != AggregateAvg {
			continue
		}
		if col, ok := stmt.ColumnByName(agg.Column); ok {
			states[i].useIntSum = col.Kind == Int4 || col.Kind == Int8
		}
	}

	for _, row := range rows {
		for i, agg := range stmt.Aggregates {
			switch agg.Kind {
			case AggregateCount:
				states[i].count += 1

			case AggregateSum, AggregateAvg:
				val, ok := row.GetValue(agg.Column)
				if !ok || !val.Valid {
					continue // SQL aggregate functions ignore NULLs
				}
				states[i].count += 1
				states[i].hasValue = true
				if states[i].useIntSum {
					switch v := val.Value.(type) {
					case int64:
						states[i].sumI += v
					case int32:
						states[i].sumI += int64(v)
					}
				} else {
					switch v := val.Value.(type) {
					case float64:
						states[i].sumF += v
					case float32:
						states[i].sumF += float64(v)
					}
				}

			case AggregateMin:
				val, ok := row.GetValue(agg.Column)
				if !ok || !val.Valid {
					continue
				}
				if !states[i].hasValue || compareValues(val, states[i].min) < 0 {
					states[i].min = val
					states[i].hasValue = true
				}

			case AggregateMax:
				val, ok := row.GetValue(agg.Column)
				if !ok || !val.Valid {
					continue
				}
				if !states[i].hasValue || compareValues(val, states[i].max) > 0 {
					states[i].max = val
					states[i].hasValue = true
				}
			}
		}
	}
	_ = ctx // ctx kept for signature compat; no blocking ops remain

	// Build result columns and a single result row.
	resultColumns := make([]Column, len(stmt.Aggregates))
	resultValues := make([]OptionalValue, len(stmt.Aggregates))

	for i, agg := range stmt.Aggregates {
		fieldName := stmt.Fields[i].OutputName() // respects AS alias
		switch agg.Kind {
		case AggregateCount:
			resultColumns[i] = Column{Name: fieldName, Kind: Int8}
			resultValues[i] = OptionalValue{Valid: true, Value: states[i].count}

		case AggregateSum:
			switch {
			case !states[i].hasValue:
				resultColumns[i] = Column{Name: fieldName, Kind: Int8}
				resultValues[i] = OptionalValue{} // NULL — no non-NULL rows
			case states[i].useIntSum:
				resultColumns[i] = Column{Name: fieldName, Kind: Int8}
				resultValues[i] = OptionalValue{Valid: true, Value: states[i].sumI}
			default:
				resultColumns[i] = Column{Name: fieldName, Kind: Double}
				resultValues[i] = OptionalValue{Valid: true, Value: states[i].sumF}
			}

		case AggregateAvg:
			resultColumns[i] = Column{Name: fieldName, Kind: Double}
			switch {
			case !states[i].hasValue || states[i].count == 0:
				resultValues[i] = OptionalValue{} // NULL
			case states[i].useIntSum:
				resultValues[i] = OptionalValue{Valid: true, Value: float64(states[i].sumI) / float64(states[i].count)}
			default:
				resultValues[i] = OptionalValue{Valid: true, Value: states[i].sumF / float64(states[i].count)}
			}

		case AggregateMin:
			if col, ok := t.ColumnByName(agg.Column); ok {
				resultColumns[i] = Column{Name: fieldName, Kind: col.Kind, Size: col.Size}
			}
			if states[i].hasValue {
				resultValues[i] = states[i].min
			}

		case AggregateMax:
			if col, ok := t.ColumnByName(agg.Column); ok {
				resultColumns[i] = Column{Name: fieldName, Kind: col.Kind, Size: col.Size}
			}
			if states[i].hasValue {
				resultValues[i] = states[i].max
			}
		}
	}

	return StatementResult{
		Columns: resultColumns,
		Rows:    NewSingleRowIterator(NewRowWithValues(resultColumns, resultValues)),
	}, nil
}

// groupState holds per-group accumulators for a GROUP BY query.
type groupState struct {
	groupValues []OptionalValue // values of the GROUP BY columns for this group
	aggStates   []aggState
}

func (t *Table) selectGroupBy(ctx context.Context, stmt Statement, rows []Row) (StatementResult, error) {
	// Determine integer vs float accumulation mode for SUM/AVG per aggregate.
	useIntSum := make([]bool, len(stmt.Aggregates))
	for i, agg := range stmt.Aggregates {
		if agg.Kind != AggregateSum && agg.Kind != AggregateAvg {
			continue
		}
		if col, ok := stmt.ColumnByName(agg.Column); ok {
			useIntSum[i] = col.Kind.IsInt()
		}
	}

	// Build a name→index map for GROUP BY fields (for looking up values after scan).
	groupByIdx := make(map[string]int, len(stmt.GroupBy))
	for i, f := range stmt.GroupBy {
		groupByIdx[f.Name] = i
	}

	// groups maps a group key string to its accumulated state.
	// groupOrder preserves insertion order so output is deterministic.
	groups := make(map[string]*groupState)
	groupOrder := make([]string, 0)

	for _, row := range rows {
		// Compute group key from the GROUP BY columns.
		groupRow := row.OnlyFields(stmt.GroupBy...)
		key := groupRow.rowDistinctKey()

		gs, exists := groups[key]
		if !exists {
			states := make([]aggState, len(stmt.Aggregates))
			for i := range states {
				states[i].useIntSum = useIntSum[i]
			}
			gs = &groupState{
				groupValues: make([]OptionalValue, len(stmt.GroupBy)),
				aggStates:   states,
			}
			// Record the group column values from this first row.
			copy(gs.groupValues, groupRow.Values)
			groups[key] = gs
			groupOrder = append(groupOrder, key)
		}

		// Accumulate aggregates for this group.
		for i, agg := range stmt.Aggregates {
			switch agg.Kind {
			case 0:
				// Non-aggregate GROUP BY column — no accumulation needed.
			case AggregateCount:
				gs.aggStates[i].count += 1
			case AggregateSum, AggregateAvg:
				val, ok := row.GetValue(agg.Column)
				if !ok || !val.Valid {
					continue
				}
				gs.aggStates[i].count += 1
				gs.aggStates[i].hasValue = true
				if gs.aggStates[i].useIntSum {
					switch v := val.Value.(type) {
					case int64:
						gs.aggStates[i].sumI += v
					case int32:
						gs.aggStates[i].sumI += int64(v)
					}
				} else {
					switch v := val.Value.(type) {
					case float64:
						gs.aggStates[i].sumF += v
					case float32:
						gs.aggStates[i].sumF += float64(v)
					}
				}
			case AggregateMin:
				val, ok := row.GetValue(agg.Column)
				if !ok || !val.Valid {
					continue
				}
				if !gs.aggStates[i].hasValue || compareValues(val, gs.aggStates[i].min) < 0 {
					gs.aggStates[i].min = val
					gs.aggStates[i].hasValue = true
				}
			case AggregateMax:
				val, ok := row.GetValue(agg.Column)
				if !ok || !val.Valid {
					continue
				}
				if !gs.aggStates[i].hasValue || compareValues(val, gs.aggStates[i].max) > 0 {
					gs.aggStates[i].max = val
					gs.aggStates[i].hasValue = true
				}
			}
		}
	}
	_ = ctx // ctx kept for signature compat; no blocking ops remain

	// Build result column metadata.
	resultColumns := make([]Column, len(stmt.Fields))
	for i, field := range stmt.Fields {
		agg := stmt.Aggregates[i]
		colName := field.OutputName() // respects AS alias
		switch agg.Kind {
		case 0:
			// GROUP BY column — use the actual table column type.
			if col, ok := t.ColumnByName(field.Name); ok {
				resultColumns[i] = col
				resultColumns[i].Name = colName
			}
		case AggregateCount:
			resultColumns[i] = Column{Name: colName, Kind: Int8}
		case AggregateSum:
			if useIntSum[i] {
				resultColumns[i] = Column{Name: colName, Kind: Int8}
			} else {
				resultColumns[i] = Column{Name: colName, Kind: Double}
			}
		case AggregateAvg:
			resultColumns[i] = Column{Name: colName, Kind: Double}
		case AggregateMin, AggregateMax:
			if col, ok := t.ColumnByName(agg.Column); ok {
				resultColumns[i] = Column{Name: colName, Kind: col.Kind, Size: col.Size}
			}
		}
	}

	// Build one result row per group, applying HAVING filter.
	resultRows := make([]Row, 0, len(groupOrder))
	for _, key := range groupOrder {
		gs := groups[key]
		values := make([]OptionalValue, len(stmt.Fields))
		for i, agg := range stmt.Aggregates {
			switch agg.Kind {
			case 0:
				// Look up the group column value by field name.
				if idx, ok := groupByIdx[stmt.Fields[i].Name]; ok {
					values[i] = gs.groupValues[idx]
				}
			case AggregateCount:
				values[i] = OptionalValue{Valid: true, Value: gs.aggStates[i].count}
			case AggregateSum:
				st := gs.aggStates[i]
				if st.hasValue {
					if st.useIntSum {
						values[i] = OptionalValue{Valid: true, Value: st.sumI}
					} else {
						values[i] = OptionalValue{Valid: true, Value: st.sumF}
					}
				}
			case AggregateAvg:
				st := gs.aggStates[i]
				if st.hasValue && st.count > 0 {
					if st.useIntSum {
						values[i] = OptionalValue{Valid: true, Value: float64(st.sumI) / float64(st.count)}
					} else {
						values[i] = OptionalValue{Valid: true, Value: st.sumF / float64(st.count)}
					}
				}
			case AggregateMin:
				if gs.aggStates[i].hasValue {
					values[i] = gs.aggStates[i].min
				}
			case AggregateMax:
				if gs.aggStates[i].hasValue {
					values[i] = gs.aggStates[i].max
				}
			}
		}
		resultRow := NewRowWithValues(resultColumns, values)

		// Apply HAVING filter against the computed aggregate row.
		if len(stmt.Having) > 0 {
			ok, err := resultRow.CheckOneOrMore(stmt.Having)
			if err != nil {
				return StatementResult{}, fmt.Errorf("HAVING: %w", err)
			}
			if !ok {
				continue
			}
		}

		resultRows = append(resultRows, resultRow)
	}

	// Apply ORDER BY if specified.
	if len(stmt.OrderBy) > 0 {
		if err := t.sortRows(resultRows, stmt.OrderBy); err != nil {
			return StatementResult{}, err
		}
	}

	// Apply OFFSET.
	if stmt.Offset.Valid {
		offset := int(stmt.Offset.Value.(int64))
		if offset >= len(resultRows) {
			resultRows = []Row{}
		} else {
			resultRows = resultRows[offset:]
		}
	}

	// Apply LIMIT.
	if stmt.Limit.Valid {
		limit := int(stmt.Limit.Value.(int64))
		if limit < len(resultRows) {
			resultRows = resultRows[:limit]
		}
	}

	idx := 0
	return StatementResult{
		Columns: resultColumns,
		Rows: NewIterator(func(ctx context.Context) (Row, error) {
			if idx >= len(resultRows) {
				return Row{}, ErrNoMoreRows
			}
			row := resultRows[idx]
			idx += 1
			return row, nil
		}),
	}, nil
}

// errLimitReached is a sentinel returned from the plan.Execute callback to stop
// scanning as soon as the requested LIMIT of projected rows has been reached.
// It is consumed inside selectStreamingDirect and never surfaced to callers.
var errLimitReached = errors.New("select: limit reached")

// selectStreamingDirect executes the query plan and projects each row inline,
// without building an intermediate []Row buffer of all matching rows.
//
// It is only called for non-JOIN queries that do not require a full
// materialisation (no ORDER BY that needs sorting, no GROUP BY, no aggregates).
// For such queries this eliminates one full-table allocation and one extra pass
// over the data compared with the collect-then-project approach.
//
// Early termination: when LIMIT is set the plan scan stops as soon as the
// required number of projected rows has been collected, via errLimitReached.
// This is safe for all non-JOIN scan types because their inner loops propagate
// callback errors immediately.
func (t *Table) selectStreamingDirect(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	selectedFields []Field,
	requestedFields []Field,
) (StatementResult, error) {
	result := StatementResult{
		Columns: make([]Column, len(requestedFields)),
	}
	for i, field := range requestedFields {
		if field.Expr != nil {
			result.Columns[i] = Column{Name: field.OutputName()}
		} else if colIdx := stmt.ColumnIdx(field.Name); colIdx >= 0 {
			result.Columns[i] = t.Columns[colIdx]
		}
	}

	var (
		remaining int64
		offset    int64
		hasLimit  = stmt.Limit.Valid
		hasOffset = stmt.Offset.Valid
	)
	if hasLimit {
		remaining = stmt.Limit.Value.(int64)
	}
	if hasOffset {
		offset = stmt.Offset.Value.(int64)
	}

	var seen map[string]struct{}
	if stmt.Distinct {
		seen = make(map[string]struct{})
	}

	// When LIMIT is present, pre-size to exactly the limit so append never reallocates.
	// Without LIMIT, start empty and grow as needed.
	var projected []Row
	if hasLimit {
		projected = make([]Row, 0, int(remaining))
	} else {
		projected = make([]Row, 0)
	}

	err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		p, projErr := projectRow(row, requestedFields)
		if projErr != nil {
			return projErr
		}
		if stmt.Distinct {
			key := p.rowDistinctKey()
			if _, dup := seen[key]; dup {
				return nil
			}
			seen[key] = struct{}{}
		}
		if hasOffset && offset > 0 {
			offset--
			return nil
		}
		projected = append(projected, p)
		if hasLimit {
			remaining--
			if remaining == 0 {
				return errLimitReached
			}
		}
		return nil
	})

	if err != nil && !errors.Is(err, errLimitReached) {
		return StatementResult{}, err
	}

	result.Rows = NewSliceIterator(projected)
	return result, nil
}

func (t *Table) selectStreaming(stmt Statement, scanned []Row, requestedFields []Field) (StatementResult, error) {
	result := StatementResult{
		Columns: make([]Column, len(requestedFields)),
	}
	for i, field := range requestedFields {
		if field.Expr != nil {
			result.Columns[i] = Column{Name: field.OutputName()}
		} else if colIdx := stmt.ColumnIdx(field.Name); colIdx >= 0 {
			result.Columns[i] = t.Columns[colIdx]
		}
	}

	var (
		limit, offset int64
		hasLimit      = stmt.Limit.Valid
		hasOffset     = stmt.Offset.Valid
	)
	if hasLimit {
		limit = stmt.Limit.Value.(int64)
	}
	if hasOffset {
		offset = stmt.Offset.Value.(int64)
	}

	var seen map[string]struct{}
	if stmt.Distinct {
		seen = make(map[string]struct{})
	}

	projected := make([]Row, 0, len(scanned))
	for _, row := range scanned {
		p, err := projectRow(row, requestedFields)
		if err != nil {
			return StatementResult{}, err
		}
		if stmt.Distinct {
			key := p.rowDistinctKey()
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
		}
		if hasOffset && offset > 0 {
			offset--
			continue
		}
		projected = append(projected, p)
		if hasLimit {
			limit--
			if limit == 0 {
				break
			}
		}
	}

	result.Rows = NewSliceIterator(projected)
	return result, nil
}

func (t *Table) selectWithSort(stmt Statement, plan QueryPlan, allRows []Row, requestedFields []Field) (StatementResult, error) {
	// Check if we can use heap optimization for LIMIT queries
	var limit int
	hasLimit := stmt.Limit.Valid
	if hasLimit {
		limit = int(stmt.Limit.Value.(int64))
	}

	var offset int
	if stmt.Offset.Valid {
		offset = int(stmt.Offset.Value.(int64))
	}

	// For queries with LIMIT (and optional OFFSET), use a heap to keep only top N+offset rows.
	// However, when DISTINCT is enabled we must see all rows before deduplication, so the
	// heap optimisation cannot be used in that case.
	if hasLimit && len(plan.OrderBy) > 0 && !stmt.Distinct {
		maxRows := offset + limit
		h := newRowHeap(plan.OrderBy, maxRows)
		for _, row := range allRows {
			h.PushRow(row)
		}
		allRows = h.ExtractSorted()
	} else {
		if err := t.sortRows(allRows, plan.OrderBy); err != nil {
			return StatementResult{}, err
		}
	}

	// Apply DISTINCT deduplication before OFFSET/LIMIT
	if stmt.Distinct {
		allRows = deduplicateRows(allRows, requestedFields)
	}

	// Apply OFFSET and LIMIT to final result
	if offset >= len(allRows) {
		allRows = []Row{}
	} else {
		end := offset + limit
		if hasLimit && end < len(allRows) {
			allRows = allRows[offset:end]
		} else {
			allRows = allRows[offset:]
		}
	}

	// Create result with materialized rows
	idx := 0
	result := StatementResult{
		Columns: make([]Column, len(requestedFields)),
	}
	for i, field := range requestedFields {
		if field.Expr != nil {
			result.Columns[i] = Column{Name: field.OutputName()}
		} else if colIdx := stmt.ColumnIdx(field.Name); colIdx >= 0 {
			result.Columns[i] = t.Columns[colIdx]
		}
	}

	result.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		if idx >= len(allRows) {
			return Row{}, ErrNoMoreRows
		}
		row := allRows[idx]
		idx += 1

		// Filter to only requested fields (important for JOINs where rows have all columns)
		return projectRow(row, requestedFields)
	})

	return result, nil
}

func (t *Table) indexScanAll(ctx context.Context, aPlan QueryPlan, scan Scan, selectedFields []Field, out func(Row) error) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}
	var (
		selectedMask   = selectedColumnsMask(t.Columns, selectedFields)
		tableFilter    = compileScanFilter(t.Columns, scan.Filters)
		coveringFilter = compileScanFilter(scan.IndexColumns, scan.Filters)
	)

	// Scan index in order (or reverse order)
	if err := idx.ScanAll(ctx, aPlan.SortReverse, func(key any, rowID RowID) error {
		var row Row

		if scan.CoveringIndex {
			// Index-only scan: build row directly from key without touching the table page.
			row = rowFromIndexKey(key, scan.IndexColumns, rowID)
		} else {
			// Find the row by ID
			cursor, err := t.Seek(ctx, rowID)
			if err != nil {
				return fmt.Errorf("find row failed: %w", err)
			}

			if len(selectedFields) == 0 {
				row = NewRowWithValues(t.Columns, nil)
				row.Key = rowID
			} else {
				var err error
				row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
				if err != nil {
					return fmt.Errorf("fetch row failed: %w", err)
				}

				// Apply remaining filters
				if tableFilter != nil {
					ok, err := tableFilter(row)
					if err != nil {
						return err
					}
					if !ok {
						return nil // Skip this row
					}
				}
			}
		}

		// For covering index scans, filters still need to be applied
		// (all filter columns are guaranteed to be in the index).
		if scan.CoveringIndex && coveringFilter != nil {
			ok, err := coveringFilter(row)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		return out(row)
	}); err != nil {
		return err
	}

	return nil
}

func (t *Table) indexRangeScan(ctx context.Context, aPlan QueryPlan, scan Scan, selectedFields []Field, out func(Row) error) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}
	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	coveringFilter := compileScanFilter(scan.IndexColumns, scan.Filters)

	// Scan index within range (forward or reverse)
	if err := idx.ScanRange(ctx, scan.RangeCondition, aPlan.SortReverse, func(key any, rowID RowID) error {
		var row Row

		if scan.CoveringIndex {
			// Index-only scan: build row directly from key without touching the table page.
			row = rowFromIndexKey(key, scan.IndexColumns, rowID)
		} else {
			// Find the row by ID
			cursor, err := t.Seek(ctx, rowID)
			if err != nil {
				return fmt.Errorf("find row failed: %w", err)
			}

			if len(selectedFields) == 0 {
				row = NewRowWithValues(t.Columns, nil)
				row.Key = rowID
			} else {
				var err error
				row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
				if err != nil {
					return fmt.Errorf("fetch row failed: %w", err)
				}

				// Apply remaining filters
				if tableFilter != nil {
					ok, err := tableFilter(row)
					if err != nil {
						return err
					}
					if !ok {
						return nil // Skip this row
					}
				}
			}
		}

		// For covering index scans, filters still need to be applied
		// (all filter columns are guaranteed to be in the index).
		if scan.CoveringIndex && coveringFilter != nil {
			ok, err := coveringFilter(row)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		return out(row)
	}); err != nil {
		return err
	}

	return nil
}

func (t *Table) indexPointScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}
	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	coveringFilter := compileScanFilter(scan.IndexColumns, scan.Filters)

	// Lookup each primary key value
	for _, indexValue := range scan.IndexKeys {
		// Find row ID from primary key index
		rowIDs, err := idx.FindRowIDs(ctx, indexValue)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// Key not found, skip
				continue
			}
			return fmt.Errorf("index lookup failed: %w", err)
		}

		for _, rowID := range rowIDs {
			var row Row

			switch {
			case scan.CoveringIndex:
				row = rowFromIndexKey(indexValue, scan.IndexColumns, rowID)
			case len(selectedFields) == 0:
				row = NewRowWithValues(t.Columns, nil)
				row.Key = rowID
			default:
				// Find the row by ID
				cursor, err := t.Seek(ctx, rowID)
				if err != nil {
					return fmt.Errorf("find row failed: %w", err)
				}

				// Fetch the row
				row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
				if err != nil {
					return fmt.Errorf("fetch row failed: %w", err)
				}

				// Apply remaining filters
				if tableFilter != nil {
					ok, err := tableFilter(row)
					if err != nil {
						return err
					}
					if !ok {
						continue // Skip this row
					}
				}
			}

			// For covering index scans, filters still need to be applied
			// (all filter columns are guaranteed to be in the index).
			if scan.CoveringIndex && coveringFilter != nil {
				ok, err := coveringFilter(row)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
			}

			if err := ctx.Err(); err != nil {
				return err
			}
			if err := out(row); err != nil {
				return err
			}
		}
	}

	return nil
}

// errStopScan is a sentinel returned by an index scan callback to stop iteration after the
// first row has been delivered.  It is never surfaced to callers.
var errStopScan = errors.New("stop scan")

// indexEndpointScan fetches exactly one row from either end of the given index:
// the first (smallest) entry when reverse=false (MIN), or the last (largest) entry
// when reverse=true (MAX).  It uses the index's in-order traversal, stopping as
// soon as the first qualifying row has been sent to out.
func (t *Table) indexEndpointScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error, reverse bool) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for endpoint scan: %s", scan.IndexName)
	}
	selectedMask := selectedColumnsMask(t.Columns, selectedFields)

	err := idx.ScanAll(ctx, reverse, func(key any, rowID RowID) error {
		var row Row

		if scan.CoveringIndex {
			// Index-only scan: build row directly from key without touching the table page.
			row = rowFromIndexKey(key, scan.IndexColumns, rowID)
		} else {
			cursor, err := t.Seek(ctx, rowID)
			if err != nil {
				return fmt.Errorf("find row failed: %w", err)
			}

			if len(selectedFields) == 0 {
				row = NewRowWithValues(t.Columns, nil)
				row.Key = rowID
			} else {
				row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
				if err != nil {
					return fmt.Errorf("fetch row failed: %w", err)
				}
			}
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		if err := out(row); err != nil {
			return err
		}

		// Stop after the first qualifying row.
		return errStopScan
	})

	if err != nil && !errors.Is(err, errStopScan) {
		return err
	}
	return nil
}

// projectRow returns a new row containing only the requested fields.
// For plain column fields it performs a name lookup on the source row.
// For computed expression fields it evaluates the Expr and stores the result
// in a synthetic column named by Field.OutputName().
func projectRow(row Row, fields []Field) (Row, error) {
	// Fast path: no expression fields — delegate to the existing helper.
	hasExpr := false
	for _, f := range fields {
		if f.Expr != nil {
			hasExpr = true
			break
		}
	}
	if !hasExpr {
		return row.OnlyFields(fields...), nil
	}

	columns := make([]Column, 0, len(fields))
	values := make([]OptionalValue, 0, len(fields))

	for _, f := range fields {
		if f.Expr != nil {
			result, err := f.Expr.Eval(row)
			if err != nil {
				return Row{}, fmt.Errorf("evaluating expression %q: %w", f.OutputName(), err)
			}
			columns = append(columns, Column{Name: f.OutputName()})
			if result == nil {
				values = append(values, OptionalValue{Valid: false})
			} else {
				values = append(values, OptionalValue{Value: result, Valid: true})
			}
		} else {
			var lookupName string
			if f.AliasPrefix != "" {
				lookupName = f.AliasPrefix + "." + f.Name
			} else {
				lookupName = f.Name
			}
			col, idx := row.GetColumn(lookupName)
			if idx >= 0 {
				columns = append(columns, col)
				values = append(values, row.Values[idx])
			} else {
				// Column not found — keep zero-value column with the requested name.
				columns = append(columns, Column{Name: f.Name})
				values = append(values, OptionalValue{Valid: false})
			}
		}
	}

	return NewRowWithValues(columns, values), nil
}

// exprSourceFields replaces each computed expression field with the underlying
// column fields it references, so the table scan fetches the right data from disk.
// Plain column fields are passed through unchanged.
func exprSourceFields(fields []Field) []Field {
	result := make([]Field, 0, len(fields))
	for _, f := range fields {
		if f.Expr == nil {
			result = append(result, f)
		} else {
			for _, colName := range f.Expr.Columns() {
				result = append(result, Field{Name: colName})
			}
		}
	}
	return result
}

// rowDistinctKey builds a string key from a projected row's values for DISTINCT deduplication.
// Each value is encoded with a type prefix so that different types with the same printed
// representation (e.g. int64(1) and float64(1)) are never considered equal.
func (r Row) rowDistinctKey() string {
	var b strings.Builder
	for i, v := range r.Values {
		if i > 0 {
			b.WriteByte('\x1f') // ASCII unit separator
		}
		if !v.Valid {
			b.WriteString("null")
			continue
		}
		switch val := v.Value.(type) {
		case TextPointer:
			fmt.Fprintf(&b, "t%d:%s", val.Length, val.String())
		case bool:
			fmt.Fprintf(&b, "b:%t", val)
		case int64:
			fmt.Fprintf(&b, "i64:%d", val)
		case int32:
			fmt.Fprintf(&b, "i32:%d", val)
		case float64:
			fmt.Fprintf(&b, "f64:%v", val)
		case float32:
			fmt.Fprintf(&b, "f32:%v", val)
		default:
			fmt.Fprintf(&b, "?:%v", val)
		}
	}
	return b.String()
}

// deduplicateRows removes duplicate rows based on their projected values for the given fields.
// It preserves the first occurrence of each unique row and maintains input order.
func deduplicateRows(rows []Row, fields []Field) []Row {
	seen := make(map[string]struct{}, len(rows))
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		projected := row.OnlyFields(fields...)
		key := projected.rowDistinctKey()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	return out
}

func (t *Table) sequentialScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error) error {
	if t.virtualRows != nil {
		return t.virtualSequentialScan(ctx, scan, out)
	}
	if t.parallelScan {
		return t.parallelSequentialScan(ctx, scan, selectedFields, out)
	}
	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return err
	}

	fullMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)

	// Two-phase unmarshal: if the WHERE filter references only a strict subset of the
	// selected columns, decode just the filter columns in phase 1 and check the
	// predicate before decoding the remaining (potentially expensive) columns in phase 2.
	// Rows that fail the predicate are discarded after the cheap phase-1 decode, avoiding
	// TextPointer / overflow allocations for every non-matching row.
	filterMask := fullMask
	twoPhase := tableFilter != nil
	if twoPhase {
		filterMask = filterOnlyMask(t.Columns, scan.Filters)
		twoPhase = maskHasTrue(filterMask) && !masksEqual(filterMask, fullMask)
	}

	page, err := t.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return fmt.Errorf("sequential scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return err
		}

		if !twoPhase {
			// ── Single-phase path (original behaviour) ────────────────────────
			row, err := cursor.fetchRowWithMask(ctx, true, fullMask)
			if err != nil {
				return err
			}
			if tableFilter != nil {
				ok, err := tableFilter(row)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
			}
			if err := out(row); err != nil {
				return err
			}
			continue
		}

		// ── Two-phase path ────────────────────────────────────────────────────
		// Re-read page when the cursor crossed a page boundary.
		if page.Index != cursor.PageIdx {
			page, err = t.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return fmt.Errorf("sequential scan: %w", err)
			}
		}

		// Snapshot the current cell before advancing the cursor so phase 2
		// can unmarshal from the same cell without a second ReadPage call.
		cell := page.LeafNode.Cells[cursor.CellIdx]

		// Advance cursor (mirrors fetchRowWithMask advance logic).
		switch {
		case cursor.CellIdx < page.LeafNode.Header.Cells-1:
			cursor.CellIdx += 1
		case page.LeafNode.Header.NextLeaf == 0:
			cursor.EndOfTable = true
		default:
			cursor.PageIdx = page.LeafNode.Header.NextLeaf
			cursor.CellIdx = 0
		}

		// Phase 1: decode only the columns needed to evaluate the predicate.
		filterRow := t.newRow()
		filterRow, err = filterRow.UnmarshalWithMask(cell, filterMask)
		if err != nil {
			return err
		}

		ok, err := tableFilter(filterRow)
		if err != nil {
			return err
		}
		if !ok {
			continue // Non-matching row: skip the expensive phase-2 decode.
		}

		// Phase 2: decode all selected columns (cell data is still valid in cache).
		row := t.newRow()
		row, err = row.UnmarshalWithMask(cell, fullMask)
		if err != nil {
			return err
		}
		row, err = row.readOverflowTexts(ctx, t.pager)
		if err != nil {
			return fmt.Errorf("sequential scan read overflow: %w", err)
		}

		if err := out(row); err != nil {
			return err
		}
	}

	return nil
}

// filterOnlyMask returns a column-selection mask that includes only the columns
// referenced in the given filter conditions (the WHERE predicate columns).
func filterOnlyMask(columns []Column, filters OneOrMore) []bool {
	filterCols := make(map[string]struct{})
	for _, group := range filters {
		for _, cond := range group {
			if cond.Operand1.Type == OperandField {
				filterCols[cond.Operand1.Value.(Field).Name] = struct{}{}
			}
			if cond.Operand2.Type == OperandField {
				filterCols[cond.Operand2.Value.(Field).Name] = struct{}{}
			}
		}
	}
	mask := make([]bool, len(columns))
	for i, col := range columns {
		_, ok := filterCols[col.Name]
		mask[i] = ok
	}
	return mask
}

// masksEqual reports whether two column-selection masks are identical.
func masksEqual(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// maskHasTrue reports whether any entry in the mask is true.
func maskHasTrue(mask []bool) bool {
	for _, b := range mask {
		if b {
			return true
		}
	}
	return false
}

func compileScanFilter(columns []Column, filters OneOrMore) func(Row) (bool, error) {
	if len(filters) == 0 {
		return nil
	}
	columnIndexes := make(map[string]int, len(columns))
	for i := range columns {
		columnIndexes[columns[i].Name] = i
	}
	return func(row Row) (bool, error) {
		return row.CheckOneOrMoreWithColumnIndexes(filters, columnIndexes)
	}
}

// virtualSequentialScan iterates the table's in-memory virtualRows, applies the
// scan filter, and calls out for each matching row.  It is only called when
// t.virtualRows != nil (derived-table virtual tables).
func (t *Table) virtualSequentialScan(ctx context.Context, scan Scan, out func(Row) error) error {
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	for _, row := range t.virtualRows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if tableFilter != nil {
			ok, err := tableFilter(row)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		if err := out(row); err != nil {
			return err
		}
	}
	return nil
}

// collectRowIDsFromScan runs a sub-scan and collects all RowIDs it produces without
// fetching table rows.  Supported sub-scan types: ScanTypeIndexPoint and ScanTypeIndexRange.
func (t *Table) collectRowIDsFromScan(ctx context.Context, scan Scan) ([]RowID, error) {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return nil, fmt.Errorf("no index found for intersect sub-scan: %s", scan.IndexName)
	}
	switch scan.Type {
	case ScanTypeIndexPoint:
		var rowIDs []RowID
		for _, key := range scan.IndexKeys {
			ids, err := idx.FindRowIDs(ctx, key)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return nil, fmt.Errorf("intersect point lookup: %w", err)
			}
			rowIDs = append(rowIDs, ids...)
		}
		return rowIDs, nil
	case ScanTypeIndexRange:
		var rowIDs []RowID
		if err := idx.ScanRange(ctx, scan.RangeCondition, false, func(_ any, rowID RowID) error {
			rowIDs = append(rowIDs, rowID)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("intersect range scan: %w", err)
		}
		return rowIDs, nil
	default:
		return nil, fmt.Errorf("unsupported sub-scan type for intersect: %s", scan.Type)
	}
}

// intersectTwoSortedSets returns the sorted intersection of two already-sorted RowID slices.
// Returns nil when the intersection is empty.
func intersectTwoSortedSets(a, b []RowID) []RowID {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	result := make([]RowID, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			// Skip duplicates within the same set.
			if len(result) == 0 || result[len(result)-1] != a[i] {
				result = append(result, a[i])
			}
			i += 1
			j += 1
		case a[i] < b[j]:
			i += 1
		default:
			j += 1
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// intersectSortedRowIDs sorts each input slice and returns their intersection.
func intersectSortedRowIDs(sets [][]RowID) []RowID {
	if len(sets) == 0 {
		return nil
	}
	for i := range sets {
		sortRowIDs(sets[i])
	}
	result := sets[0]
	for _, next := range sets[1:] {
		result = intersectTwoSortedSets(result, next)
		if len(result) == 0 {
			return nil
		}
	}
	return result
}

// sortRowIDs sorts a RowID slice in-place.
func sortRowIDs(ids []RowID) {
	// Insertion sort for small slices; falls back to std sort for large ones.
	if len(ids) < 16 {
		for i := 1; i < len(ids); i++ {
			v := ids[i]
			j := i
			for j > 0 && ids[j-1] > v {
				ids[j] = ids[j-1]
				j--
			}
			ids[j] = v
		}
		return
	}
	// Standard sort for larger slices.
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}

// indexIntersectScan executes a ScanTypeIndexIntersect plan:
//  1. Collect RowID sets from each sub-scan.
//  2. Intersect all sets in memory.
//  3. Fetch the surviving rows and apply any remaining post-filters.
func (t *Table) indexIntersectScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error) error {
	sets := make([][]RowID, 0, len(scan.SubScans))
	for _, sub := range scan.SubScans {
		ids, err := t.collectRowIDsFromScan(ctx, sub)
		if err != nil {
			return err
		}
		sets = append(sets, ids)
	}

	surviving := intersectSortedRowIDs(sets)
	if len(surviving) == 0 {
		return nil
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)

	for _, rowID := range surviving {
		if err := ctx.Err(); err != nil {
			return err
		}

		var row Row
		if len(selectedFields) == 0 {
			row = NewRowWithValues(t.Columns, nil)
			row.Key = rowID
		} else {
			cursor, err := t.Seek(ctx, rowID)
			if err != nil {
				return fmt.Errorf("intersect seek: %w", err)
			}
			row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
			if err != nil {
				return fmt.Errorf("intersect fetch: %w", err)
			}
		}

		if tableFilter != nil {
			ok, err := tableFilter(row)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}

		if err := out(row); err != nil {
			return err
		}
	}
	return nil
}
