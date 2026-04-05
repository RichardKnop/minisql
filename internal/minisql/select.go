package minisql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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

	// Create query plan
	plan, err := t.PlanQuery(ctx, stmt)
	if err != nil {
		return StatementResult{}, err
	}

	t.logger.Sugar().With("query type", "SELECT", "plan", plan).Debug("query plan")

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

	var (
		// Buffered channel reduces goroutine blocking/context switching
		// Buffer size of 128 is a good balance between memory and performance
		filteredPipe = make(chan Row, 128)
		errorsPipe   = make(chan error, len(plan.Scans))
		wg           = new(sync.WaitGroup)
	)

	// Execute scans based on plan
	wg.Go(func() {
		if err := plan.Execute(ctx, t.provider, selectedFields, filteredPipe); err != nil {
			errorsPipe <- err
		}
	})
	go func() {
		wg.Wait()
		close(filteredPipe)
	}()

	if stmt.IsSelectCountAll() {
		return t.selectCount(ctx, filteredPipe, errorsPipe)
	}

	// Grouped aggregate queries (GROUP BY) materialise all rows and group them.
	if stmt.IsSelectGroupBy() {
		return t.selectGroupBy(ctx, stmt, filteredPipe, errorsPipe)
	}

	// Aggregate queries (SUM, AVG, MIN, MAX) always materialise all rows.
	if stmt.IsSelectAggregate() {
		return t.selectAggregate(ctx, stmt, filteredPipe, errorsPipe)
	}

	// If we are sorting in memory, it means we are doing sequential scan as we need to
	// gather all rows first to be able to determine correct order.
	if plan.SortInMemory {
		return t.selectWithSort(stmt, plan, filteredPipe, errorsPipe, requestedFields)
	}

	// Stream results (already ordered or no ordering needed)
	return t.selectStreaming(stmt, filteredPipe, errorsPipe, requestedFields)
}

func (t *Table) selectCount(ctx context.Context, filteredPipe chan Row, errorsPipe chan error) (StatementResult, error) {
	var count int64

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-ctx.Done():
				return
			case _, open := <-filteredPipe:
				if !open {
					return
				}
				count += 1
			}
		}
	}()

	select {
	case <-ctx.Done():
		return StatementResult{}, fmt.Errorf("context done: %w", ctx.Err())
	case err := <-errorsPipe:
		return StatementResult{}, err
	case <-drainDone:
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
}

// aggState holds the running accumulator for a single aggregate expression.
type aggState struct {
	count     int64
	sumI      int64   // integer accumulator (Int4 / Int8 source columns)
	sumF      float64 // float accumulator (Real / Double source columns)
	useIntSum bool
	min       OptionalValue
	max       OptionalValue
	hasValue  bool // true once a non-NULL value has been seen
}

func (t *Table) selectAggregate(ctx context.Context, stmt Statement, filteredPipe chan Row, errorsPipe chan error) (StatementResult, error) {
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

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for row := range filteredPipe {
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
	}()

	select {
	case <-ctx.Done():
		return StatementResult{}, fmt.Errorf("context done: %w", ctx.Err())
	case err := <-errorsPipe:
		return StatementResult{}, err
	case <-drainDone:
	}

	// Build result columns and a single result row.
	resultColumns := make([]Column, len(stmt.Aggregates))
	resultValues := make([]OptionalValue, len(stmt.Aggregates))

	for i, agg := range stmt.Aggregates {
		fieldName := stmt.Fields[i].Name // e.g. "SUM(price)"
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

func (t *Table) selectGroupBy(ctx context.Context, stmt Statement, filteredPipe chan Row, errorsPipe chan error) (StatementResult, error) {
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

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for row := range filteredPipe {
			// Compute group key from the GROUP BY columns.
			groupRow := row.OnlyFields(stmt.GroupBy...)
			key := rowDistinctKey(groupRow)

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
	}()

	select {
	case <-ctx.Done():
		return StatementResult{}, fmt.Errorf("context done: %w", ctx.Err())
	case err := <-errorsPipe:
		return StatementResult{}, err
	case <-drainDone:
	}

	// Build result column metadata.
	resultColumns := make([]Column, len(stmt.Fields))
	for i, field := range stmt.Fields {
		agg := stmt.Aggregates[i]
		switch agg.Kind {
		case 0:
			// GROUP BY column — use the actual table column type.
			if col, ok := t.ColumnByName(field.Name); ok {
				resultColumns[i] = col
			}
		case AggregateCount:
			resultColumns[i] = Column{Name: field.Name, Kind: Int8}
		case AggregateSum:
			if useIntSum[i] {
				resultColumns[i] = Column{Name: field.Name, Kind: Int8}
			} else {
				resultColumns[i] = Column{Name: field.Name, Kind: Double}
			}
		case AggregateAvg:
			resultColumns[i] = Column{Name: field.Name, Kind: Double}
		case AggregateMin, AggregateMax:
			if col, ok := t.ColumnByName(agg.Column); ok {
				resultColumns[i] = Column{Name: field.Name, Kind: col.Kind, Size: col.Size}
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
			idx++
			return row, nil
		}),
	}, nil
}

func (t *Table) selectStreaming(stmt Statement, filteredPipe chan Row, errorsPipe chan error, requestedFields []Field) (StatementResult, error) {
	result := StatementResult{
		Columns: make([]Column, len(requestedFields)),
	}
	for i, field := range requestedFields {
		if field.Expr != nil {
			// Computed column: synthesise metadata from the output name.
			result.Columns[i] = Column{Name: field.OutputName()}
		} else if colIdx := stmt.ColumnIdx(field.Name); colIdx >= 0 {
			result.Columns[i] = t.Columns[colIdx]
		}
	}

	// Filter rows according to the WHERE conditions. In case of an index scan,
	// any remaining filtering will happen here. In case of a sequential scan,
	// this will filter all rows.
	// LIMIT and OFFSET are also applied here.
	// Buffered channel reduces blocking when consumer is slower
	limitedPipe := make(chan Row, 64)
	go func(in <-chan Row, out chan<- Row) {
		defer close(out)
		var limit, offset int64
		if stmt.Limit.Valid {
			limit = stmt.Limit.Value.(int64)
		}
		if stmt.Offset.Valid {
			offset = stmt.Offset.Value.(int64)
		}
		var seen map[string]struct{}
		if stmt.Distinct {
			seen = make(map[string]struct{})
		}
		for row := range in {
			projected, err := projectRow(row, requestedFields)
			if err != nil {
				// Send error and stop — the error channel is checked by the caller.
				select {
				case out <- Row{}:
				default:
				}
				return
			}
			if stmt.Distinct {
				key := rowDistinctKey(projected)
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
			}
			if stmt.Limit.Valid && limit == 0 {
				return
			}
			if stmt.Offset.Valid && offset > 0 {
				offset -= 1
				continue
			}
			if stmt.Limit.Valid {
				limit -= 1
			}
			out <- projected
		}
	}(filteredPipe, limitedPipe)

	result.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		select {
		case <-ctx.Done():
			return Row{}, fmt.Errorf("context done: %w", ctx.Err())
		case err := <-errorsPipe:
			return Row{}, err
		case row, open := <-limitedPipe:
			if !open {
				return Row{}, ErrNoMoreRows
			}

			return row, nil
		}
	})

	return result, nil
}

func (t *Table) selectWithSort(stmt Statement, plan QueryPlan, unfilteredPipe <-chan Row, errorsPipe chan error, requestedFields []Field) (StatementResult, error) {
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

	// For queries with LIMIT (and optional OFFSET), use a heap to keep only top N+offset rows
	// This is much more memory efficient than collecting all rows.
	// However, when DISTINCT is enabled we must see all rows before deduplication, so the
	// heap optimisation cannot be used in that case.
	var allRows []Row
	if hasLimit && len(plan.OrderBy) > 0 && !stmt.Distinct {
		// Use heap-based approach for memory efficiency
		maxRows := offset + limit
		h := newRowHeap(plan.OrderBy, maxRows)

		for row := range unfilteredPipe {
			h.PushRow(row)
		}

		allRows = h.ExtractSorted()
	} else {
		// No LIMIT or no ORDER BY - collect all rows
		// Pre-allocate with a reasonable initial capacity
		allRows = make([]Row, 0, 1024)
		for row := range unfilteredPipe {
			allRows = append(allRows, row)
		}

		// Sort in memory
		if err := t.sortRows(allRows, plan.OrderBy); err != nil {
			return StatementResult{}, err
		}
	}

	// Check for errors
	select {
	case err := <-errorsPipe:
		if err != nil {
			return StatementResult{}, err
		}
	default:
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

func (t *Table) indexScanAll(ctx context.Context, aPlan QueryPlan, scan Scan, selectedFields []Field, out chan<- Row) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}

	// Scan index in order (or reverse order)
	if err := idx.ScanAll(ctx, aPlan.SortReverse, func(key any, rowID RowID) error {
		// Find the row by ID
		cursor, err := t.Seek(ctx, rowID)
		if err != nil {
			return fmt.Errorf("find row failed: %w", err)
		}

		var row Row

		if len(selectedFields) == 0 {
			row = NewRowWithValues(t.Columns, nil)
			row.Key = rowID
		} else {
			// Fetch the row
			row, err = cursor.fetchRow(ctx, false, selectedFields...)
			if err != nil {
				return fmt.Errorf("fetch row failed: %w", err)
			}

			// Apply remaining filters
			ok, err := scan.FilterRow(row)
			if err != nil {
				return err
			}
			if !ok {
				return nil // Skip this row
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- row:
			return nil
		}
	}); err != nil {
		return err
	}

	return nil
}

func (t *Table) indexRangeScan(ctx context.Context, aPlan QueryPlan, scan Scan, selectedFields []Field, out chan<- Row) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}

	// Scan index within range (forward or reverse)
	if err := idx.ScanRange(ctx, scan.RangeCondition, aPlan.SortReverse, func(key any, rowID RowID) error {
		// Find the row by ID
		cursor, err := t.Seek(ctx, rowID)
		if err != nil {
			return fmt.Errorf("find row failed: %w", err)
		}

		var row Row

		if len(selectedFields) == 0 {
			row = NewRowWithValues(t.Columns, nil)
			row.Key = rowID
		} else {
			// Fetch the row
			row, err = cursor.fetchRow(ctx, false, selectedFields...)
			if err != nil {
				return fmt.Errorf("fetch row failed: %w", err)
			}

			// Apply remaining filters
			ok, err := scan.FilterRow(row)
			if err != nil {
				return err
			}
			if !ok {
				return nil // Skip this row
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- row:
			return nil
		}
	}); err != nil {
		return err
	}

	return nil
}

func (t *Table) indexPointScan(ctx context.Context, scan Scan, selectedFields []Field, out chan<- Row) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}

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

			if len(selectedFields) == 0 {
				row = NewRowWithValues(t.Columns, nil)
				row.Key = rowID
			} else {
				// Find the row by ID
				cursor, err := t.Seek(ctx, rowID)
				if err != nil {
					return fmt.Errorf("find row failed: %w", err)
				}

				// Fetch the row
				row, err = cursor.fetchRow(ctx, false, selectedFields...)
				if err != nil {
					return fmt.Errorf("fetch row failed: %w", err)
				}

				// Apply remaining filters
				ok, err := scan.FilterRow(row)
				if err != nil {
					return err
				}
				if !ok {
					continue // Skip this row
				}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- row:
				continue
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
func (t *Table) indexEndpointScan(ctx context.Context, scan Scan, selectedFields []Field, out chan<- Row, reverse bool) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for endpoint scan: %s", scan.IndexName)
	}

	err := idx.ScanAll(ctx, reverse, func(key any, rowID RowID) error {
		cursor, err := t.Seek(ctx, rowID)
		if err != nil {
			return fmt.Errorf("find row failed: %w", err)
		}

		var row Row
		if len(selectedFields) == 0 {
			row = NewRowWithValues(t.Columns, nil)
			row.Key = rowID
		} else {
			row, err = cursor.fetchRow(ctx, false, selectedFields...)
			if err != nil {
				return fmt.Errorf("fetch row failed: %w", err)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- row:
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
func rowDistinctKey(row Row) string {
	var b strings.Builder
	for i, v := range row.Values {
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
		case Time:
			fmt.Fprintf(&b, "ts:%d", val.Microseconds)
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
		key := rowDistinctKey(projected)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	return out
}

func (t *Table) sequentialScan(ctx context.Context, scan Scan, selectedFields []Field, out chan<- Row) error {
	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return err
	}

	page, err := t.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return fmt.Errorf("sequential scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	for !cursor.EndOfTable {
		row, err := cursor.fetchRow(ctx, true, selectedFields...)
		if err != nil {
			return err
		}

		// Apply remaining filters
		ok, err := scan.FilterRow(row)
		if err != nil {
			return err
		}
		if !ok {
			continue // Skip this row
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- row:
			continue
		}
	}

	return nil
}
