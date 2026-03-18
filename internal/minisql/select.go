package minisql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrNoMoreRows = errors.New("no more rows")
)

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
	// TODO - handle * plus other fiels, for example SELECT *, a, b FROM table WHERE c = 1
	var (
		requestedFields []Field
		selectedFields  []Field
	)
	if stmt.IsSelectAll() {
		requestedFields = fieldsFromColumns(t.Columns...)
		selectedFields = requestedFields
	} else if stmt.IsSelectAggregate() {
		// For aggregate queries, fetch the actual source columns (not the synthetic output names).
		colSet := make(map[string]struct{})
		for _, agg := range stmt.Aggregates {
			if agg.Column != "" {
				colSet[agg.Column] = struct{}{}
			}
		}
		for colName := range colSet {
			requestedFields = append(requestedFields, Field{Name: colName})
		}
		selectedFields = requestedFields
	} else {
		if !stmt.IsSelectCountAll() {
			requestedFields = stmt.Fields
			selectedFields = requestedFields
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

	// Aggregate queries (SUM, AVG, MIN, MAX) always materialise all rows.
	if stmt.IsSelectAggregate() {
		return t.selectAggregate(ctx, stmt, filteredPipe, errorsPipe)
	}

	// If we are sorting in memory, it means we are doing sequential scan as we need to
	// gather all rows first to be able to determine correct order.
	if plan.SortInMemory {
		return t.selectWithSort(stmt, plan, filteredPipe, errorsPipe, requestedFields)
	}

	if stmt.IsSelectCountAll() {
		return t.selectCount(ctx, filteredPipe, errorsPipe)
	}

	// Stream results (already ordered or no ordering needed)
	return t.selectStreaming(stmt, filteredPipe, errorsPipe, requestedFields)
}

func (t *Table) selectCount(ctx context.Context, filteredPipe chan Row, errorsPipe chan error) (StatementResult, error) {
	var count int64

	stopChan := make(chan struct{})

	go func() {
		defer close(stopChan)
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
	case <-stopChan:
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
	count    int64
	sumI     int64   // integer accumulator (Int4 / Int8 source columns)
	sumF     float64 // float accumulator (Real / Double source columns)
	useIntSum bool
	min      OptionalValue
	max      OptionalValue
	hasValue bool // true once a non-NULL value has been seen
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
		for aRow := range filteredPipe {
			for i, agg := range stmt.Aggregates {
				switch agg.Kind {
				case AggregateCount:
					states[i].count++

				case AggregateSum, AggregateAvg:
					val, ok := aRow.GetValue(agg.Column)
					if !ok || !val.Valid {
						continue // SQL aggregate functions ignore NULLs
					}
					states[i].count++
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
					val, ok := aRow.GetValue(agg.Column)
					if !ok || !val.Valid {
						continue
					}
					if !states[i].hasValue || compareValues(val, states[i].min) < 0 {
						states[i].min = val
						states[i].hasValue = true
					}

				case AggregateMax:
					val, ok := aRow.GetValue(agg.Column)
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
			if !states[i].hasValue {
				resultColumns[i] = Column{Name: fieldName, Kind: Int8}
				resultValues[i] = OptionalValue{} // NULL — no non-NULL rows
			} else if states[i].useIntSum {
				resultColumns[i] = Column{Name: fieldName, Kind: Int8}
				resultValues[i] = OptionalValue{Valid: true, Value: states[i].sumI}
			} else {
				resultColumns[i] = Column{Name: fieldName, Kind: Double}
				resultValues[i] = OptionalValue{Valid: true, Value: states[i].sumF}
			}

		case AggregateAvg:
			resultColumns[i] = Column{Name: fieldName, Kind: Double}
			if !states[i].hasValue || states[i].count == 0 {
				resultValues[i] = OptionalValue{} // NULL
			} else if states[i].useIntSum {
				resultValues[i] = OptionalValue{Valid: true, Value: float64(states[i].sumI) / float64(states[i].count)}
			} else {
				resultValues[i] = OptionalValue{Valid: true, Value: states[i].sumF / float64(states[i].count)}
			}

		case AggregateMin:
			if col, ok := stmt.ColumnByName(agg.Column); ok {
				resultColumns[i] = Column{Name: fieldName, Kind: col.Kind, Size: col.Size}
			}
			if states[i].hasValue {
				resultValues[i] = states[i].min
			}

		case AggregateMax:
			if col, ok := stmt.ColumnByName(agg.Column); ok {
				resultColumns[i] = Column{Name: fieldName, Kind: col.Kind, Size: col.Size}
			}
			if states[i].hasValue {
				resultValues[i] = states[i].max
			}
		}
	}

	return StatementResult{
		Columns: resultColumns,
		Rows: NewSingleRowIterator(NewRowWithValues(resultColumns, resultValues)),
	}, nil
}

func (t *Table) selectStreaming(stmt Statement, filteredPipe chan Row, errorsPipe chan error, requestedFields []Field) (StatementResult, error) {

	aResult := StatementResult{
		Columns: make([]Column, len(requestedFields)),
	}
	for i, aField := range requestedFields {
		if colIdx := stmt.ColumnIdx(aField.Name); colIdx >= 0 {
			aResult.Columns[i] = t.Columns[colIdx]
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
		for aRow := range in {
			projected := aRow.OnlyFields(requestedFields...)
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

	aResult.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		select {
		case <-ctx.Done():
			return Row{}, fmt.Errorf("context done: %w", ctx.Err())
		case err := <-errorsPipe:
			return Row{}, err
		case aRow, open := <-limitedPipe:
			if !open {
				return Row{}, ErrNoMoreRows
			}

			return aRow, nil
		}
	})

	return aResult, nil
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

		for aRow := range unfilteredPipe {
			h.PushRow(aRow)
		}

		allRows = h.ExtractSorted()
	} else {
		// No LIMIT or no ORDER BY - collect all rows
		// Pre-allocate with a reasonable initial capacity
		allRows = make([]Row, 0, 1024)
		for aRow := range unfilteredPipe {
			allRows = append(allRows, aRow)
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
	aResult := StatementResult{
		Columns: make([]Column, len(requestedFields)),
	}
	for i, field := range requestedFields {
		if colIdx := stmt.ColumnIdx(field.Name); colIdx >= 0 {
			aResult.Columns[i] = t.Columns[colIdx]
		}
	}

	aResult.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		if idx >= len(allRows) {
			return Row{}, ErrNoMoreRows
		}
		row := allRows[idx]
		idx++

		// Filter to only requested fields (important for JOINs where rows have all columns)
		return row.OnlyFields(requestedFields...), nil
	})

	return aResult, nil
}

func (t *Table) indexScanAll(ctx context.Context, aPlan QueryPlan, aScan Scan, selectedFields []Field, out chan<- Row) error {
	anIndex, ok := t.IndexByName(aScan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", aScan.IndexName)
	}

	// Scan index in order (or reverse order)
	if err := anIndex.ScanAll(ctx, aPlan.SortReverse, func(key any, rowID RowID) error {
		// Find the row by ID
		cursor, err := t.Seek(ctx, rowID)
		if err != nil {
			return fmt.Errorf("find row failed: %w", err)
		}

		var aRow Row

		if len(selectedFields) == 0 {
			aRow = NewRowWithValues(t.Columns, nil)
			aRow.Key = rowID
		} else {
			// Fetch the row
			aRow, err = cursor.fetchRow(ctx, false, selectedFields...)
			if err != nil {
				return fmt.Errorf("fetch row failed: %w", err)
			}

			// Apply remaining filters
			ok, err := aScan.FilterRow(aRow)
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
		case out <- aRow:
			return nil
		}
	}); err != nil {
		return err
	}

	return nil
}

func (t *Table) indexRangeScan(ctx context.Context, aPlan QueryPlan, aScan Scan, selectedFields []Field, out chan<- Row) error {
	anIndex, ok := t.IndexByName(aScan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", aScan.IndexName)
	}

	// Scan index within range (forward or reverse)
	if err := anIndex.ScanRange(ctx, aScan.RangeCondition, aPlan.SortReverse, func(key any, rowID RowID) error {
		// Find the row by ID
		cursor, err := t.Seek(ctx, rowID)
		if err != nil {
			return fmt.Errorf("find row failed: %w", err)
		}

		var aRow Row

		if len(selectedFields) == 0 {
			aRow = NewRowWithValues(t.Columns, nil)
			aRow.Key = rowID
		} else {
			// Fetch the row
			aRow, err = cursor.fetchRow(ctx, false, selectedFields...)
			if err != nil {
				return fmt.Errorf("fetch row failed: %w", err)
			}

			// Apply remaining filters
			ok, err := aScan.FilterRow(aRow)
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
		case out <- aRow:
			return nil
		}
	}); err != nil {
		return err
	}

	return nil
}

func (t *Table) indexPointScan(ctx context.Context, aScan Scan, selectedFields []Field, out chan<- Row) error {
	anIndex, ok := t.IndexByName(aScan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", aScan.IndexName)
	}

	// Lookup each primary key value
	for _, indexValue := range aScan.IndexKeys {
		// Find row ID from primary key index
		rowIDs, err := anIndex.FindRowIDs(ctx, indexValue)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// Key not found, skip
				continue
			}
			return fmt.Errorf("index lookup failed: %w", err)
		}

		for _, rowID := range rowIDs {
			var aRow Row

			if len(selectedFields) == 0 {
				aRow = NewRowWithValues(t.Columns, nil)
				aRow.Key = rowID
			} else {
				// Find the row by ID
				aCursor, err := t.Seek(ctx, rowID)
				if err != nil {
					return fmt.Errorf("find row failed: %w", err)
				}

				// Fetch the row
				aRow, err = aCursor.fetchRow(ctx, false, selectedFields...)
				if err != nil {
					return fmt.Errorf("fetch row failed: %w", err)
				}

				// Apply remaining filters
				ok, err := aScan.FilterRow(aRow)
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
			case out <- aRow:
				continue
			}
		}
	}

	return nil
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

func (t *Table) sequentialScan(ctx context.Context, aScan Scan, selectedFields []Field, out chan<- Row) error {
	aCursor, err := t.SeekFirst(ctx)
	if err != nil {
		return err
	}

	aPage, err := t.pager.ReadPage(ctx, aCursor.PageIdx)
	if err != nil {
		return fmt.Errorf("sequential scan: %w", err)
	}
	aCursor.EndOfTable = aPage.LeafNode.Header.Cells == 0

	for !aCursor.EndOfTable {
		aRow, err := aCursor.fetchRow(ctx, true, selectedFields...)
		if err != nil {
			return err
		}

		// Apply remaining filters
		ok, err := aScan.FilterRow(aRow)
		if err != nil {
			return err
		}
		if !ok {
			continue // Skip this row
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- aRow:
			continue
		}
	}

	return nil
}
