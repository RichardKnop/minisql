package minisql

import (
	"context"
	"errors"
	"fmt"
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
					selectedFields = append(selectedFields, Field{Name: cond.Operand1.Value.(string)})
				}
				if cond.Operand2.Type == OperandField {
					selectedFields = append(selectedFields, Field{Name: cond.Operand2.Value.(string)})
				}
			}
		}
	}

	var (
		filteredPipe = make(chan Row)
		errorsPipe   = make(chan error, len(plan.Scans))
		wg           = new(sync.WaitGroup)
	)

	// Execute scans based on plan
	wg.Go(func() {
		if err := plan.Execute(ctx, t, selectedFields, filteredPipe); err != nil {
			errorsPipe <- err
		}
	})
	go func() {
		wg.Wait()
		close(filteredPipe)
	}()

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
	limitedPipe := make(chan Row)
	go func(in <-chan Row, out chan<- Row) {
		defer close(out)
		var limit, offset int64
		if stmt.Limit.Valid {
			limit = stmt.Limit.Value.(int64)
		}
		if stmt.Offset.Valid {
			offset = stmt.Offset.Value.(int64)
		}
		for aRow := range in {
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
			out <- aRow.OnlyFields(requestedFields...)
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

	// Collect all rows
	var allRows []Row
	for aRow := range unfilteredPipe {
		allRows = append(allRows, aRow)
	}

	// Check for errors
	select {
	case err := <-errorsPipe:
		if err != nil {
			return StatementResult{}, err
		}
	default:
	}

	// Sort in memory
	if err := t.sortRows(allRows, plan.OrderBy); err != nil {
		return StatementResult{}, err
	}

	// Apply LIMIT and OFFSET
	offset := 0
	limit := len(allRows)
	if stmt.Offset.Valid {
		offset = int(stmt.Offset.Value.(int64))
	}
	if stmt.Limit.Valid {
		limit = int(stmt.Limit.Value.(int64))
	}

	if offset >= len(allRows) {
		allRows = []Row{}
	} else {
		end := offset + limit
		if end > len(allRows) {
			end = len(allRows)
		}
		allRows = allRows[offset:end]
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

		return row, nil
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

func (t *Table) indexRangeScan(ctx context.Context, aScan Scan, selectedFields []Field, out chan<- Row) error {
	anIndex, ok := t.IndexByName(aScan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", aScan.IndexName)
	}

	// Scan index within range
	if err := anIndex.ScanRange(ctx, aScan.RangeCondition, func(key any, rowID RowID) error {
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
