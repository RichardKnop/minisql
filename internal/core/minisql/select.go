package minisql

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNoMoreRows = errors.New("no more rows")
)

func (t *Table) Select(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if err := stmt.Validate(t); err != nil {
		return StatementResult{}, err
	}

	// Create query plan
	plan := t.PlanQuery(ctx, stmt)

	t.logger.Sugar().With("query type", "SELECT").Debugf("Query plan: scan_type=%s, use_index=%v, index_keys=%v",
		plan.ScanType.String(), plan.IsIndexScan(), plan.IndexKeyGroups)

	// Only fetch fields included in the SELECT query or fields needed for WHERE conditions
	// TODO - handle * plus other fiels, for example SELECT *, a, b FROM table WHERE c = 1
	var (
		selectAll       = stmt.IsSelectAll()
		requestedFields []Field
		selectedFields  []Field
	)
	if selectAll {
		requestedFields = fieldsFromColumns(t.Columns...)
		selectedFields = requestedFields
	} else {
		requestedFields = stmt.Fields
		selectedFields = requestedFields
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

	aResult := StatementResult{
		Columns: make([]Column, 0, len(requestedFields)),
		Rows: func(ctx context.Context) (Row, error) {
			return Row{}, ErrNoMoreRows
		},
	}
	for _, aField := range requestedFields {
		if colIdx := stmt.ColumnIdx(aField.Name); colIdx >= 0 {
			aResult.Columns = append(aResult.Columns, t.Columns[colIdx])
		}
	}

	var (
		unfilteredPipe = make(chan Row)
		filteredPipe   = make(chan Row)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
	)

	// Execute based on plan
	if plan.IsIndexScan() {
		// Use primary key index lookup
		go t.indexPointScan(ctx, plan, selectedFields, unfilteredPipe, errorsPipe, stopChan)
	} else {
		// Sequential scan
		go t.sequentialScan(ctx, selectedFields, unfilteredPipe, errorsPipe, stopChan)
	}

	// Filter rows according to the WHERE conditions. In case of an index scan,
	// any remaining filtering will happen here. In case of a sequential scan,
	// this will filter all rows.
	// LIMIT and OFFSET are also applied here.
	go func(in <-chan Row, out chan<- Row) {
		defer close(out)
		defer close(stopChan)
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
			ok, err := plan.FilterRow(aRow)
			if err != nil {
				errorsPipe <- err
				return
			}
			if !ok {
				continue
			}
			if stmt.Offset.Valid && offset > 0 {
				offset -= 1
				continue
			}
			if stmt.Limit.Valid {
				limit -= 1
			}
			sendFetchedRow(aRow, out, selectAll, requestedFields...)
		}
	}(unfilteredPipe, filteredPipe)

	aResult.Rows = func(ctx context.Context) (Row, error) {
		select {
		case <-ctx.Done():
			return Row{}, fmt.Errorf("context done: %w", ctx.Err())
		case err := <-errorsPipe:
			return Row{}, err
		case aRow, open := <-filteredPipe:
			if !open {
				return Row{}, ErrNoMoreRows
			}

			return aRow, nil
		}
	}

	return aResult, nil
}

func sendFetchedRow(aRow Row, out chan<- Row, selectAll bool, requestedFields ...Field) {
	if selectAll {
		out <- aRow
	} else {
		out <- aRow.OnlyFields(requestedFields...)
	}
}

func (t *Table) indexPointScan(ctx context.Context, plan QueryPlan,
	selectedFields []Field, out chan<- Row, errorsPipe chan<- error, stopChan <-chan bool) {

	defer close(out)

	// Lookup each primary key value
	for _, pkGroup := range plan.IndexKeyGroups {
		for _, pkValue := range pkGroup {
			// Find row ID from primary key index
			rowID, err := t.PrimaryKey.Index.Find(ctx, pkValue)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					// Key not found, skip
					continue
				}
				errorsPipe <- fmt.Errorf("index lookup failed: %w", err)
				return
			}

			// Find the row by ID
			aCursor, err := t.Seek(ctx, rowID)
			if err != nil {
				errorsPipe <- fmt.Errorf("find row failed: %w", err)
				return
			}

			// Fetch the row
			rowCursor := *aCursor
			aRow, err := aCursor.fetchRow(ctx, selectedFields...)
			if err != nil {
				errorsPipe <- fmt.Errorf("fetch row failed: %w", err)
				return
			}
			aRow.cursor = rowCursor

			select {
			case <-stopChan:
				return
			case out <- aRow:
				continue
			}
		}
	}
}

func (t *Table) sequentialScan(ctx context.Context, selectedFields []Field,
	out chan<- Row, errorsPipe chan<- error, stopChan <-chan bool) {
	defer close(out)

	aCursor, err := t.SeekFirst(ctx)
	if err != nil {
		errorsPipe <- err
		return
	}

	aPage, err := t.pager.ReadPage(ctx, aCursor.PageIdx)
	if err != nil {
		errorsPipe <- fmt.Errorf("sequential scan: %w", err)
		return
	}
	aCursor.EndOfTable = aPage.LeafNode.Header.Cells == 0

	for !aCursor.EndOfTable {
		rowCursor := *aCursor
		aRow, err := aCursor.fetchRow(ctx, selectedFields...)
		if err != nil {
			errorsPipe <- err
			return
		}
		aRow.cursor = rowCursor

		select {
		case <-stopChan:
			return
		case out <- aRow:
			continue
		}
	}
}
