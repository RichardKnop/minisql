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

	aCursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}
	aPage, err := t.pager.ReadPage(ctx, aCursor.PageIdx)
	if err != nil {
		return StatementResult{}, fmt.Errorf("select: %w", err)
	}
	aCursor.EndOfTable = aPage.LeafNode.Header.Cells == 0

	t.logger.Sugar().With(
		"page_index", int(aCursor.PageIdx),
		"cell_index", int(aCursor.CellIdx),
	).Debug("fetching rows from")

	var (
		unfilteredPipe = make(chan Row)
		filteredPipe   = make(chan Row)
		limitedPipe    = make(chan Row)
		errorsPipe     = make(chan error, 1)
		stopChan       = make(chan bool)
	)

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

	go func(out chan<- Row) {
		defer close(out)
		for !aCursor.EndOfTable {
			aRow, err := aCursor.fetchRow(ctx, selectedFields...)
			if err != nil {
				errorsPipe <- err
				return
			}

			select {
			case <-stopChan:
				return
			case out <- aRow:
				continue
			}
		}
	}(unfilteredPipe)

	// Filter rows according the WHERE conditions
	go func(in <-chan Row, out chan<- Row, conditions OneOrMore) {
		defer close(out)
		for aRow := range in {
			if len(conditions) == 0 {
				out <- aRow
				continue
			}
			ok, err := aRow.CheckOneOrMore(conditions)
			if err != nil {
				errorsPipe <- err
				return
			}
			if ok {
				out <- aRow
			}
		}
	}(unfilteredPipe, filteredPipe, stmt.Conditions)

	// Count row count for LIMIT clause.
	var limit int64 // TODO - set limit from parser
	go func(in <-chan Row, out chan<- Row, limit int64) {
		defer close(out)
		defer close(stopChan)
		i := int64(0)
		for aRow := range in {
			i += 1
			if i > limit && limit > 0 {
				return
			}
			if selectAll {
				out <- aRow
			} else {
				out <- aRow.OnlyFields(requestedFields...)
			}
		}
	}(filteredPipe, limitedPipe, limit)

	aResult.Rows = func(ctx context.Context) (Row, error) {
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
	}

	return aResult, nil
}
