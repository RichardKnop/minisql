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
	aCursor, err := t.Seek(ctx, uint64(0))
	if err != nil {
		return StatementResult{}, err
	}
	aPage, err := t.pager.GetPage(ctx, t, aCursor.PageIdx)
	if err != nil {
		return StatementResult{}, err
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

	go func(out chan<- Row) {
		defer close(out)
		for aCursor.EndOfTable == false {
			aRow, err := aCursor.fetchRow(ctx)
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
			ok, err := rowMatchesConditions(conditions, aRow)
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
			out <- aRow
		}
	}(filteredPipe, limitedPipe, limit)

	aResult := StatementResult{
		Columns: t.Columns,
		Rows: func(ctx context.Context) (Row, error) {
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
		},
	}

	return aResult, nil
}

func rowMatchesConditions(conditions OneOrMore, aRow Row) (bool, error) {
	if len(conditions) == 0 {
		return true, nil
	}

	for _, aConditionGroup := range conditions {
		groupConditionResult := true
		for _, aCondition := range aConditionGroup {
			ok, err := checkConditionOnRow(aCondition, aRow)
			if err != nil {
				return false, err
			}

			if !ok {
				groupConditionResult = false
				break
			}
		}

		if groupConditionResult {
			return true, nil
		}
	}

	return false, nil
}

func checkConditionOnRow(aCondition Condition, aRow Row) (bool, error) {
	// left side is field, right side is literal value
	if aCondition.Operand1.IsField() && !aCondition.Operand2.IsField() {
		value, ok := aRow.GetValue(fmt.Sprint(aCondition.Operand1))
		if !ok {
			return false, fmt.Errorf("row does not have '%s' column", aCondition.Operand1.Value)
		}
		return value == aCondition.Operand2, nil
	}

	// left side is literal value, right side is field
	if aCondition.Operand2.IsField() && !aCondition.Operand1.IsField() {
		value, ok := aRow.GetValue(fmt.Sprint(aCondition.Operand2))
		if !ok {
			return false, fmt.Errorf("row does not have '%s' column", aCondition.Operand2.Value)
		}
		return value == aCondition.Operand1, nil
	}

	// both left and right are fields, compare 2 row values
	if aCondition.Operand1.IsField() && aCondition.Operand2.IsField() {

	}

	// both left and right are literal values, compare them
	if !aCondition.Operand1.IsField() && !aCondition.Operand2.IsField() {

	}

	return false, nil
}
