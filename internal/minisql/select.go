package minisql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"go.uber.org/zap"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

// ErrNoMoreRows is returned by an Iterator's row function when the result set is exhausted.
var ErrNoMoreRows = errors.New("no more rows")

// Select executes a SELECT statement against the table. It chooses the optimal
// scan strategy (sequential, index point, index range, covering index, etc.) from
// the query plan, applies WHERE filters, handles GROUP BY / HAVING, ORDER BY,
// LIMIT / OFFSET, aggregates, DISTINCT, JOINs, and UNION / UNION ALL clauses.
func (t *Table) Select(ctx context.Context, stmt Statement) (StatementResult, error) {
	stmt.TableName = t.Name
	stmt.Columns = t.Columns

	if stmt.Kind != Select {
		return StatementResult{}, fmt.Errorf("invalid statement kind for SELECT: %v", stmt.Kind)
	}

	// Fast path: COUNT(*) with no WHERE clause and no JOIN.
	// B-tree tables walk leaf page headers without deserialising row data.
	// Virtual tables (CTEs, derived tables) already have rows in memory — return
	// len(virtualRows) directly without a second scan pass.
	if stmt.IsSelectCountAll() && len(stmt.Conditions) == 0 && len(stmt.Joins) == 0 {
		if t.virtualRows != nil {
			return countResult(int64(len(t.virtualRows))), nil
		}
		return t.countAllLeafWalk(ctx)
	}

	// Window-function queries: materialise all rows first, then apply window logic.
	if stmt.HasWindowFuncs() {
		return t.selectWithWindowFuncs(ctx, stmt)
	}

	// Create query plan
	plan, err := t.PlanQuery(ctx, stmt)
	if err != nil {
		return StatementResult{}, err
	}

	if ce := t.logger.Check(zap.DebugLevel, "query plan"); ce != nil {
		ce.Write(zap.String("query type", "SELECT"), zap.Any("plan", plan))
	}

	// For JOIN queries with GROUP BY or aggregates, replace stmt.Columns with the
	// combined alias-prefixed column list so that groupByAccumulator and newAggStates
	// can resolve column indices against the combined join schema (e.g. "o.user_id",
	// "u.name") rather than the base table schema.
	if len(plan.Joins) > 0 && (stmt.IsSelectGroupBy() || stmt.IsSelectAggregate()) {
		if combined, ok := combinedJoinSchema(ctx, plan, t.provider); ok {
			stmt.Columns = combined
		}
	}

	if stmt.IsSelectCountAll() && len(stmt.Joins) == 0 && t.virtualRows == nil {
		result, ok, err := t.tryCountFromExactInvertedIndex(ctx, plan)
		if err != nil {
			return StatementResult{}, err
		}
		if ok {
			return result, nil
		}
		result, ok, err = t.tryCountFromFullTextIndex(ctx, plan)
		if err != nil {
			return StatementResult{}, err
		}
		if ok {
			return result, nil
		}
	}

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
		}
		// For simple streaming queries (no GROUP BY, no aggregates, no in-memory sort,
		// no JOINs, no window functions), selectedFields is computed lazily just before
		// selectStreamingDirect — the fast paths (covering-index, row-view) use only
		// requestedFields and don't need it at all.
		// For slow-path queries and COUNT(*) with WHERE filters, compute eagerly now.
		isSimpleStreaming := !stmt.IsSelectCountAll() && !stmt.IsSelectGroupBy() &&
			!stmt.IsSelectAggregate() && !plan.SortInMemory && len(plan.Joins) == 0 &&
			!stmt.HasWindowFuncs()
		if !isSimpleStreaming {
			if !stmt.IsSelectCountAll() {
				if stmt.cachedSelectedFields != nil {
					selectedFields = stmt.cachedSelectedFields
				} else {
					selectedFields = exprSourceFields(requestedFields)
				}
			}
			// Always append condition operand fields — needed for COUNT(*) sequential
			// scan filters to read the right column data.
			conditionFieldsEstimate := 0
			for _, conditions := range stmt.Conditions {
				conditionFieldsEstimate += len(conditions) * 2
			}
			if cap(selectedFields) == 0 && conditionFieldsEstimate > 0 {
				selectedFields = make([]Field, 0, conditionFieldsEstimate)
			}
			for _, conditions := range stmt.Conditions {
				for _, cond := range conditions {
					selectedFields = appendOperandSourceFields(selectedFields, cond.Operand1)
					selectedFields = appendOperandSourceFields(selectedFields, cond.Operand2)
				}
			}
		}
	}

	// For COUNT(*) queries where every WHERE condition was consumed by an index
	// scan (no post-scan filters remain), the scan callback never needs to read
	// column data from the main table — only the row key matters for counting.
	// Clear selectedFields so the scan skips the B-tree row fetch entirely.
	if stmt.IsSelectCountAll() && len(selectedFields) > 0 {
		allConsumed := true
		for _, scan := range plan.Scans {
			if len(scan.Filters) > 0 {
				allConsumed = false
				break
			}
		}
		if allConsumed {
			selectedFields = nil
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
		if result, ok, err := t.selectStreamingDirectCoveringIndex(ctx, stmt, plan, requestedFields); ok || err != nil {
			return result, err
		}
		if result, ok, err := t.selectStreamingDirectRowView(ctx, stmt, plan, requestedFields); ok || err != nil {
			return result, err
		}
		// Neither fast path handled the query; compute selectedFields now for the
		// selectStreamingDirect fallback (sequential scans, non-row-view index paths).
		if stmt.cachedSelectedFields != nil {
			selectedFields = stmt.cachedSelectedFields
		} else {
			selectedFields = exprSourceFields(requestedFields)
			conditionFieldsEstimate := 0
			for _, conditions := range stmt.Conditions {
				conditionFieldsEstimate += len(conditions) * 2
			}
			if cap(selectedFields) == 0 && conditionFieldsEstimate > 0 {
				selectedFields = make([]Field, 0, conditionFieldsEstimate)
			}
			for _, conditions := range stmt.Conditions {
				for _, cond := range conditions {
					selectedFields = appendOperandSourceFields(selectedFields, cond.Operand1)
					selectedFields = appendOperandSourceFields(selectedFields, cond.Operand2)
				}
			}
		}
		return t.selectStreamingDirect(ctx, stmt, plan, selectedFields, requestedFields)
	}

	// COUNT(*) with post-scan filters: count matching rows without collecting
	// them into a []Row. For sequential scans, also reuse a single values buffer
	// across all rows to eliminate the dominant per-row heap allocation.
	if stmt.IsSelectCountAll() && len(plan.Joins) == 0 {
		if result, ok, err := t.tryCountFromIndexScan(ctx, plan); err != nil || ok {
			return result, err
		}
		if len(plan.Scans) == 1 && plan.Scans[0].Type == ScanTypeSequential {
			return t.countSequentialScanZeroAlloc(ctx, plan.Scans[0], selectedFields)
		}
		var count int64
		err = plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
			count += 1
			return nil
		})
		if err != nil {
			return StatementResult{}, err
		}
		return countResult(count), nil
	}

	// GROUP BY + single sequential scan (no joins): stream rows through a reuse buffer
	// to avoid one make([]OptionalValue) per row. Falls back to the general path for
	// virtual tables and parallel scans (handled inside selectGroupByZeroAlloc).
	if stmt.IsSelectGroupBy() && len(plan.Joins) == 0 && len(plan.Scans) == 1 && plan.Scans[0].Type == ScanTypeSequential {
		return t.selectGroupByZeroAlloc(ctx, stmt, plan.Scans[0], selectedFields)
	}
	if stmt.IsSelectGroupBy() {
		return t.selectGroupByStreaming(ctx, stmt, plan, selectedFields)
	}

	if result, ok, err := t.trySelectMinMaxFromIndexEndpoint(ctx, stmt, plan); err != nil || ok {
		return result, err
	}

	if stmt.IsSelectAggregate() {
		if len(plan.Joins) == 0 && len(plan.Scans) == 1 && plan.Scans[0].Type == ScanTypeSequential && t.virtualRows == nil && !t.parallelScan {
			return t.selectAggregateSequentialRowView(ctx, stmt, plan.Scans[0], selectedFields)
		}
		return t.selectAggregateStreaming(ctx, stmt, plan, selectedFields)
	}

	if result, ok, err := t.selectWithSortStreamingLimitRowView(ctx, stmt, plan, selectedFields, requestedFields); ok || err != nil {
		return result, err
	}
	if result, ok, err := t.selectWithSortStreamingLimit(ctx, stmt, plan, selectedFields, requestedFields); ok || err != nil {
		return result, err
	}
	if result, ok, err := t.selectWithSortRowView(ctx, stmt, plan, selectedFields, requestedFields); ok || err != nil {
		return result, err
	}

	// Fast path for hash semi-join with a sequential outer scan and RowView-compatible
	// conditions. Scans the outer table without materialising non-matching rows.
	if result, ok, err := t.selectSemiJoinDirectRowView(ctx, stmt, plan, requestedFields); ok || err != nil {
		return result, err
	}

	// Fast path for single INNER/LEFT INLJ with a unique inner index, a sequential
	// outer scan, and no extra join conditions. Streams CombinedRowViews by copying
	// inner cell bytes into a reusable buffer, eliminating per-row []OptionalValue.
	if result, ok, err := t.selectINLJDirectRowView(ctx, stmt, plan, requestedFields); ok || err != nil {
		return result, err
	}

	// Fast path for single INNER/LEFT hash join with compact-cell inner build and a
	// sequential outer scan. Delivers combined RowViews without goroutine, channel,
	// or []OptionalValue allocation per combined row.
	if result, ok, err := t.selectHashJoinDirectRowView(ctx, stmt, plan, requestedFields); ok || err != nil {
		return result, err
	}

	// Streaming join path: for INNER/LEFT/RIGHT/FULL OUTER joins with no
	// ORDER BY sort, GROUP BY, aggregate, or DISTINCT, deliver rows from the
	// join goroutine directly to the Iterator without buffering into []Row.
	// This eliminates the rows = append(rows, row) slice growth.
	if result, ok, err := t.selectStreamingJoin(ctx, stmt, plan, selectedFields, requestedFields); ok || err != nil {
		return result, err
	}

	// JOIN + ORDER BY + LIMIT: use a bounded min-heap (size = limit+offset) so
	// memory is O(limit) rather than O(join cardinality).
	if result, ok, err := t.selectJoinWithSortStreamingLimit(ctx, stmt, plan, selectedFields, requestedFields); ok || err != nil {
		return result, err
	}

	// COUNT(*) + JOIN: count matching rows without materialising them.
	if stmt.IsSelectCountAll() && len(plan.Joins) > 0 {
		var count int64
		if err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
			count += 1
			return nil
		}); err != nil {
			return StatementResult{}, err
		}
		return countResult(count), nil
	}

	// Universal ORDER BY spill: when sortMemLimit is set and this is a sort
	// query that cannot be bounded by a heap (no LIMIT, or LIMIT+DISTINCT),
	// stream rows through plan.Execute with periodic disk flushes to bound
	// peak memory regardless of result cardinality.
	// LIMIT+no-DISTINCT is excluded because selectJoinWithSortStreamingLimit
	// already bounds memory to O(LIMIT), or the heap path in selectWithSort
	// handles it in O(LIMIT) memory.
	if plan.SortInMemory && t.sortMemLimit > 0 && (!stmt.Limit.Valid || stmt.Distinct) {
		return t.selectWithSortSpill(ctx, stmt, plan, selectedFields, requestedFields)
	}

	// Materialising path: buffer every matching row, then dispatch.
	// Reached for JOIN queries that require semi/anti-semi joins — where all
	// rows must be collected before output can begin.
	// GROUP BY, aggregate, and COUNT(*) JOINs exit via dedicated paths above.
	// Simple JOINs (including DISTINCT and ORDER BY + LIMIT) exit via the
	// dedicated paths above.
	//
	// LIMIT pushdown: for JOIN queries with no in-memory sort we can stop
	// collecting rows after OFFSET+LIMIT rows, avoiding a full scan of every
	// matching row.  plan.Execute propagates errLimitReached from the callback;
	// the JOIN goroutine is cancelled and drained inside Execute itself.
	var joinScanLimit int64
	if len(plan.Joins) > 0 && stmt.Limit.Valid && !plan.SortInMemory {
		joinScanLimit = stmt.Limit.Value.(int64)
		if stmt.Offset.Valid {
			joinScanLimit += stmt.Offset.Value.(int64)
		}
	}

	var rows []Row
	err = plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		rows = append(rows, row)
		if joinScanLimit > 0 && int64(len(rows)) >= joinScanLimit {
			return errLimitReached
		}
		return nil
	})
	if err != nil && !errors.Is(err, errLimitReached) {
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
	return countResult(int64(len(rows))), nil
}

func countResult(count int64) StatementResult {
	return StatementResult{
		Columns: []Column{{Name: "COUNT(*)"}},
		Rows: NewSingleRowIterator(NewRowWithValues(
			[]Column{{Name: "COUNT(*)"}},
			[]OptionalValue{{Valid: true, Value: count}},
		)),
	}
}

// combinedJoinSchema builds the alias-prefixed column list for a JOIN query,
// mirroring the schema constructed inside executeNestedLoopJoin. The result is
// used to update stmt.Columns so that groupByAccumulator and newAggStates can
// resolve column indices against the combined join schema (e.g. "o.user_id")
// rather than the base table schema.
func combinedJoinSchema(ctx context.Context, plan QueryPlan, provider TableProvider) ([]Column, bool) {
	if len(plan.Joins) == 0 || len(plan.Scans) == 0 {
		return nil, false
	}
	baseScan := plan.Scans[0]
	baseTable, ok := provider.GetTable(ctx, baseScan.TableName)
	if !ok {
		return nil, false
	}
	firstJoin := plan.Joins[0]
	firstInnerScan := plan.Scans[firstJoin.RightScanIndex]
	firstInner, ok := provider.GetTable(ctx, firstInnerScan.TableName)
	if !ok {
		return nil, false
	}
	combined := buildCombinedColumns(baseTable.Columns, baseScan.TableAlias, firstInner.Columns, firstInnerScan.TableAlias)
	for i := 1; i < len(plan.Joins); i++ {
		join := plan.Joins[i]
		innerScan := plan.Scans[join.RightScanIndex]
		innerTable, ok := provider.GetTable(ctx, innerScan.TableName)
		if !ok {
			return nil, false
		}
		combined = buildCombinedColumnsProgressive(combined, innerTable.Columns, innerScan.TableAlias)
	}
	return combined, true
}

// countAllLeafWalk counts every row in the table.
//
// Fast path: if a row-count getter has been registered (set by the Database
// after loading the table), returns the cached count in O(1) without any I/O.
// The fast path is skipped when an active write transaction is in context
// because its uncommitted changes (in the WriteSet) are not yet reflected in
// the row-count cache — the page walk uses ReadPage which checks the WriteSet.
//
// Fallback: walks the B+ tree leaf page chain and sums Header.Cells on each
// page — O(leaf pages), no row data read or deserialised.
//
// This is only valid for COUNT(*) with no WHERE clause and no JOIN.
func (t *Table) countAllLeafWalk(ctx context.Context) (StatementResult, error) {
	var count int64
	tx := TxFromContext(ctx)
	useCache := t.getRowCount != nil && (tx == nil || tx.ReadOnly)
	if useCache {
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

	return countResult(count), nil
}

// aggState holds the running accumulator for a single aggregate expression.
// Used by the scalar aggregate path (no GROUP BY).
type aggState struct {
	min       OptionalValue
	max       OptionalValue
	count     int64
	sumI      int64
	sumF      float64
	useIntSum bool
	hasValue  bool
}

// groupAggState is the per-group accumulator used inside groupByAccumulator.
// min/max are omitted here and stored in a separate minMaxPool so COUNT/SUM/AVG
// queries don't pay 48 bytes per entry for fields they never use.
type groupAggState struct {
	count     int64
	sumI      int64
	sumF      float64
	useIntSum bool
	hasValue  bool
}

func (t *Table) selectAggregate(ctx context.Context, stmt Statement, rows []Row) (StatementResult, error) {
	states, aggColIdx := t.newAggStates(stmt)
	for _, row := range rows {
		if err := accumulateAggregateRow(stmt, states, aggColIdx, row); err != nil {
			return StatementResult{}, err
		}
	}
	_ = ctx // kept for signature compatibility with earlier callers.
	return t.aggregateResult(stmt, states), nil
}

func (t *Table) selectAggregateStreaming(ctx context.Context, stmt Statement, plan QueryPlan, selectedFields []Field) (StatementResult, error) {
	states, aggColIdx := t.newAggStates(stmt)
	if err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		return accumulateAggregateRow(stmt, states, aggColIdx, row)
	}); err != nil {
		return StatementResult{}, err
	}
	return t.aggregateResult(stmt, states), nil
}

func (t *Table) selectAggregateSequentialRowView(ctx context.Context, stmt Statement, scan Scan, selectedFields []Field) (StatementResult, error) {
	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}

	filter := t.compileRowViewScanFilter(scan, selectedFields)
	states, aggColIdx := t.newAggStates(stmt)

	page, err := t.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return StatementResult{}, fmt.Errorf("aggregate sequential scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return StatementResult{}, err
		}
		if page.Index != cursor.PageIdx {
			page, err = t.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return StatementResult{}, fmt.Errorf("aggregate sequential scan: %w", err)
			}
		}
		if cursor.CellIdx > page.LeafNode.Header.Cells-1 || len(page.LeafNode.Cells) == 0 {
			return StatementResult{}, fmt.Errorf("cell index %d out of bounds, max %d", cursor.CellIdx, page.LeafNode.Header.Cells-1)
		}

		cell := page.LeafNode.Cells[cursor.CellIdx]
		advanceLeafCursor(cursor, page)
		view := NewRowView(t.Columns, cell)

		ok, err := filter.accept(ctx, t.pager, view)
		if err != nil {
			return StatementResult{}, err
		}
		if !ok {
			continue
		}

		if err := accumulateAggregateRowView(ctx, t.pager, stmt.Aggregates, states, aggColIdx, view); err != nil {
			return StatementResult{}, err
		}
	}

	return t.aggregateResult(stmt, states), nil
}

func (t *Table) newAggStates(stmt Statement) ([]aggState, []int) {
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

	// Pre-compute column index for each aggregate's source column to avoid
	// per-row linear scans through column names.
	aggColIdx := make([]int, len(stmt.Aggregates))
	for i, agg := range stmt.Aggregates {
		aggColIdx[i] = -1
		if agg.Column == "" {
			continue
		}
		for j, col := range stmt.Columns {
			if col.Name == agg.Column {
				aggColIdx[i] = j
				break
			}
		}
	}
	return states, aggColIdx
}

func accumulateAggregateRow(stmt Statement, states []aggState, aggColIdx []int, row Row) error {
	for i, agg := range stmt.Aggregates {
		switch agg.Kind {
		case AggregateCount:
			states[i].count += 1

		case AggregateSum, AggregateAvg:
			val, ok := aggregateRowValue(row, agg.Column, aggColIdx[i])
			if !ok || !val.Valid {
				continue
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
			val, ok := aggregateRowValue(row, agg.Column, aggColIdx[i])
			if !ok || !val.Valid {
				continue
			}
			if !states[i].hasValue || compareValues(val, states[i].min) < 0 {
				states[i].min = val
				states[i].hasValue = true
			}

		case AggregateMax:
			val, ok := aggregateRowValue(row, agg.Column, aggColIdx[i])
			if !ok || !val.Valid {
				continue
			}
			if !states[i].hasValue || compareValues(val, states[i].max) > 0 {
				states[i].max = val
				states[i].hasValue = true
			}
		}
	}
	return nil
}

func accumulateAggregateRowView(ctx context.Context, pager TxPager, aggregates []AggregateExpr, states []aggState, aggColIdx []int, view RowView) error {
	for i, agg := range aggregates {
		switch agg.Kind {
		case AggregateCount:
			states[i].count += 1

		case AggregateSum, AggregateAvg:
			val, ok, err := aggregateRowViewValue(ctx, pager, view, aggColIdx[i])
			if err != nil {
				return err
			}
			if !ok || !val.Valid {
				continue
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
			val, ok, err := aggregateRowViewValue(ctx, pager, view, aggColIdx[i])
			if err != nil {
				return err
			}
			if !ok || !val.Valid {
				continue
			}
			if !states[i].hasValue || compareValues(val, states[i].min) < 0 {
				states[i].min = val
				states[i].hasValue = true
			}

		case AggregateMax:
			val, ok, err := aggregateRowViewValue(ctx, pager, view, aggColIdx[i])
			if err != nil {
				return err
			}
			if !ok || !val.Valid {
				continue
			}
			if !states[i].hasValue || compareValues(val, states[i].max) > 0 {
				states[i].max = val
				states[i].hasValue = true
			}
		}
	}
	return nil
}

func aggregateRowValue(row Row, colName string, colIdx int) (OptionalValue, bool) {
	if colIdx >= 0 && colIdx < len(row.Values) && colIdx < len(row.Columns) && row.Columns[colIdx].Name == colName {
		return row.Values[colIdx], true
	}
	return row.GetValue(colName)
}

func aggregateRowViewValue(ctx context.Context, pager TxPager, view RowView, colIdx int) (OptionalValue, bool, error) {
	if colIdx < 0 || colIdx >= len(view.Columns()) {
		return OptionalValue{}, false, nil
	}
	value, err := view.ValueAtWithOverflow(ctx, pager, colIdx)
	if err != nil {
		return OptionalValue{}, false, err
	}
	return value, true, nil
}

func (t *Table) aggregateResult(stmt Statement, states []aggState) StatementResult {
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
	}
}

func (t *Table) trySelectMinMaxFromIndexEndpoint(ctx context.Context, stmt Statement, plan QueryPlan) (StatementResult, bool, error) {
	if !stmt.IsSelectAggregate() || len(stmt.Aggregates) != 1 || len(stmt.Fields) != 1 || len(plan.Joins) > 0 || len(plan.Scans) != 1 {
		return StatementResult{}, false, nil
	}

	agg := stmt.Aggregates[0]
	scan := plan.Scans[0]
	switch {
	case agg.Kind == AggregateMin && scan.Type == ScanTypeIndexFirst:
	case agg.Kind == AggregateMax && scan.Type == ScanTypeIndexLast:
	default:
		return StatementResult{}, false, nil
	}
	if len(scan.Filters) > 0 {
		return StatementResult{}, false, nil
	}

	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return StatementResult{}, true, fmt.Errorf("no index found for min/max scan: %s", scan.IndexName)
	}

	var (
		endpointKey any
		found       bool
	)
	err := idx.ScanAll(ctx, scan.Type == ScanTypeIndexLast, func(key any, _ RowID) error {
		endpointKey = key
		found = true
		return errStopScan
	})
	if err != nil && !errors.Is(err, errStopScan) {
		return StatementResult{}, true, err
	}

	fieldName := stmt.Fields[0].OutputName()
	resultCol := Column{Name: fieldName}
	if col, ok := t.ColumnByName(agg.Column); ok {
		resultCol.Kind = col.Kind
		resultCol.Size = col.Size
	}

	value := OptionalValue{}
	if found {
		value = OptionalValue{Valid: true, Value: endpointKey}
	}
	row := NewRowWithValues([]Column{resultCol}, []OptionalValue{value})
	return StatementResult{
		Columns: []Column{resultCol},
		Rows:    NewSingleRowIterator(row),
	}, true, nil
}

// groupEntry holds per-group state offsets into the shared flat pools.
// Using flat pools eliminates one heap allocation per group for aggStates and
// groupValues slices. Index-based access remains safe across pool reallocations
// because Go copies pool contents on grow.
type groupEntry struct {
	aggStateStart int32
	groupValStart int32
	minMaxStart   int32 // index into acc.minMaxPool; -1 if query has no MIN/MAX
}

// groupByAccumulator holds all streaming GROUP BY state. Create with
// newGroupByAccumulator, feed rows with process(), then call buildResult().
// Separating state from the row loop lets the zero-alloc sequential scan path
// share the same result-building code as the general materialised path.
type groupByAccumulator struct {
	aggregates        []AggregateExpr
	useIntSum         []bool
	groupByColIdx     []int
	aggColIdx         []int
	fieldToGroupByIdx []int
	groupMap          map[string]int32
	groupEntries      []groupEntry
	aggStatePool      []groupAggState
	groupValPool      []OptionalValue
	keyBuf            []byte
	// minMaxPool holds one OptionalValue per (group × numMinMax). Only allocated
	// when the query contains at least one MIN or MAX aggregate.
	minMaxPool    []OptionalValue
	minMaxAggSlot []int // aggIdx → slot within group's minMax block (-1 if not MIN/MAX)
	numMinMax     int
}

func newGroupByAccumulator(stmt Statement, t *Table, estRows int) *groupByAccumulator {
	numAggs := len(stmt.Aggregates)
	numGroupBy := len(stmt.GroupBy)

	// Determine integer vs float accumulation mode for SUM/AVG per aggregate.
	useIntSum := make([]bool, numAggs)
	for i, agg := range stmt.Aggregates {
		if agg.Kind != AggregateSum && agg.Kind != AggregateAvg {
			continue
		}
		if col, ok := stmt.ColumnByName(agg.Column); ok {
			useIntSum[i] = col.Kind.IsInt()
		}
	}

	// Pre-compute column index for each GROUP BY field (avoids per-row linear scans).
	groupByColIdx := make([]int, numGroupBy)
	for i, f := range stmt.GroupBy {
		groupByColIdx[i] = groupByColumnIndex(stmt.Columns, f)
	}

	// Pre-compute column index for each aggregate's source column.
	aggColIdx := make([]int, numAggs)
	for i, agg := range stmt.Aggregates {
		aggColIdx[i] = -1
		if agg.Column == "" || agg.Kind == 0 || agg.Kind == AggregateCount {
			continue
		}
		for j, col := range stmt.Columns {
			if col.Name == agg.Column {
				aggColIdx[i] = j
				break
			}
		}
	}

	// Pre-compute field → GROUP BY index for result emission (avoids map lookup per group per field).
	fieldToGroupByIdx := make([]int, numAggs)
	for i := range stmt.Fields {
		fieldToGroupByIdx[i] = -1
		if stmt.Aggregates[i].Kind != 0 {
			continue
		}
		// Build the qualified form of the SELECT field name (e.g. "o.user_id") so
		// that JOIN queries with alias-prefixed SELECT columns match GROUP BY entries
		// that store the full qualified name (e.g. Field{Name: "o.user_id"}).
		qualifiedFieldName := stmt.Fields[i].Name
		if stmt.Fields[i].AliasPrefix != "" {
			qualifiedFieldName = stmt.Fields[i].AliasPrefix + "." + stmt.Fields[i].Name
		}
		for j, gf := range stmt.GroupBy {
			if gf.Name == stmt.Fields[i].Name || gf.Name == qualifiedFieldName {
				fieldToGroupByIdx[i] = j
				break
			}
		}
	}

	// Map each aggregate index to its MIN/MAX slot (only for MIN/MAX aggregates).
	numMinMax := 0
	minMaxAggSlot := make([]int, numAggs)
	for i, agg := range stmt.Aggregates {
		if agg.Kind == AggregateMin || agg.Kind == AggregateMax {
			minMaxAggSlot[i] = numMinMax
			numMinMax += 1
		} else {
			minMaxAggSlot[i] = -1
		}
	}

	// Estimate the number of distinct groups using a 1%-cardinality heuristic
	// (estRows/100). This is more accurate than estRows/10 for typical aggregation
	// queries and avoids the pool growth that occurs when actual groups exceed the
	// initial capacity. The cap at 512 prevents excessive upfront allocation for
	// very large tables. If index statistics are available for a GROUP BY column,
	// use the NDV (number of distinct values) directly.
	estGroups := estRows / 100
	if estGroups < 16 {
		estGroups = 16
	} else if estGroups > 512 {
		estGroups = 512
	}
	for _, colIdx := range groupByColIdx {
		if colIdx < 0 || colIdx >= len(t.Columns) {
			continue
		}
		colName := t.Columns[colIdx].Name
		for idxName, idx := range t.SecondaryIndexes {
			if len(idx.Columns) > 0 && idx.Columns[0].Name == colName {
				if stats, ok := t.indexStats[idxName]; ok && len(stats.NDistinct) > 0 && stats.NDistinct[0] > 0 {
					ndv := int(stats.NDistinct[0])
					if ndv > estGroups {
						if ndv > 512 {
							ndv = 512
						}
						estGroups = ndv
					}
				}
				break
			}
		}
	}

	var minMaxPool []OptionalValue
	if numMinMax > 0 {
		minMaxPool = make([]OptionalValue, 0, estGroups*numMinMax)
	}

	return &groupByAccumulator{
		aggregates:        stmt.Aggregates,
		useIntSum:         useIntSum,
		groupByColIdx:     groupByColIdx,
		aggColIdx:         aggColIdx,
		fieldToGroupByIdx: fieldToGroupByIdx,
		groupMap:          make(map[string]int32, estGroups),
		groupEntries:      make([]groupEntry, 0, estGroups),
		aggStatePool:      make([]groupAggState, 0, estGroups*numAggs),
		groupValPool:      make([]OptionalValue, 0, estGroups*numGroupBy),
		minMaxPool:        minMaxPool,
		minMaxAggSlot:     minMaxAggSlot,
		numMinMax:         numMinMax,
	}
}

// process accumulates one row into the group state. Safe to call with a
// reused Row.Values buffer as long as the caller does not retain the row
// after returning — values needed for grouping are copied into groupValPool.
func (acc *groupByAccumulator) process(row Row) {
	acc.keyBuf = buildGroupKey(acc.keyBuf[:0], row, acc.groupByColIdx)

	// Use string(acc.keyBuf) only for the map lookup — the compiler elides the
	// allocation when the string is used solely as a map key (no escape).
	gsIdx, exists := acc.groupMap[string(acc.keyBuf)]
	if !exists {
		aggStart := int32(len(acc.aggStatePool))
		for i := range len(acc.aggregates) {
			acc.aggStatePool = append(acc.aggStatePool, groupAggState{useIntSum: acc.useIntSum[i]})
		}

		gvStart := int32(len(acc.groupValPool))
		for _, colIdx := range acc.groupByColIdx {
			if colIdx >= 0 && colIdx < len(row.Values) {
				acc.groupValPool = append(acc.groupValPool, row.Values[colIdx])
			} else {
				acc.groupValPool = append(acc.groupValPool, OptionalValue{})
			}
		}

		mmStart := int32(-1)
		if acc.numMinMax > 0 {
			mmStart = int32(len(acc.minMaxPool))
			for range acc.numMinMax {
				acc.minMaxPool = append(acc.minMaxPool, OptionalValue{})
			}
		}

		gsIdx = int32(len(acc.groupEntries))
		acc.groupEntries = append(acc.groupEntries, groupEntry{
			aggStateStart: aggStart,
			groupValStart: gvStart,
			minMaxStart:   mmStart,
		})
		acc.groupMap[string(acc.keyBuf)] = gsIdx
	}

	aggBase := int(acc.groupEntries[gsIdx].aggStateStart)
	mmBase := int(acc.groupEntries[gsIdx].minMaxStart)
	for i, agg := range acc.aggregates {
		switch agg.Kind {
		case 0:
			// Non-aggregate GROUP BY column — no accumulation needed.
		case AggregateCount:
			acc.aggStatePool[aggBase+i].count += 1
		case AggregateSum, AggregateAvg:
			colIdx := acc.aggColIdx[i]
			if colIdx < 0 || colIdx >= len(row.Values) {
				continue
			}
			val := row.Values[colIdx]
			if !val.Valid {
				continue
			}
			acc.aggStatePool[aggBase+i].count += 1
			acc.aggStatePool[aggBase+i].hasValue = true
			if acc.aggStatePool[aggBase+i].useIntSum {
				switch v := val.Value.(type) {
				case int64:
					acc.aggStatePool[aggBase+i].sumI += v
				case int32:
					acc.aggStatePool[aggBase+i].sumI += int64(v)
				}
			} else {
				switch v := val.Value.(type) {
				case float64:
					acc.aggStatePool[aggBase+i].sumF += v
				case float32:
					acc.aggStatePool[aggBase+i].sumF += float64(v)
				}
			}
		case AggregateMin:
			colIdx := acc.aggColIdx[i]
			if colIdx < 0 || colIdx >= len(row.Values) {
				continue
			}
			val := row.Values[colIdx]
			if !val.Valid {
				continue
			}
			slot := mmBase + acc.minMaxAggSlot[i]
			if !acc.minMaxPool[slot].Valid || compareValues(val, acc.minMaxPool[slot]) < 0 {
				acc.minMaxPool[slot] = val
			}
		case AggregateMax:
			colIdx := acc.aggColIdx[i]
			if colIdx < 0 || colIdx >= len(row.Values) {
				continue
			}
			val := row.Values[colIdx]
			if !val.Valid {
				continue
			}
			slot := mmBase + acc.minMaxAggSlot[i]
			if !acc.minMaxPool[slot].Valid || compareValues(val, acc.minMaxPool[slot]) > 0 {
				acc.minMaxPool[slot] = val
			}
		}
	}
}

func (acc *groupByAccumulator) processView(view RowView) error {
	var err error
	acc.keyBuf, err = buildGroupKeyFromRowView(acc.keyBuf[:0], view, acc.groupByColIdx)
	if err != nil {
		return err
	}

	gsIdx, exists := acc.groupMap[string(acc.keyBuf)]
	if !exists {
		aggStart := int32(len(acc.aggStatePool))
		for i := range len(acc.aggregates) {
			acc.aggStatePool = append(acc.aggStatePool, groupAggState{useIntSum: acc.useIntSum[i]})
		}

		gvStart := int32(len(acc.groupValPool))
		for _, colIdx := range acc.groupByColIdx {
			if colIdx >= 0 && colIdx < len(view.Columns()) {
				val, err := view.ValueAt(colIdx)
				if err != nil {
					return err
				}
				acc.groupValPool = append(acc.groupValPool, val)
			} else {
				acc.groupValPool = append(acc.groupValPool, OptionalValue{})
			}
		}

		mmStart := int32(-1)
		if acc.numMinMax > 0 {
			mmStart = int32(len(acc.minMaxPool))
			for range acc.numMinMax {
				acc.minMaxPool = append(acc.minMaxPool, OptionalValue{})
			}
		}

		gsIdx = int32(len(acc.groupEntries))
		acc.groupEntries = append(acc.groupEntries, groupEntry{
			aggStateStart: aggStart,
			groupValStart: gvStart,
			minMaxStart:   mmStart,
		})
		acc.groupMap[string(acc.keyBuf)] = gsIdx
	}

	aggBase := int(acc.groupEntries[gsIdx].aggStateStart)
	mmBase := int(acc.groupEntries[gsIdx].minMaxStart)
	for i, agg := range acc.aggregates {
		state := &acc.aggStatePool[aggBase+i]
		switch agg.Kind {
		case 0:
			// Non-aggregate GROUP BY column — no accumulation needed.
		case AggregateCount:
			state.count += 1
		case AggregateSum, AggregateAvg:
			colIdx := acc.aggColIdx[i]
			if colIdx < 0 || colIdx >= len(view.Columns()) {
				continue
			}
			val, err := view.ValueAt(colIdx)
			if err != nil {
				return err
			}
			if !val.Valid {
				continue
			}
			state.count += 1
			state.hasValue = true
			if state.useIntSum {
				switch v := val.Value.(type) {
				case int64:
					state.sumI += v
				case int32:
					state.sumI += int64(v)
				}
			} else {
				switch v := val.Value.(type) {
				case float64:
					state.sumF += v
				case float32:
					state.sumF += float64(v)
				}
			}
		case AggregateMin:
			colIdx := acc.aggColIdx[i]
			if colIdx < 0 || colIdx >= len(view.Columns()) {
				continue
			}
			val, err := view.ValueAt(colIdx)
			if err != nil {
				return err
			}
			if !val.Valid {
				continue
			}
			slot := mmBase + acc.minMaxAggSlot[i]
			if !acc.minMaxPool[slot].Valid || compareValues(val, acc.minMaxPool[slot]) < 0 {
				acc.minMaxPool[slot] = val
			}
		case AggregateMax:
			colIdx := acc.aggColIdx[i]
			if colIdx < 0 || colIdx >= len(view.Columns()) {
				continue
			}
			val, err := view.ValueAt(colIdx)
			if err != nil {
				return err
			}
			if !val.Valid {
				continue
			}
			slot := mmBase + acc.minMaxAggSlot[i]
			if !acc.minMaxPool[slot].Valid || compareValues(val, acc.minMaxPool[slot]) > 0 {
				acc.minMaxPool[slot] = val
			}
		}
	}

	return nil
}

func (t *Table) selectGroupBy(ctx context.Context, stmt Statement, rows []Row) (StatementResult, error) {
	_ = ctx // ctx kept for signature compat; no blocking ops remain
	acc := newGroupByAccumulator(stmt, t, len(rows))
	for _, row := range rows {
		acc.process(row)
	}
	return acc.buildResult(stmt, t)
}

func (t *Table) selectGroupByStreaming(ctx context.Context, stmt Statement, plan QueryPlan, selectedFields []Field) (StatementResult, error) {
	estRows := int(t.estimatedRowCount())
	if estRows <= 0 {
		estRows = 160 // conservative default (estGroups = 16)
	}
	acc := newGroupByAccumulator(stmt, t, estRows)
	if err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		acc.process(row)
		return nil
	}); err != nil {
		return StatementResult{}, err
	}
	return acc.buildResult(stmt, t)
}

// selectGroupByZeroAlloc handles GROUP BY over a single sequential scan by
// accumulating directly from RowView. Falls back to the general path for virtual
// tables and parallel scans.
func (t *Table) selectGroupByZeroAlloc(ctx context.Context, stmt Statement, scan Scan, selectedFields []Field) (StatementResult, error) {
	if t.virtualRows != nil || t.parallelScan {
		estRows := int(t.estimatedRowCount())
		if estRows <= 0 {
			estRows = 160 // conservative default (estGroups = 16)
		}
		acc := newGroupByAccumulator(stmt, t, estRows)
		if err := t.sequentialScan(ctx, scan, selectedFields, func(row Row) error {
			acc.process(row)
			return nil
		}); err != nil {
			return StatementResult{}, err
		}
		return acc.buildResult(stmt, t)
	}

	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}

	filter := t.compileRowViewScanFilter(scan, selectedFields)

	estRows := int(t.estimatedRowCount())
	if estRows <= 0 {
		estRows = 80 // conservative default (estGroups = 8)
	}
	acc := newGroupByAccumulator(stmt, t, estRows)

	page, err := t.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return StatementResult{}, fmt.Errorf("group by sequential scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return StatementResult{}, err
		}

		if page.Index != cursor.PageIdx {
			page, err = t.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return StatementResult{}, fmt.Errorf("group by sequential scan: %w", err)
			}
		}

		cell := page.LeafNode.Cells[cursor.CellIdx]

		switch {
		case cursor.CellIdx < page.LeafNode.Header.Cells-1:
			cursor.CellIdx += 1
		case page.LeafNode.Header.NextLeaf == 0:
			cursor.EndOfTable = true
		default:
			cursor.PageIdx = page.LeafNode.Header.NextLeaf
			cursor.CellIdx = 0
		}

		view := NewRowView(t.Columns, cell)

		ok, err := filter.accept(ctx, t.pager, view)
		if err != nil {
			return StatementResult{}, err
		}
		if !ok {
			continue
		}

		if err := acc.processView(view); err != nil {
			return StatementResult{}, err
		}
	}

	return acc.buildResult(stmt, t)
}

// computeGroupValues fills values[0:nFields] with the aggregate results for the
// gi-th group (in insertion order). The caller must pre-allocate values.
func (acc *groupByAccumulator) computeGroupValues(gi int, values []OptionalValue) {
	entry := acc.groupEntries[gi]
	aggBase := int(entry.aggStateStart)
	gvBase := int(entry.groupValStart)
	mmBase := int(entry.minMaxStart)
	for i, agg := range acc.aggregates {
		switch agg.Kind {
		case 0:
			if j := acc.fieldToGroupByIdx[i]; j >= 0 {
				values[i] = acc.groupValPool[gvBase+j]
			} else {
				values[i] = OptionalValue{}
			}
		case AggregateCount:
			values[i] = OptionalValue{Valid: true, Value: acc.aggStatePool[aggBase+i].count}
		case AggregateSum:
			st := acc.aggStatePool[aggBase+i]
			if st.hasValue {
				if st.useIntSum {
					values[i] = OptionalValue{Valid: true, Value: st.sumI}
				} else {
					values[i] = OptionalValue{Valid: true, Value: st.sumF}
				}
			} else {
				values[i] = OptionalValue{}
			}
		case AggregateAvg:
			st := acc.aggStatePool[aggBase+i]
			if st.hasValue && st.count > 0 {
				if st.useIntSum {
					values[i] = OptionalValue{Valid: true, Value: float64(st.sumI) / float64(st.count)}
				} else {
					values[i] = OptionalValue{Valid: true, Value: st.sumF / float64(st.count)}
				}
			} else {
				values[i] = OptionalValue{}
			}
		case AggregateMin:
			slot := mmBase + acc.minMaxAggSlot[i]
			if acc.minMaxPool[slot].Valid {
				values[i] = acc.minMaxPool[slot]
			} else {
				values[i] = OptionalValue{}
			}
		case AggregateMax:
			slot := mmBase + acc.minMaxAggSlot[i]
			if acc.minMaxPool[slot].Valid {
				values[i] = acc.minMaxPool[slot]
			} else {
				values[i] = OptionalValue{}
			}
		}
	}
}

func (acc *groupByAccumulator) buildResult(stmt Statement, t *Table) (StatementResult, error) {
	nFields := len(stmt.Fields)
	nGroups := len(acc.groupEntries)

	// Build result column metadata.
	resultColumns := make([]Column, nFields)
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
			if acc.useIntSum[i] {
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

	// Preallocate one flat block for all group values — one alloc covers every group.
	// passedIndices tracks which groups passed HAVING, using int32 (4 bytes each)
	// instead of full Row structs.
	allResultValues := make([]OptionalValue, nGroups*nFields)
	passedIndices := make([]int32, 0, nGroups)

	for gi := range nGroups {
		values := allResultValues[gi*nFields : (gi+1)*nFields]
		acc.computeGroupValues(gi, values)

		// Apply HAVING filter against the computed aggregate row.
		if len(stmt.Having) > 0 {
			resultRow := NewRowWithValues(resultColumns, values)
			ok, err := resultRow.CheckOneOrMore(stmt.Having)
			if err != nil {
				return StatementResult{}, fmt.Errorf("HAVING: %w", err)
			}
			if !ok {
				continue
			}
		}

		passedIndices = append(passedIndices, int32(gi))
	}

	// Apply ORDER BY if specified — sort passedIndices by comparing allResultValues slices.
	if len(stmt.OrderBy) > 0 {
		slices.SortFunc(passedIndices, func(a, b int32) int {
			rowA := NewRowWithValues(resultColumns, allResultValues[int(a)*nFields:(int(a)+1)*nFields])
			rowB := NewRowWithValues(resultColumns, allResultValues[int(b)*nFields:(int(b)+1)*nFields])
			for _, clause := range stmt.OrderBy {
				valA, foundA := rowA.getValueQualified(clause.Field.AliasPrefix, clause.Field.Name)
				valB, foundB := rowB.getValueQualified(clause.Field.AliasPrefix, clause.Field.Name)
				if !foundA || !foundB {
					continue
				}
				cmp := compareValues(valA, valB)
				if cmp == 0 {
					continue
				}
				if clause.Direction == Desc {
					return -cmp
				}
				return cmp
			}
			return 0
		})
	}

	// Apply OFFSET.
	if stmt.Offset.Valid {
		offset := int(stmt.Offset.Value.(int64))
		if offset >= len(passedIndices) {
			passedIndices = passedIndices[:0]
		} else {
			passedIndices = passedIndices[offset:]
		}
	}

	// Apply LIMIT.
	if stmt.Limit.Valid {
		limit := int(stmt.Limit.Value.(int64))
		if limit < len(passedIndices) {
			passedIndices = passedIndices[:limit]
		}
	}

	idx := 0
	return StatementResult{
		Columns: resultColumns,
		Rows: NewIterator(func(ctx context.Context) (Row, error) {
			if idx >= len(passedIndices) {
				return Row{}, ErrNoMoreRows
			}
			gi := int(passedIndices[idx])
			idx += 1
			return NewRowWithValues(resultColumns, allResultValues[gi*nFields:(gi+1)*nFields]), nil
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

	// When the index delivers rows in ORDER BY order and every projected field
	// is in ORDER BY, equal projected rows are adjacent → adjacent-compare dedup
	// uses O(1) memory instead of an O(N) hash set.
	adjacentDedup := stmt.Distinct && !plan.SortInMemory && len(plan.OrderBy) > 0 && allProjectedInOrderBy(requestedFields, plan.OrderBy)
	var seen map[string]struct{}
	if stmt.Distinct && !adjacentDedup {
		seen = make(map[string]struct{})
	}
	var prevDistinctKey string

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
			if adjacentDedup {
				if key == prevDistinctKey {
					return nil
				}
				prevDistinctKey = key
			} else {
				if _, dup := seen[key]; dup {
					return nil
				}
				seen[key] = struct{}{}
			}
		}
		if hasOffset && offset > 0 {
			offset -= 1
			return nil
		}
		projected = append(projected, p)
		if hasLimit {
			remaining -= 1
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
	result.rawRows = projected
	return result, nil
}

func (t *Table) selectStreamingDirectCoveringIndex(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if stmt.Distinct || len(plan.Scans) != 1 {
		return StatementResult{}, false, nil
	}
	scan := plan.resolvedScan(0)
	if !scan.CoveringIndex {
		return StatementResult{}, false, nil
	}
	switch scan.Type {
	case ScanTypeIndexAll, ScanTypeIndexRange, ScanTypeIndexPoint:
	default:
		return StatementResult{}, false, nil
	}
	for _, field := range requestedFields {
		if field.Expr != nil {
			return StatementResult{}, false, nil
		}
	}

	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return StatementResult{}, true, fmt.Errorf("no index found for covering scan: %s", scan.IndexName)
	}

	resultColumns := make([]Column, len(requestedFields))
	projectionIndexes := make([]int, len(requestedFields))
	for i, field := range requestedFields {
		col, colIdx := columnByFieldName(scan.IndexColumns, field.Name)
		if colIdx < 0 {
			return StatementResult{}, false, nil
		}
		col.Name = field.OutputName()
		resultColumns[i] = col
		projectionIndexes[i] = colIdx
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

	filter := compileScanFilter(scan.IndexColumns, scan.Filters)

	return StatementResult{
		Columns: resultColumns,
		Rows: t.coveringIndexIterator(
			ctx,
			idx,
			plan,
			scan,
			filter,
			projectionIndexes,
			resultColumns,
			remaining,
			offset,
			hasLimit,
			hasOffset,
		),
	}, true, nil
}

type coveringIndexIteratorItem struct {
	row Row
	err error
}

func (t *Table) coveringIndexIterator(
	ctx context.Context,
	idx BTreeIndex,
	plan QueryPlan,
	scan Scan,
	filter func(Row) (bool, error),
	projectionIndexes []int,
	resultColumns []Column,
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) Iterator {
	if hasLimit && remaining == 0 {
		return NewSliceIterator(nil)
	}

	iterCtx, cancel := context.WithCancel(ctx)
	ch := make(chan coveringIndexIteratorItem, 16)

	send := func(item coveringIndexIteratorItem) error {
		select {
		case <-iterCtx.Done():
			return errLimitReached
		case ch <- item:
			return nil
		}
	}

	emit := func(key any, rowID RowID) error {
		if filter != nil {
			row := rowFromIndexKey(key, scan.IndexColumns, rowID)
			ok, err := filter(row)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		if hasOffset && offset > 0 {
			offset -= 1
			return nil
		}
		projectedRow, err := projectCoveringIndexKey(key, scan.IndexColumns, rowID, projectionIndexes, resultColumns)
		if err != nil {
			return err
		}
		if err := send(coveringIndexIteratorItem{row: projectedRow}); err != nil {
			return err
		}
		if hasLimit {
			remaining -= 1
			if remaining == 0 {
				return errLimitReached
			}
		}
		return iterCtx.Err()
	}

	go func() {
		defer close(ch)
		err := scanCoveringIndexRows(iterCtx, idx, plan, scan, emit)
		if err != nil && !errors.Is(err, errLimitReached) && !errors.Is(err, context.Canceled) {
			_ = send(coveringIndexIteratorItem{err: err})
		}
	}()

	return newIteratorWithClose(func(ctx context.Context) (Row, error) {
		select {
		case <-ctx.Done():
			cancel()
			return Row{}, ctx.Err()
		case item, ok := <-ch:
			if !ok {
				return Row{}, ErrNoMoreRows
			}
			if item.err != nil {
				return Row{}, item.err
			}
			return item.row, nil
		}
	}, func() error {
		cancel()
		return nil
	})
}

func scanCoveringIndexRows(ctx context.Context, idx BTreeIndex, plan QueryPlan, scan Scan, emit func(any, RowID) error) error {
	switch scan.Type {
	case ScanTypeIndexAll:
		return idx.ScanAll(ctx, plan.SortReverse, emit)
	case ScanTypeIndexRange:
		return idx.ScanRange(ctx, scan.RangeCondition, plan.SortReverse, emit)
	case ScanTypeIndexPoint:
		for _, indexValue := range scan.IndexKeys {
			err := idx.VisitRowIDs(ctx, indexValue, func(rowID RowID) error {
				return emit(indexValue, rowID)
			})
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if errors.Is(err, errLimitReached) {
				return err
			}
			if err != nil {
				return fmt.Errorf("index lookup failed: %w", err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported covering index scan type: %s", scan.Type)
	}
}

func (t *Table) selectStreamingDirectRowView(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if t.virtualRows != nil || len(plan.Scans) != 1 {
		return StatementResult{}, false, nil
	}
	var fieldIndexes []int
	var resultColumns []Column
	if plan.CachedFieldIndexes != nil {
		fieldIndexes = plan.CachedFieldIndexes
		resultColumns = plan.CachedResultColumns
	} else {
		var ok bool
		fieldIndexes, resultColumns, ok = rowViewProjectionPlan(t.Columns, requestedFields, stmt.TableName, stmt.TableAlias)
		if !ok {
			return StatementResult{}, false, nil
		}
	}

	result := StatementResult{Columns: resultColumns}
	scan := plan.resolvedScan(0)
	var tableFilter func(context.Context, RowView) (bool, error)
	if scan.Type == ScanTypeInverted {
		var ok bool
		tableFilter, ok = compileInvertedRowViewFilter(t.Columns, t.pager, scan.Filters)
		if !ok {
			return StatementResult{}, false, nil
		}
	} else {
		if !rowViewFilterSupports(t.Columns, scan.Filters) {
			return StatementResult{}, false, nil
		}
		tableFilter = compileRowViewFilterForColumns(t.Columns, t.pager, scan.Filters)
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

	iterRemaining := remaining
	iterOffset := offset
	iterHasLimit := hasLimit
	iterHasOffset := hasOffset
	if stmt.Distinct {
		iterRemaining = 0
		iterOffset = 0
		iterHasLimit = false
		iterHasOffset = false
	}

	var newRowViewIter func() RowViewIterator
	switch scan.Type {
	case ScanTypeSequential:
		var (
			iterFactory func() RowViewIterator
			err         error
		)
		if t.parallelScan {
			iterFactory, err = t.parallelSequentialRowViewIteratorFactory(ctx, tableFilter, iterRemaining, iterOffset, iterHasLimit, iterHasOffset)
		} else {
			iterFactory, err = t.sequentialRowViewIteratorFactory(ctx, tableFilter, iterRemaining, iterOffset, iterHasLimit, iterHasOffset)
		}
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	case ScanTypeIndexAll, ScanTypeIndexRange:
		if scan.CoveringIndex {
			return StatementResult{}, false, nil
		}
		iterFactory, err := t.indexRowViewIteratorFactory(ctx, plan, scan, tableFilter, iterRemaining, iterOffset, iterHasLimit, iterHasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	case ScanTypeIndexPoint:
		if scan.CoveringIndex {
			return StatementResult{}, false, nil
		}
		if t.isUniquePointIndex(scan.IndexName) {
			iterFactory, err := t.uniqueIndexPointRowViewIteratorFactory(scan, tableFilter, iterRemaining, iterOffset, iterHasLimit, iterHasOffset)
			if err != nil {
				return StatementResult{}, true, err
			}
			newRowViewIter = iterFactory
			break
		}
		iterFactory, err := t.indexRowViewIteratorFactory(ctx, plan, scan, tableFilter, iterRemaining, iterOffset, iterHasLimit, iterHasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	case ScanTypeIndexIntersect, ScanTypeIndexUnion:
		iterFactory, err := t.indexSetRowViewIteratorFactory(ctx, scan, tableFilter, iterRemaining, iterOffset, iterHasLimit, iterHasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	case ScanTypeFullText:
		iterFactory, ok, err := t.fullTextRowViewIteratorFactory(ctx, scan, tableFilter, iterRemaining, iterOffset, iterHasLimit, iterHasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		if !ok {
			return StatementResult{}, false, nil
		}
		newRowViewIter = iterFactory
	case ScanTypeInverted:
		iterFactory, err := t.invertedRowViewIteratorFactory(ctx, scan, tableFilter, iterRemaining, iterOffset, iterHasLimit, iterHasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	default:
		return StatementResult{}, false, nil
	}

	if stmt.Distinct {
		var distinctFactory func() RowViewIterator
		if !plan.SortInMemory && allProjectedInOrderBy(requestedFields, plan.OrderBy) {
			distinctFactory = newDistinctAdjacentRowViewIteratorFactory(
				ctx, t.pager, newRowViewIter, fieldIndexes, t.Columns,
				remaining, offset, hasLimit, hasOffset,
			)
		} else {
			distinctFactory = newDistinctRowViewIteratorFactory(
				ctx, t.pager, newRowViewIter, fieldIndexes, t.Columns,
				distinctSeenCapacityFromEstimate(t.estimatedRowCount()),
				remaining, offset, hasLimit, hasOffset,
			)
		}
		result.RowViews = distinctFactory()
		result.RowViewPager = t.pager
		result.RowViewFieldIndexes = fieldIndexes
		result.Rows = lazyRowViewMaterializingIterator(t.pager, distinctFactory, fieldIndexes, resultColumns)
		return result, true, nil
	}

	result.RowViews = newRowViewIter()
	result.RowViewPager = t.pager
	result.RowViewFieldIndexes = fieldIndexes
	result.Rows = lazyRowViewMaterializingIterator(t.pager, newRowViewIter, fieldIndexes, resultColumns)
	return result, true, nil
}

// selectSemiJoinDirectRowView is a fast path for SELECT queries whose only join
// is a single hash semi-join and whose outer table is a non-virtual sequential
// scan with RowView-compatible conditions.  It builds the inner hash bucket once,
// then scans the outer table using RowViews and probes the bucket without
// materialising non-matching rows, wiring matched RowViews straight to the
// driver layer via result.RowViews.
func (t *Table) selectSemiJoinDirectRowView(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if len(plan.Joins) != 1 || plan.Joins[0].Type != Semi {
		return StatementResult{}, false, nil
	}
	if t.virtualRows != nil || t.parallelScan {
		return StatementResult{}, false, nil
	}
	if stmt.IsSelectGroupBy() || stmt.IsSelectAggregate() || stmt.Distinct || plan.SortInMemory {
		return StatementResult{}, false, nil
	}
	if len(plan.Scans) == 0 || plan.Scans[0].Type != ScanTypeSequential {
		return StatementResult{}, false, nil
	}

	outerScan := plan.Scans[0]

	// Output must be projectable from outer table columns only (semi-join yields no inner columns).
	fieldIndexes, resultColumns, ok := rowViewProjectionPlan(t.Columns, requestedFields, stmt.TableName, stmt.TableAlias)
	if !ok {
		return StatementResult{}, false, nil
	}
	if !rowViewFilterSupports(t.Columns, outerScan.Filters) {
		return StatementResult{}, false, nil
	}
	outerFilter := compileRowViewFilterForColumns(t.Columns, t.pager, outerScan.Filters)

	hashTables, err := buildHashBuckets(ctx, plan, t.provider)
	if err != nil {
		return StatementResult{}, true, err
	}
	bucket, hasBucket := hashTables[0]
	if !hasBucket || (bucket.present == nil && bucket.intPresent == nil) {
		return StatementResult{}, false, nil
	}

	join := plan.Joins[0]

	var remaining, offset int64
	hasLimit := stmt.Limit.Valid
	hasOffset := stmt.Offset.Valid
	if hasLimit {
		remaining = stmt.Limit.Value.(int64)
	}
	if hasOffset {
		offset = stmt.Offset.Value.(int64)
	}

	newRowViewIter, err := semiJoinProbeRowViewIteratorFactory(
		ctx, t, join, bucket, outerFilter,
		remaining, offset, hasLimit, hasOffset,
	)
	if err != nil {
		return StatementResult{}, true, err
	}

	result := StatementResult{Columns: resultColumns}
	result.RowViews = newRowViewIter()
	result.RowViewPager = t.pager
	result.RowViewFieldIndexes = fieldIndexes
	result.Rows = lazyRowViewMaterializingIterator(t.pager, newRowViewIter, fieldIndexes, resultColumns)
	return result, true, nil
}

// selectHashJoinDirectRowView is a fast path for SELECT queries whose only join
// is a single INNER or LEFT hash join with no extra join conditions, a sequential
// outer scan, and a compact-cell inner build (bucket.cells != nil). It bypasses
// the goroutine + channel path of selectStreamingJoin and delivers combined
// RowViews directly to the driver without allocating []OptionalValue per row.
//
// Guards: single join, INNER or LEFT type, hash algorithm, no extra join
// conditions, non-virtual/non-parallel outer scan, RowView-compatible outer
// filters, compact-cell inner bucket.
func (t *Table) selectHashJoinDirectRowView(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if len(plan.Joins) != 1 {
		return StatementResult{}, false, nil
	}
	join := plan.Joins[0]
	if join.Type != Inner && join.Type != Left {
		return StatementResult{}, false, nil
	}
	if join.Algorithm != JoinAlgorithmHash {
		return StatementResult{}, false, nil
	}
	// Bail out if there are extra ON conditions beyond the equi-join key pairs:
	// those require per-row evaluation that this fast path does not perform.
	if len(join.Conditions) > len(join.JoinColumnPairs) {
		return StatementResult{}, false, nil
	}
	if stmt.IsSelectGroupBy() || stmt.IsSelectAggregate() || stmt.IsSelectCountAll() || stmt.Distinct || plan.SortInMemory {
		return StatementResult{}, false, nil
	}
	if len(plan.Scans) == 0 || plan.Scans[0].Type != ScanTypeSequential {
		return StatementResult{}, false, nil
	}

	outerScan := plan.Scans[0]
	outerTable, ok := t.provider.GetTable(ctx, outerScan.TableName)
	if !ok {
		return StatementResult{}, true, minisqlErrors.ErrNoSuchTable{Name: outerScan.TableName}
	}
	if outerTable.virtualRows != nil || outerTable.parallelScan {
		return StatementResult{}, false, nil
	}
	if !rowViewFilterSupports(outerTable.Columns, outerScan.Filters) {
		return StatementResult{}, false, nil
	}

	innerScan := plan.Scans[join.RightScanIndex]

	innerTable, ok := t.provider.GetTable(ctx, innerScan.TableName)
	if !ok {
		return StatementResult{}, true, minisqlErrors.ErrNoSuchTable{Name: innerScan.TableName}
	}

	combinedCols := buildCombinedColumns(outerTable.Columns, outerScan.TableAlias, innerTable.Columns, innerScan.TableAlias)

	fieldIndexes, resultColumns, ok := combinedRowViewProjectionPlan(combinedCols, requestedFields)
	if !ok {
		return StatementResult{}, false, nil
	}

	hashTables, err := buildHashBuckets(ctx, plan, t.provider)
	if err != nil {
		return StatementResult{}, true, err
	}
	bucket, hasBucket := hashTables[0]
	if !hasBucket || bucket.cells == nil {
		return StatementResult{}, false, nil
	}

	outerFilter := compileRowViewFilterForColumns(outerTable.Columns, outerTable.pager, outerScan.Filters)

	var remaining, offset int64
	hasLimit := stmt.Limit.Valid
	hasOffset := stmt.Offset.Valid
	if hasLimit {
		remaining = stmt.Limit.Value.(int64)
	}
	if hasOffset {
		offset = stmt.Offset.Value.(int64)
	}

	newRowViewIter, err := hashJoinProbeRowViewIteratorFactory(
		ctx, outerTable, innerTable, join, bucket, combinedCols,
		outerFilter, remaining, offset, hasLimit, hasOffset,
		join.Type == Left,
	)
	if err != nil {
		return StatementResult{}, true, err
	}

	result := StatementResult{Columns: resultColumns}
	result.RowViews = newRowViewIter()
	result.RowViewPager = outerTable.pager
	result.RowViewFieldIndexes = fieldIndexes
	result.Rows = lazyRowViewMaterializingIterator(outerTable.pager, newRowViewIter, fieldIndexes, resultColumns)
	return result, true, nil
}

// selectINLJDirectRowView is a fast path for SELECT queries whose only join is a
// single INNER or LEFT index nested-loop join (INLJ) with a unique inner index,
// a sequential outer scan, RowView-compatible outer filters, and no extra join
// conditions.  It bypasses row materialisation by streaming CombinedRowViews
// that route column reads directly to outer page bytes and copied inner cell bytes.
//
// For INNER JOIN, unmatched outer rows are skipped without allocating a Row.
// For LEFT JOIN, unmatched outer rows emit a NULL-inner CombinedRowView.
func (t *Table) selectINLJDirectRowView(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if len(plan.Joins) != 1 {
		return StatementResult{}, false, nil
	}
	join := plan.Joins[0]
	if join.Type != Inner && join.Type != Left {
		return StatementResult{}, false, nil
	}
	if join.Algorithm != JoinAlgorithmNestedLoop {
		return StatementResult{}, false, nil
	}
	// Bail out for multi-column join keys or extra ON conditions beyond the
	// equi-join pairs (those require per-row evaluation not implemented here).
	if len(join.JoinColumnPairs) != 1 || len(join.Conditions) > len(join.JoinColumnPairs) {
		return StatementResult{}, false, nil
	}
	if stmt.IsSelectGroupBy() || stmt.IsSelectAggregate() || stmt.IsSelectCountAll() || stmt.Distinct || plan.SortInMemory {
		return StatementResult{}, false, nil
	}
	if len(plan.Scans) == 0 || plan.Scans[0].Type != ScanTypeSequential {
		return StatementResult{}, false, nil
	}

	outerScan := plan.Scans[0]
	outerTable, ok := t.provider.GetTable(ctx, outerScan.TableName)
	if !ok {
		return StatementResult{}, true, minisqlErrors.ErrNoSuchTable{Name: outerScan.TableName}
	}
	if outerTable.virtualRows != nil || outerTable.parallelScan {
		return StatementResult{}, false, nil
	}
	if !rowViewFilterSupports(outerTable.Columns, outerScan.Filters) {
		return StatementResult{}, false, nil
	}

	innerScan := plan.Scans[join.RightScanIndex]
	if innerScan.Type != ScanTypeIndexPoint {
		return StatementResult{}, false, nil
	}
	// Only handle unique inner index (0 or 1 inner match per outer row).
	if innerScan.IndexName == "" {
		return StatementResult{}, false, nil
	}

	innerTable, ok := t.provider.GetTable(ctx, innerScan.TableName)
	if !ok {
		return StatementResult{}, true, minisqlErrors.ErrNoSuchTable{Name: innerScan.TableName}
	}
	if !innerTable.isUniquePointIndex(innerScan.IndexName) {
		return StatementResult{}, false, nil
	}
	// Skip when inner scan has post-lookup filters — would require applying them to
	// the inner RowView, which adds complexity not yet implemented here.
	if len(innerScan.Filters) > 0 {
		return StatementResult{}, false, nil
	}

	innerIdx, ok := innerTable.IndexByName(innerScan.IndexName)
	if !ok {
		return StatementResult{}, false, nil
	}

	combinedCols := buildCombinedColumns(outerTable.Columns, outerScan.TableAlias, innerTable.Columns, innerScan.TableAlias)

	fieldIndexes, resultColumns, ok := combinedRowViewProjectionPlan(combinedCols, requestedFields)
	if !ok {
		return StatementResult{}, false, nil
	}

	outerFilter := compileRowViewFilterForColumns(outerTable.Columns, outerTable.pager, outerScan.Filters)

	var remaining, offset int64
	hasLimit := stmt.Limit.Valid
	hasOffset := stmt.Offset.Valid
	if hasLimit {
		remaining = stmt.Limit.Value.(int64)
	}
	if hasOffset {
		offset = stmt.Offset.Value.(int64)
	}

	newRowViewIter, err := inljProbeRowViewIteratorFactory(
		ctx, outerTable, innerTable, innerIdx, join, combinedCols,
		outerFilter, remaining, offset, hasLimit, hasOffset,
		join.Type == Left,
	)
	if err != nil {
		return StatementResult{}, true, err
	}

	result := StatementResult{Columns: resultColumns}
	result.RowViews = newRowViewIter()
	result.RowViewPager = outerTable.pager
	result.RowViewFieldIndexes = fieldIndexes
	result.Rows = lazyRowViewMaterializingIterator(outerTable.pager, newRowViewIter, fieldIndexes, resultColumns)
	return result, true, nil
}

func columnByFieldName(columns []Column, name string) (Column, int) {
	for i, col := range columns {
		if col.Name == name {
			return col, i
		}
	}
	return Column{}, -1
}

func projectCoveringIndexKey(key any, indexColumns []Column, rowID RowID, projectionIndexes []int, columns []Column) (Row, error) {
	if ck, ok := key.(CompositeKey); ok {
		values := make([]OptionalValue, len(projectionIndexes))
		for i, idx := range projectionIndexes {
			if idx < 0 || idx >= len(ck.Values) {
				return Row{}, fmt.Errorf("covering index projection column %d out of range", idx)
			}
			values[i] = OptionalValue{Value: ck.Values[idx], Valid: true}
		}
		projected := NewRowWithValues(columns, values)
		projected.Key = rowID
		return projected, nil
	}

	if len(indexColumns) != 1 {
		return Row{}, fmt.Errorf("single-column index key has %d index columns", len(indexColumns))
	}
	if len(projectionIndexes) != 1 || projectionIndexes[0] != 0 {
		return Row{}, fmt.Errorf("single-column index projection indexes %v", projectionIndexes)
	}
	projected := NewRowWithValues(columns, []OptionalValue{{Value: key, Valid: true}})
	projected.Key = rowID
	return projected, nil
}

func (t *Table) sequentialRowViewIteratorFactory(
	ctx context.Context,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) (func() RowViewIterator, error) {
	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return nil, err
	}
	page, err := t.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return nil, fmt.Errorf("row view sequential scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	return func() RowViewIterator {
		iterCursor := cursor
		iterPage := page
		iterRemaining := remaining
		iterOffset := offset
		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			for !iterCursor.EndOfTable {
				if err := iterCtx.Err(); err != nil {
					return RowView{}, err
				}
				if iterPage.Index != iterCursor.PageIdx {
					var err error
					iterPage, err = t.pager.ReadPage(iterCtx, iterCursor.PageIdx)
					if err != nil {
						return RowView{}, fmt.Errorf("row view sequential scan: %w", err)
					}
				}

				cell := iterPage.LeafNode.Cells[iterCursor.CellIdx]
				switch {
				case iterCursor.CellIdx < iterPage.LeafNode.Header.Cells-1:
					iterCursor.CellIdx += 1
				case iterPage.LeafNode.Header.NextLeaf == 0:
					iterCursor.EndOfTable = true
				default:
					iterCursor.PageIdx = iterPage.LeafNode.Header.NextLeaf
					iterCursor.CellIdx = 0
				}

				view := NewRowView(t.Columns, cell)
				if tableFilter != nil {
					ok, err := tableFilter(iterCtx, view)
					if err != nil {
						return RowView{}, err
					}
					if !ok {
						continue
					}
				}
				if hasOffset && iterOffset > 0 {
					iterOffset -= 1
					continue
				}
				if hasLimit {
					if iterRemaining == 0 {
						return RowView{}, ErrNoMoreRows
					}
					iterRemaining -= 1
				}
				return view, nil
			}
			return RowView{}, ErrNoMoreRows
		})
	}, nil
}

func (t *Table) indexRowViewIteratorFactory(
	ctx context.Context,
	plan QueryPlan,
	scan Scan,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) (func() RowViewIterator, error) {
	if scan.Type == ScanTypeIndexPoint {
		return t.indexPointRowViewIteratorFactory(scan, tableFilter, remaining, offset, hasLimit, hasOffset)
	}

	canApplyScanLimit := tableFilter == nil
	return func() RowViewIterator {
		iterRemaining := remaining
		iterOffset := offset
		nextRowID := t.indexRangeRowIDIterator(ctx, plan, scan, canApplyScanLimit)
		return newRowViewIteratorWithClose(func(iterCtx context.Context) (RowView, error) {
			for {
				if err := iterCtx.Err(); err != nil {
					return RowView{}, err
				}
				rowID, err := nextRowID.Next(iterCtx)
				if errors.Is(err, ErrNoMoreRows) {
					return RowView{}, ErrNoMoreRows
				}
				if err != nil {
					return RowView{}, err
				}

				view, err := t.rowViewByRowID(iterCtx, rowID)
				if err != nil {
					return RowView{}, err
				}
				if tableFilter != nil {
					ok, err := tableFilter(iterCtx, view)
					if err != nil {
						return RowView{}, err
					}
					if !ok {
						continue
					}
				}
				if hasOffset && iterOffset > 0 {
					iterOffset -= 1
					continue
				}
				if hasLimit {
					if iterRemaining == 0 {
						return RowView{}, ErrNoMoreRows
					}
					iterRemaining -= 1
				}
				return view, nil
			}
		}, nextRowID.Close)
	}, nil
}

type rowIDStreamItem struct {
	rowID RowID
	err   error
}

type rowIDStreamIterator struct {
	ch     <-chan rowIDStreamItem
	cancel context.CancelFunc
}

// Next returns the next streamed row ID, cancelling the producer when ctx is done.
func (i rowIDStreamIterator) Next(ctx context.Context) (RowID, error) {
	select {
	case <-ctx.Done():
		i.cancel()
		return 0, ctx.Err()
	case item, ok := <-i.ch:
		if !ok {
			return 0, ErrNoMoreRows
		}
		if item.err != nil {
			return 0, item.err
		}
		return item.rowID, nil
	}
}

// Close cancels the producer backing this row-id stream.
func (i rowIDStreamIterator) Close() error {
	i.cancel()
	return nil
}

func (t *Table) indexRangeRowIDIterator(ctx context.Context, plan QueryPlan, scan Scan, canApplyScanLimit bool) rowIDStreamIterator {
	iterCtx, cancel := context.WithCancel(ctx)
	ch := make(chan rowIDStreamItem, 16)

	send := func(item rowIDStreamItem) error {
		select {
		case <-iterCtx.Done():
			return errLimitReached
		case ch <- item:
			return nil
		}
	}

	go func() {
		defer close(ch)
		err := t.scanIndexRangeRowIDs(iterCtx, plan, scan, canApplyScanLimit, func(rowID RowID) error {
			return send(rowIDStreamItem{rowID: rowID})
		})
		if err != nil && !errors.Is(err, errLimitReached) && !errors.Is(err, context.Canceled) {
			_ = send(rowIDStreamItem{err: err})
		}
	}()

	return rowIDStreamIterator{ch: ch, cancel: cancel}
}

func (t *Table) indexSetRowViewIteratorFactory(
	ctx context.Context,
	scan Scan,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) (func() RowViewIterator, error) {
	rowIDs, err := t.collectIndexSetScanRowIDs(ctx, scan)
	if err != nil {
		return nil, err
	}

	return t.rowIDRowViewIteratorFactory(rowIDs, tableFilter, remaining, offset, hasLimit, hasOffset), nil
}

func (t *Table) rowIDRowViewIteratorFactory(
	rowIDs []RowID,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) func() RowViewIterator {
	return func() RowViewIterator {
		idx := 0
		iterRemaining := remaining
		iterOffset := offset
		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			for idx < len(rowIDs) {
				if err := iterCtx.Err(); err != nil {
					return RowView{}, err
				}
				rowID := rowIDs[idx]
				idx += 1

				view, err := t.rowViewByRowID(iterCtx, rowID)
				if err != nil {
					return RowView{}, err
				}
				if tableFilter != nil {
					ok, err := tableFilter(iterCtx, view)
					if err != nil {
						return RowView{}, err
					}
					if !ok {
						continue
					}
				}
				if hasOffset && iterOffset > 0 {
					iterOffset -= 1
					continue
				}
				if hasLimit {
					if iterRemaining == 0 {
						return RowView{}, ErrNoMoreRows
					}
					iterRemaining -= 1
				}
				return view, nil
			}
			return RowView{}, ErrNoMoreRows
		})
	}
}

func (t *Table) fullTextRowViewIteratorFactory(
	ctx context.Context,
	scan Scan,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) (func() RowViewIterator, bool, error) {
	secondaryIndex, query, queryTokens, err := t.fullTextScanState(scan)
	if err != nil {
		return nil, false, err
	}
	if len(queryTokens) == 0 {
		return func() RowViewIterator {
			return NewRowViewIterator(func(context.Context) (RowView, error) {
				return RowView{}, ErrNoMoreRows
			})
		}, true, nil
	}
	if len(queryTokens) != 1 || len(query.Phrases) > 0 {
		rowIDs, err := t.collectFullTextScanRowIDs(ctx, secondaryIndex, scan, query, queryTokens)
		if err != nil {
			return nil, false, err
		}
		return t.rowIDRowViewIteratorFactory(rowIDs, tableFilter, remaining, offset, hasLimit, hasOffset), true, nil
	}

	term := queryTokens[0]
	return func() RowViewIterator {
		iterRemaining := remaining
		iterOffset := offset
		var (
			iter     invertedPostingIterator
			rowIDs   []RowID
			rowIdx   int
			lastRow  RowID
			haveLast bool
			done     bool
		)
		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			if iter == nil {
				var err error
				iter, err = secondaryIndex.InvertedIndex.Lookup(iterCtx, term)
				if err != nil {
					return RowView{}, fmt.Errorf("full-text lookup failed: %w", err)
				}
			}

			for {
				if err := iterCtx.Err(); err != nil {
					return RowView{}, err
				}
				for rowIdx < len(rowIDs) {
					rowID := rowIDs[rowIdx]
					rowIdx += 1
					if haveLast && rowID == lastRow {
						continue
					}
					haveLast = true
					lastRow = rowID

					view, err := t.rowViewByRowID(iterCtx, rowID)
					if err != nil {
						return RowView{}, err
					}
					if tableFilter != nil {
						ok, err := tableFilter(iterCtx, view)
						if err != nil {
							return RowView{}, err
						}
						if !ok {
							continue
						}
					}
					if hasOffset && iterOffset > 0 {
						iterOffset -= 1
						continue
					}
					if hasLimit {
						if iterRemaining == 0 {
							return RowView{}, ErrNoMoreRows
						}
						iterRemaining -= 1
					}
					return view, nil
				}
				if done {
					return RowView{}, ErrNoMoreRows
				}

				block, ok, err := iter.NextBlock(iterCtx)
				if err != nil {
					return RowView{}, fmt.Errorf("full-text lookup failed: %w", err)
				}
				if !ok {
					done = true
					continue
				}

				rowIDs = rowIDs[:0]
				mode, err := forEachInvertedPostingRowID(block.Payload, func(rowID RowID) error {
					rowIDs = append(rowIDs, rowID)
					return nil
				})
				if err != nil {
					return RowView{}, err
				}
				if mode != invertedPostingModePositions {
					return RowView{}, fmt.Errorf("full-text index %s uses posting mode %d", scan.IndexName, mode)
				}
				rowIdx = 0
			}
		})
	}, true, nil
}

func (t *Table) invertedRowViewIteratorFactory(
	ctx context.Context,
	scan Scan,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) (func() RowViewIterator, error) {
	secondaryIndex, ok := t.SecondaryIndexes[scan.IndexName]
	if !ok || secondaryIndex.Method != IndexMethodInverted || secondaryIndex.InvertedIndex == nil {
		return nil, fmt.Errorf("no index found for inverted scan: %s", scan.IndexName)
	}
	if len(scan.IndexKeys) == 0 {
		return func() RowViewIterator {
			return NewRowViewIterator(func(context.Context) (RowView, error) {
				return RowView{}, ErrNoMoreRows
			})
		}, nil
	}
	if len(scan.IndexKeys) == 1 {
		term, ok := scan.IndexKeys[0].(string)
		if !ok {
			return func() RowViewIterator {
				return NewRowViewIterator(func(context.Context) (RowView, error) {
					return RowView{}, ErrNoMoreRows
				})
			}, nil
		}
		return t.singleTermInvertedRowViewIteratorFactory(secondaryIndex, scan, term, tableFilter, remaining, offset, hasLimit, hasOffset), nil
	}

	rowIDs, err := t.collectInvertedScanRowIDs(ctx, scan)
	if err != nil {
		return nil, err
	}
	return t.rowIDRowViewIteratorFactory(rowIDs, tableFilter, remaining, offset, hasLimit, hasOffset), nil
}

func (t *Table) singleTermInvertedRowViewIteratorFactory(
	secondaryIndex SecondaryIndex,
	scan Scan,
	term string,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) func() RowViewIterator {
	return func() RowViewIterator {
		iterRemaining := remaining
		iterOffset := offset
		var (
			iter   invertedPostingIterator
			rowIDs []RowID
			rowIdx int
			done   bool
		)
		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			if iter == nil {
				var err error
				iter, err = secondaryIndex.InvertedIndex.Lookup(iterCtx, term)
				if err != nil {
					return RowView{}, fmt.Errorf("inverted lookup failed: %w", err)
				}
			}

			for {
				if err := iterCtx.Err(); err != nil {
					return RowView{}, err
				}
				for rowIdx < len(rowIDs) {
					rowID := rowIDs[rowIdx]
					rowIdx += 1

					view, err := t.rowViewByRowID(iterCtx, rowID)
					if err != nil {
						return RowView{}, err
					}
					if tableFilter != nil {
						ok, err := tableFilter(iterCtx, view)
						if err != nil {
							return RowView{}, err
						}
						if !ok {
							continue
						}
					}
					if hasOffset && iterOffset > 0 {
						iterOffset -= 1
						continue
					}
					if hasLimit {
						if iterRemaining == 0 {
							return RowView{}, ErrNoMoreRows
						}
						iterRemaining -= 1
					}
					return view, nil
				}
				if done {
					return RowView{}, ErrNoMoreRows
				}

				block, ok, err := iter.NextBlock(iterCtx)
				if err != nil {
					return RowView{}, fmt.Errorf("inverted lookup failed: %w", err)
				}
				if !ok {
					done = true
					continue
				}

				rowIDs = rowIDs[:0]
				mode, err := forEachInvertedPostingRowID(block.Payload, func(rowID RowID) error {
					rowIDs = append(rowIDs, rowID)
					return nil
				})
				if err != nil {
					return RowView{}, err
				}
				if mode != invertedPostingModeRowIDs {
					return RowView{}, fmt.Errorf("inverted index %s uses posting mode %d", scan.IndexName, mode)
				}
				rowIdx = 0
			}
		})
	}
}

type pointRowIDIteratorIndex interface {
	PointRowIDIterator(context.Context, any) (rowIDNextFunc, error)
}

type uniquePointRowIDIndex interface {
	PointUniqueRowID(context.Context, any) (RowID, error)
}

func (t *Table) uniqueIndexPointRowViewIteratorFactory(
	scan Scan,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) (func() RowViewIterator, error) {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return nil, fmt.Errorf("no index found for row view scan: %s", scan.IndexName)
	}
	pointIdx, ok := idx.(uniquePointRowIDIndex)
	if !ok {
		return nil, fmt.Errorf("index %s does not support unique row ID lookup", scan.IndexName)
	}

	return func() RowViewIterator {
		keyIdx := 0
		iterRemaining := remaining
		iterOffset := offset
		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			for keyIdx < len(scan.IndexKeys) {
				if err := iterCtx.Err(); err != nil {
					return RowView{}, err
				}
				rowID, err := pointIdx.PointUniqueRowID(iterCtx, scan.IndexKeys[keyIdx])
				keyIdx += 1
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					return RowView{}, fmt.Errorf("index lookup failed: %w", err)
				}

				view, err := t.rowViewByRowID(iterCtx, rowID)
				if err != nil {
					return RowView{}, err
				}
				if tableFilter != nil {
					ok, err := tableFilter(iterCtx, view)
					if err != nil {
						return RowView{}, err
					}
					if !ok {
						continue
					}
				}
				if hasOffset && iterOffset > 0 {
					iterOffset -= 1
					continue
				}
				if hasLimit {
					if iterRemaining == 0 {
						return RowView{}, ErrNoMoreRows
					}
					iterRemaining -= 1
				}
				return view, nil
			}
			return RowView{}, ErrNoMoreRows
		})
	}, nil
}

func (t *Table) indexPointRowViewIteratorFactory(
	scan Scan,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) (func() RowViewIterator, error) {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return nil, fmt.Errorf("no index found for row view scan: %s", scan.IndexName)
	}
	pointIdx, ok := idx.(pointRowIDIteratorIndex)
	if !ok {
		return nil, fmt.Errorf("index %s does not support row ID iteration", scan.IndexName)
	}

	return func() RowViewIterator {
		keyIdx := 0
		var nextRowID rowIDNextFunc
		iterRemaining := remaining
		iterOffset := offset
		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			for {
				if err := iterCtx.Err(); err != nil {
					return RowView{}, err
				}
				if nextRowID == nil {
					if keyIdx >= len(scan.IndexKeys) {
						return RowView{}, ErrNoMoreRows
					}
					var err error
					nextRowID, err = pointIdx.PointRowIDIterator(iterCtx, scan.IndexKeys[keyIdx])
					keyIdx += 1
					if errors.Is(err, ErrNotFound) {
						nextRowID = nil
						continue
					}
					if err != nil {
						return RowView{}, fmt.Errorf("index lookup failed: %w", err)
					}
				}

				rowID, err := nextRowID(iterCtx)
				if errors.Is(err, ErrNoMoreRows) {
					nextRowID = nil
					continue
				}
				if err != nil {
					return RowView{}, err
				}

				view, err := t.rowViewByRowID(iterCtx, rowID)
				if err != nil {
					return RowView{}, err
				}
				if tableFilter != nil {
					ok, err := tableFilter(iterCtx, view)
					if err != nil {
						return RowView{}, err
					}
					if !ok {
						continue
					}
				}
				if hasOffset && iterOffset > 0 {
					iterOffset -= 1
					continue
				}
				if hasLimit {
					if iterRemaining == 0 {
						return RowView{}, ErrNoMoreRows
					}
					iterRemaining -= 1
				}
				return view, nil
			}
		})
	}, nil
}

func (t *Table) isUniquePointIndex(name string) bool {
	if t.HasPrimaryKey() && t.PrimaryKey.Name == name {
		return true
	}
	_, ok := t.UniqueIndexes[name]
	return ok
}

func (t *Table) scanIndexRangeRowIDs(ctx context.Context, plan QueryPlan, scan Scan, canApplyScanLimit bool, emit func(RowID) error) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for row view scan: %s", scan.IndexName)
	}
	var emitted int64
	emitRowID := func(rowID RowID) error {
		if err := emit(rowID); err != nil {
			return err
		}
		if canApplyScanLimit && scan.ScanLimit > 0 {
			emitted += 1
			if emitted >= scan.ScanLimit {
				return errLimitReached
			}
		}
		return ctx.Err()
	}
	switch scan.Type {
	case ScanTypeIndexAll:
		if err := idx.ScanAll(ctx, plan.SortReverse, func(_ any, rowID RowID) error {
			return emitRowID(rowID)
		}); err != nil && !errors.Is(err, errLimitReached) {
			return err
		}
	case ScanTypeIndexRange:
		if err := idx.ScanRange(ctx, scan.RangeCondition, plan.SortReverse, func(_ any, rowID RowID) error {
			return emitRowID(rowID)
		}); err != nil && !errors.Is(err, errLimitReached) {
			return err
		}
	case ScanTypeIndexPoint:
		for _, indexValue := range scan.IndexKeys {
			err := idx.VisitRowIDs(ctx, indexValue, func(rowID RowID) error {
				return emitRowID(rowID)
			})
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if errors.Is(err, errLimitReached) {
				break
			}
			if err != nil {
				return fmt.Errorf("index lookup failed: %w", err)
			}
			if canApplyScanLimit && scan.ScanLimit > 0 && emitted >= scan.ScanLimit {
				break
			}
		}
	default:
		return fmt.Errorf("unsupported row view index scan type: %s", scan.Type)
	}
	return nil
}

func (t *Table) rowViewByRowID(ctx context.Context, rowID RowID) (RowView, error) {
	rootPage, err := t.pager.ReadPage(ctx, t.GetRootPageIdx())
	if err != nil {
		return RowView{}, fmt.Errorf("find row failed: %w", err)
	}
	return t.rowViewByRowIDFromPage(ctx, rootPage, rowID)
}

func (t *Table) rowByRowID(ctx context.Context, rowID RowID, selectedFields ...Field) (Row, error) {
	view, err := t.rowViewByRowID(ctx, rowID)
	if err != nil {
		return Row{}, err
	}
	row, err := view.MaterializeWithOverflow(ctx, t.pager, selectedColumnsMask(t.Columns, selectedFields))
	if err != nil {
		return Row{}, fmt.Errorf("materialize row: %w", err)
	}
	return row, nil
}

func (t *Table) rowViewByRowIDFromPage(ctx context.Context, page *Page, rowID RowID) (RowView, error) {
	if page.LeafNode != nil {
		cellIdx := seekLeafCellIndex(page.LeafNode, rowID)
		if cellIdx >= page.LeafNode.Header.Cells || page.LeafNode.Cells[cellIdx].Key != rowID {
			return RowView{}, fmt.Errorf("row id %d not found", rowID)
		}
		return NewRowView(t.Columns, page.LeafNode.Cells[cellIdx]), nil
	}
	if page.InternalNode == nil {
		return RowView{}, errors.New("root page type")
	}

	childIdx := page.InternalNode.IndexOfChild(rowID)
	childPageIdx, err := page.InternalNode.Child(childIdx)
	if err != nil {
		return RowView{}, err
	}
	childPage, err := t.pager.ReadPage(ctx, childPageIdx)
	if err != nil {
		return RowView{}, fmt.Errorf("internal node seek: %w", err)
	}
	return t.rowViewByRowIDFromPage(ctx, childPage, rowID)
}

func seekLeafCellIndex(node *LeafNode, rowID RowID) uint32 {
	var (
		minIdx uint32
		maxIdx = node.Header.Cells
	)
	for i := maxIdx; i != minIdx; {
		index := (minIdx + i) / 2
		keyIdx := node.Cells[index].Key
		if rowID == keyIdx {
			return index
		}
		if rowID < keyIdx {
			i = index
		} else {
			minIdx = index + 1
		}
	}
	return minIdx
}

func rowViewProjectionPlan(columns []Column, fields []Field, allowedAliases ...string) ([]int, []Column, bool) {
	indexes := make([]int, len(fields))
	resultColumns := make([]Column, len(fields))
	for i, field := range fields {
		if field.Expr != nil {
			return nil, nil, false
		}
		if field.AliasPrefix != "" && !rowViewAliasAllowed(field.AliasPrefix, allowedAliases) {
			return nil, nil, false
		}
		idx := -1
		for j, col := range columns {
			if col.Name == field.Name {
				idx = j
				resultColumns[i] = col
				resultColumns[i].Name = field.OutputName()
				break
			}
		}
		if idx < 0 {
			return nil, nil, false
		}
		indexes[i] = idx
	}
	return indexes, resultColumns, true
}

func rowViewAliasAllowed(alias string, allowedAliases []string) bool {
	for _, allowed := range allowedAliases {
		if allowed != "" && alias == allowed {
			return true
		}
	}
	return false
}

// combinedRowViewProjectionPlan builds a field-index mapping for projecting from
// a combined join RowView whose column names carry alias prefixes (e.g. "a.id").
// A field with AliasPrefix="a", Name="id" matches the combined column "a.id".
// A field with AliasPrefix="", Name="id" matches a combined column named "id"
// (outer table with no alias).
func combinedRowViewProjectionPlan(combinedCols []Column, fields []Field) ([]int, []Column, bool) {
	indexes := make([]int, len(fields))
	resultColumns := make([]Column, len(fields))
	for i, field := range fields {
		if field.Expr != nil {
			return nil, nil, false
		}
		qualName := field.Name
		if field.AliasPrefix != "" {
			qualName = field.AliasPrefix + "." + field.Name
		}
		idx := -1
		for j, col := range combinedCols {
			if col.Name == qualName {
				idx = j
				resultColumns[i] = col
				resultColumns[i].Name = field.OutputName()
				break
			}
		}
		if idx < 0 {
			return nil, nil, false
		}
		indexes[i] = idx
	}
	return indexes, resultColumns, true
}

func projectRowView(ctx context.Context, pager TxPager, view RowView, fieldIndexes []int, columns []Column) (Row, error) {
	values := make([]OptionalValue, len(fieldIndexes))
	for i, idx := range fieldIndexes {
		value, err := view.ValueAtWithOverflow(ctx, pager, idx)
		if err != nil {
			return Row{}, err
		}
		values[i] = value
	}
	row := NewRowWithValues(columns, values)
	row.Key = view.Key()
	return row, nil
}

func lazyRowViewMaterializingIterator(pager TxPager, viewFactory func() RowViewIterator, fieldIndexes []int, columns []Column) Iterator {
	var (
		views  RowViewIterator
		opened bool
	)

	ensureOpen := func() {
		if opened {
			return
		}
		views = viewFactory()
		opened = true
	}

	return newIteratorWithClose(func(iterCtx context.Context) (Row, error) {
		ensureOpen()
		if !views.Next(iterCtx) {
			if err := views.Err(); err != nil {
				return Row{}, err
			}
			return Row{}, ErrNoMoreRows
		}
		view := views.RowView()
		return projectRowView(iterCtx, pager, view, fieldIndexes, columns)
	}, func() error {
		if !opened {
			return nil
		}
		return views.Close()
	})
}

// appendDistinctKeyFromView encodes the projected columns of view into buf for
// DISTINCT dedup.  Unlike appendHashKeyFromView, NULL values are included in
// the key (NULL == NULL for DISTINCT purposes).  Uses typed RowView accessors
// so no []OptionalValue is allocated; text bytes are copied directly into buf.
func appendDistinctKeyFromView(
	ctx context.Context,
	pager TxPager,
	buf []byte,
	view RowView,
	fieldIndexes []int,
	columns []Column,
) ([]byte, error) {
	for i, colIdx := range fieldIndexes {
		if i > 0 {
			buf = append(buf, '\x1f')
		}
		isNull, err := view.IsNull(colIdx)
		if err != nil {
			return nil, err
		}
		if isNull {
			buf = append(buf, "null"...)
			continue
		}
		col := columns[colIdx]
		switch col.Kind {
		case Int4:
			v, _, err := view.Int64At(colIdx)
			if err != nil {
				return nil, err
			}
			buf = append(buf, "i32:"...)
			buf = strconv.AppendInt(buf, v, 10)
		case Int8:
			v, _, err := view.Int64At(colIdx)
			if err != nil {
				return nil, err
			}
			buf = append(buf, "i64:"...)
			buf = strconv.AppendInt(buf, v, 10)
		case Timestamp:
			v, _, err := view.Int64At(colIdx)
			if err != nil {
				return nil, err
			}
			buf = append(buf, "ts:"...)
			buf = strconv.AppendInt(buf, v, 10)
		case Real:
			v, _, err := view.Float64At(colIdx)
			if err != nil {
				return nil, err
			}
			buf = append(buf, "f32:"...)
			buf = strconv.AppendFloat(buf, v, 'g', -1, 32)
		case Double:
			v, _, err := view.Float64At(colIdx)
			if err != nil {
				return nil, err
			}
			buf = append(buf, "f64:"...)
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case Boolean:
			v, _, err := view.BoolAt(colIdx)
			if err != nil {
				return nil, err
			}
			buf = append(buf, "b:"...)
			if v {
				buf = append(buf, "true"...)
			} else {
				buf = append(buf, "false"...)
			}
		case Varchar, Text, JSON:
			var tp TextPointer
			if pager != nil {
				tp, err = view.TextAtWithOverflow(ctx, pager, colIdx)
			} else {
				tp, err = view.TextAt(colIdx)
			}
			if err != nil {
				return nil, err
			}
			buf = append(buf, 't')
			buf = strconv.AppendInt(buf, int64(tp.Length), 10)
			buf = append(buf, ':')
			buf = append(buf, tp.Data...) // copy inline bytes; no string alloc
		case UUID:
			v, _, err := view.UUIDAt(colIdx)
			if err != nil {
				return nil, err
			}
			buf = append(buf, v[:]...)
		default:
			val, err := view.ValueAt(colIdx)
			if err != nil {
				return nil, err
			}
			if !val.Valid {
				buf = append(buf, "null"...)
			} else {
				buf = fmt.Appendf(buf, "?:%v", val.Value)
			}
		}
	}
	return buf, nil
}

func distinctSeenCapacityFromEstimate(n int64) int {
	if n <= 0 {
		return 0
	}
	maxInt := int64(^uint(0) >> 1)
	if n > maxInt {
		return int(maxInt)
	}
	return int(n)
}

// newDistinctRowViewIteratorFactory returns a factory that creates a
// RowViewIterator wrapping innerFactory with DISTINCT dedup applied via a
// hash-set keyed by the projected column values.  LIMIT/OFFSET are applied
// after dedup.  Each factory call produces an independent iterator with its
// own seen-set so the factory may be called multiple times safely.
func newDistinctRowViewIteratorFactory(
	ctx context.Context,
	pager TxPager,
	innerFactory func() RowViewIterator,
	fieldIndexes []int,
	columns []Column,
	seenCapacity int,
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) func() RowViewIterator {
	return func() RowViewIterator {
		inner := innerFactory()
		seen := make(map[string]struct{}, seenCapacity)
		buf := make([]byte, 0, 64)
		rem := remaining
		off := offset
		done := false

		return newRowViewIteratorWithClose(func(iterCtx context.Context) (RowView, error) {
			if done || (hasLimit && rem == 0) {
				return RowView{}, ErrNoMoreRows
			}
			for {
				if !inner.Next(iterCtx) {
					done = true
					if err := inner.Err(); err != nil {
						return RowView{}, err
					}
					return RowView{}, ErrNoMoreRows
				}
				view := inner.RowView()
				buf = buf[:0]
				var err error
				buf, err = appendDistinctKeyFromView(ctx, pager, buf, view, fieldIndexes, columns)
				if err != nil {
					return RowView{}, err
				}
				key := string(buf)
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				if hasOffset && off > 0 {
					off -= 1
					continue
				}
				if hasLimit {
					rem -= 1
				}
				return view, nil
			}
		}, inner.Close)
	}
}

// newDistinctAdjacentRowViewIteratorFactory is like newDistinctRowViewIteratorFactory
// but uses adjacent-compare instead of a hash set.  It is only correct when the
// underlying iterator delivers rows in ORDER BY order and every projected field
// is covered by ORDER BY — in that case equal projected rows are guaranteed to
// be adjacent, so a single prevKey []byte suffices for dedup (O(1) memory).
func newDistinctAdjacentRowViewIteratorFactory(
	ctx context.Context,
	pager TxPager,
	innerFactory func() RowViewIterator,
	fieldIndexes []int,
	columns []Column,
	remaining int64,
	offset int64,
	hasLimit bool,
	hasOffset bool,
) func() RowViewIterator {
	return func() RowViewIterator {
		inner := innerFactory()
		var prevKey []byte
		buf := make([]byte, 0, 64)
		rem := remaining
		off := offset
		done := false

		return newRowViewIteratorWithClose(func(iterCtx context.Context) (RowView, error) {
			if done || (hasLimit && rem == 0) {
				return RowView{}, ErrNoMoreRows
			}
			for {
				if !inner.Next(iterCtx) {
					done = true
					if err := inner.Err(); err != nil {
						return RowView{}, err
					}
					return RowView{}, ErrNoMoreRows
				}
				view := inner.RowView()
				buf = buf[:0]
				var err error
				buf, err = appendDistinctKeyFromView(ctx, pager, buf, view, fieldIndexes, columns)
				if err != nil {
					return RowView{}, err
				}
				if bytes.Equal(buf, prevKey) {
					continue
				}
				prevKey = append(prevKey[:0], buf...)
				if hasOffset && off > 0 {
					off--
					continue
				}
				if hasLimit {
					rem--
				}
				return view, nil
			}
		}, inner.Close)
	}
}

func (t *Table) selectStreaming(stmt Statement, scanned []Row, requestedFields []Field) (StatementResult, error) {
	result := StatementResult{
		Columns: t.selectResultColumns(stmt, requestedFields),
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

	// Precompute projection schema from the first row's column layout.  For
	// plain field references (no expression), this eliminates per-row
	// make([]Column,...) inside OnlyFields; only make([]OptionalValue,...) is
	// allocated per row.  Falls back to the full projectRow path when any
	// field has an expression or when there are no rows.
	var projectedCols []Column
	var projectedIdxs []int
	if len(scanned) > 0 {
		projectedCols, projectedIdxs = buildProjectionSchema(scanned[0].Columns, requestedFields)
	}

	idx := 0
	result.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		if hasLimit && limit == 0 {
			return Row{}, ErrNoMoreRows
		}

		for idx < len(scanned) {
			row := scanned[idx]
			idx += 1

			var p Row
			if projectedCols != nil {
				p = row.projectFast(projectedCols, projectedIdxs)
			} else {
				var err error
				p, err = projectRow(row, requestedFields)
				if err != nil {
					return Row{}, err
				}
			}
			if stmt.Distinct {
				key := p.rowDistinctKey()
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
			}
			if hasOffset && offset > 0 {
				offset -= 1
				continue
			}
			if hasLimit {
				limit -= 1
			}
			return p, nil
		}

		return Row{}, ErrNoMoreRows
	})
	return result, nil
}

// selectStreamingJoin streams INNER/LEFT/RIGHT/FULL OUTER join results without
// buffering into []Row.  The join goroutine runs concurrently; the returned
// Iterator reads rows from it on demand.  Ineligible when ORDER BY sort,
// GROUP BY, aggregate, COUNT(*), or semi/anti-semi joins are present.
// DISTINCT is handled inline via a hash-set dedup on the projected row key.
func (t *Table) selectStreamingJoin(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	selectedFields []Field,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if len(plan.Joins) == 0 ||
		plan.SortInMemory ||
		stmt.IsSelectGroupBy() ||
		stmt.IsSelectAggregate() ||
		stmt.IsSelectCountAll() {
		return StatementResult{}, false, nil
	}
	// Semi/anti-semi joins are handled by selectSemiJoinDirectRowView above.
	for _, j := range plan.Joins {
		if j.Type == Semi || j.Type == AntiSemi {
			return StatementResult{}, false, nil
		}
	}

	joinCtx, joinCancel := context.WithCancel(ctx)
	ch := make(chan Row, 128)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		if err := plan.executeNestedLoopJoin(joinCtx, t.provider, selectedFields, ch); err != nil {
			if !errors.Is(err, context.Canceled) {
				select {
				case errCh <- err:
				default:
				}
			}
		}
	}()

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

	var projectedCols []Column
	var projectedIdxs []int
	schemaComputed := false

	var seen map[string]struct{}
	if stmt.Distinct {
		seen = make(map[string]struct{})
	}

	result := StatementResult{
		Columns: t.selectResultColumns(stmt, requestedFields),
	}
	result.Rows = newIteratorWithClose(
		func(iterCtx context.Context) (Row, error) {
			if hasLimit && limit == 0 {
				return Row{}, ErrNoMoreRows
			}
			for {
				select {
				case <-iterCtx.Done():
					return Row{}, iterCtx.Err()
				case row, ok := <-ch:
					if !ok {
						// Channel closed — surface any join error.
						select {
						case err := <-errCh:
							return Row{}, err
						default:
							return Row{}, ErrNoMoreRows
						}
					}
					// Lazily compute projection schema from the first row.
					if !schemaComputed {
						projectedCols, projectedIdxs = buildProjectionSchema(row.Columns, requestedFields)
						schemaComputed = true
					}
					var p Row
					if projectedCols != nil {
						p = row.projectFast(projectedCols, projectedIdxs)
					} else {
						var err error
						p, err = projectRow(row, requestedFields)
						if err != nil {
							return Row{}, err
						}
					}
					if seen != nil {
						key := p.rowDistinctKey()
						if _, dup := seen[key]; dup {
							continue
						}
						seen[key] = struct{}{}
					}
					if hasOffset && offset > 0 {
						offset -= 1
						continue
					}
					if hasLimit {
						limit -= 1
					}
					return p, nil
				}
			}
		},
		func() error {
			joinCancel()
			drainRowCh(ch)
			return nil
		},
	)
	return result, true, nil
}

// selectJoinWithSortStreamingLimit handles JOIN + ORDER BY + LIMIT using a bounded
// min-heap of size limit+offset, keeping memory proportional to the output size
// rather than the full join cardinality.
func (t *Table) selectJoinWithSortStreamingLimit(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	selectedFields []Field,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if len(plan.Joins) == 0 ||
		!plan.SortInMemory ||
		!stmt.Limit.Valid ||
		len(plan.OrderBy) == 0 ||
		stmt.Distinct ||
		stmt.IsSelectCountAll() ||
		stmt.IsSelectGroupBy() ||
		stmt.IsSelectAggregate() {
		return StatementResult{}, false, nil
	}
	// Semi/anti-semi joins with ORDER BY fall through to the materialising path.
	for _, j := range plan.Joins {
		if j.Type == Semi || j.Type == AntiSemi {
			return StatementResult{}, false, nil
		}
	}

	limit := int(stmt.Limit.Value.(int64))
	offset := 0
	if stmt.Offset.Valid {
		offset = int(stmt.Offset.Value.(int64))
	}
	maxRows := limit + offset

	outputFields := orderByOutputFields(requestedFields)
	h := newRowHeap(plan.OrderBy, maxRows)
	err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		updated, err := addOrderByOutputFieldsToRow(row, outputFields, plan.OrderBy)
		if err != nil {
			return err
		}
		h.PushRow(updated)
		return nil
	})
	if err != nil {
		return StatementResult{}, true, err
	}

	allRows := h.ExtractSorted()
	if offset >= len(allRows) {
		allRows = []Row{}
	} else {
		end := offset + limit
		if end < len(allRows) {
			allRows = allRows[offset:end]
		} else {
			allRows = allRows[offset:]
		}
	}

	idx := 0
	result := StatementResult{
		Columns: t.selectResultColumns(stmt, requestedFields),
	}
	result.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		if idx >= len(allRows) {
			return Row{}, ErrNoMoreRows
		}
		row := allRows[idx]
		idx += 1
		return projectRow(row, requestedFields)
	})
	return result, true, nil
}

func (t *Table) selectWithSortStreamingLimit(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	selectedFields []Field,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if len(plan.Joins) > 0 ||
		!plan.SortInMemory ||
		!stmt.Limit.Valid ||
		len(plan.OrderBy) == 0 ||
		stmt.Distinct ||
		stmt.IsSelectCountAll() ||
		stmt.IsSelectGroupBy() ||
		stmt.IsSelectAggregate() {
		return StatementResult{}, false, nil
	}

	limit := int(stmt.Limit.Value.(int64))
	offset := 0
	if stmt.Offset.Valid {
		offset = int(stmt.Offset.Value.(int64))
	}
	maxRows := limit + offset

	h := newRowHeap(plan.OrderBy, maxRows)
	outputFields := orderByOutputFields(requestedFields)
	err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		updated, err := addOrderByOutputFieldsToRow(row, outputFields, plan.OrderBy)
		if err != nil {
			return err
		}
		h.PushRow(updated)
		return nil
	})
	if err != nil {
		return StatementResult{}, true, err
	}

	allRows := h.ExtractSorted()
	if offset >= len(allRows) {
		allRows = []Row{}
	} else {
		end := offset + limit
		if end < len(allRows) {
			allRows = allRows[offset:end]
		} else {
			allRows = allRows[offset:]
		}
	}

	idx := 0
	result := StatementResult{
		Columns: t.selectResultColumns(stmt, requestedFields),
	}
	result.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		if idx >= len(allRows) {
			return Row{}, ErrNoMoreRows
		}
		row := allRows[idx]
		idx += 1
		return projectRow(row, requestedFields)
	})

	return result, true, nil
}

func (t *Table) selectWithSortStreamingLimitRowView(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	selectedFields []Field,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if len(plan.Joins) > 0 ||
		len(plan.Scans) != 1 ||
		plan.Scans[0].Type != ScanTypeSequential ||
		t.virtualRows != nil ||
		!plan.SortInMemory ||
		!stmt.Limit.Valid ||
		len(plan.OrderBy) == 0 ||
		stmt.Distinct ||
		stmt.IsSelectCountAll() ||
		stmt.IsSelectGroupBy() ||
		stmt.IsSelectAggregate() {
		return StatementResult{}, false, nil
	}
	for _, field := range requestedFields {
		if field.Expr != nil {
			return StatementResult{}, false, nil
		}
	}
	for _, clause := range plan.OrderBy {
		if clause.Field.Expr != nil {
			return StatementResult{}, false, nil
		}
	}

	heapFields := orderByHeapFields(requestedFields, plan.OrderBy)
	fieldIndexes, _, ok := rowViewProjectionPlan(t.Columns, heapFields, stmt.TableName, stmt.TableAlias)
	if !ok {
		return StatementResult{}, false, nil
	}
	heapColumns := heapColumnsForFields(t.Columns, heapFields)
	_, resultColumns, ok := rowViewProjectionPlan(t.Columns, requestedFields, stmt.TableName, stmt.TableAlias)
	if !ok {
		return StatementResult{}, false, nil
	}

	scan := plan.Scans[0]
	limit := int(stmt.Limit.Value.(int64))
	offset := 0
	if stmt.Offset.Valid {
		offset = int(stmt.Offset.Value.(int64))
	}
	maxRows := limit + offset
	h := newRowHeap(plan.OrderBy, maxRows)

	err := t.scanProjectedRowViews(ctx, scan, selectedFields, fieldIndexes, heapColumns, func(row Row) error {
		h.PushRow(row)
		return nil
	})
	if err != nil {
		return StatementResult{}, true, err
	}

	allRows := h.ExtractSorted()
	if offset >= len(allRows) {
		allRows = nil
	} else {
		end := offset + limit
		if end < len(allRows) {
			allRows = allRows[offset:end]
		} else {
			allRows = allRows[offset:]
		}
	}

	idx := 0
	result := StatementResult{
		Columns: resultColumns,
	}
	result.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		if idx >= len(allRows) {
			return Row{}, ErrNoMoreRows
		}
		row := allRows[idx]
		idx += 1
		values := make([]OptionalValue, len(requestedFields))
		for i := range requestedFields {
			values[i] = row.Values[i]
		}
		projected := NewRowWithValues(resultColumns, values)
		projected.Key = row.Key
		return projected, nil
	})

	return result, true, nil
}

func (t *Table) selectWithSortRowView(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	selectedFields []Field,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if len(plan.Joins) > 0 ||
		len(plan.Scans) != 1 ||
		plan.Scans[0].Type != ScanTypeSequential ||
		t.virtualRows != nil ||
		!plan.SortInMemory ||
		stmt.Limit.Valid ||
		len(plan.OrderBy) == 0 ||
		stmt.IsSelectCountAll() ||
		stmt.IsSelectGroupBy() ||
		stmt.IsSelectAggregate() {
		return StatementResult{}, false, nil
	}
	for _, field := range requestedFields {
		if field.Expr != nil {
			return StatementResult{}, false, nil
		}
	}
	for _, clause := range plan.OrderBy {
		if clause.Field.Expr != nil {
			return StatementResult{}, false, nil
		}
	}

	sortFields := orderByHeapFields(requestedFields, plan.OrderBy)
	fieldIndexes, sortColumns, ok := rowViewProjectionPlan(t.Columns, sortFields, stmt.TableName, stmt.TableAlias)
	if !ok {
		return StatementResult{}, false, nil
	}
	_, resultColumns, ok := rowViewProjectionPlan(t.Columns, requestedFields, stmt.TableName, stmt.TableAlias)
	if !ok {
		return StatementResult{}, false, nil
	}

	// allOrderByInProjected is true when every ORDER BY column is already present
	// in the projected (SELECT) fields — no extra sort-only column was appended by
	// orderByHeapFields. In that case, extending the sort by the remaining projected
	// fields guarantees equal projected rows are adjacent → hash-free adjacent dedup.
	// When ORDER BY references columns outside the SELECT list, duplicate projected
	// rows can land non-adjacently after sorting, so we must fall back to the hash set.
	allOrderByInProjected := len(sortFields) == len(requestedFields)
	effectiveOrderBy := plan.OrderBy
	if stmt.Distinct && allOrderByInProjected {
		effectiveOrderBy = distinctExtendOrderBy(plan.OrderBy, requestedFields)
	}

	scan := plan.Scans[0]
	var (
		allRows    []Row
		accumBytes int64
		tmpRuns    []string
	)

	flushRun := func() error {
		if len(allRows) == 0 {
			return nil
		}
		if err := t.sortRows(allRows, effectiveOrderBy); err != nil {
			return err
		}
		w, err := newRunWriter()
		if err != nil {
			return err
		}
		for _, r := range allRows {
			if wErr := w.writeRow(r); wErr != nil {
				_ = w.close()
				return wErr
			}
		}
		path := w.filePath()
		if err := w.close(); err != nil {
			return err
		}
		tmpRuns = append(tmpRuns, path)
		if m := t.metrics; m != nil {
			m.sortSpillRuns.Add(1)
			m.sortSpillBytes.Add(accumBytes)
		}
		allRows = allRows[:0]
		accumBytes = 0
		return nil
	}

	err := t.scanProjectedRowViews(ctx, scan, selectedFields, fieldIndexes, sortColumns, func(row Row) error {
		accumBytes += int64(row.Size()) + 16 // 8-byte RowID + 8-byte NullBitmask
		allRows = append(allRows, row)
		if t.sortMemLimit > 0 && accumBytes >= t.sortMemLimit {
			return flushRun()
		}
		return nil
	})
	if err != nil {
		for _, p := range tmpRuns {
			_ = os.Remove(p)
		}
		return StatementResult{}, true, err
	}

	if len(tmpRuns) > 0 {
		// External merge: sort remaining in-memory rows, then N-way merge all runs.
		if err := t.sortRows(allRows, effectiveOrderBy); err != nil {
			for _, p := range tmpRuns {
				_ = os.Remove(p)
			}
			return StatementResult{}, true, err
		}
		merged, err := t.externalSortMerge(tmpRuns, allRows, sortColumns, effectiveOrderBy)
		if err != nil {
			return StatementResult{}, true, err
		}
		allRows = merged
	} else {
		if err := t.sortRows(allRows, effectiveOrderBy); err != nil {
			return StatementResult{}, true, err
		}
		if m := t.metrics; m != nil {
			m.sortsInMemory.Add(1)
		}
	}

	if stmt.Distinct {
		if allOrderByInProjected {
			allRows = deduplicateSortedRows(allRows, requestedFields)
		} else {
			allRows = deduplicateRows(allRows, requestedFields)
		}
	}

	offset := 0
	if stmt.Offset.Valid {
		offset = int(stmt.Offset.Value.(int64))
	}
	if offset >= len(allRows) {
		allRows = nil
	} else if offset > 0 {
		allRows = allRows[offset:]
	}

	nResult := len(requestedFields)
	result := StatementResult{
		Columns: resultColumns,
	}
	idx := 0
	result.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		if idx >= len(allRows) {
			return Row{}, ErrNoMoreRows
		}
		row := allRows[idx]
		idx += 1
		// When all ORDER BY columns are already in SELECT, scanProjectedRowViews
		// produces exactly nResult values — reuse the slice to avoid an alloc+copy.
		if len(row.Values) <= nResult {
			row.Columns = resultColumns
			return row, nil
		}
		// Extra sort-only columns were appended beyond nResult — sub-slice them
		// off without copying the backing array.
		projected := NewRowWithValues(resultColumns, row.Values[:nResult])
		projected.Key = row.Key
		return projected, nil
	})

	return result, true, nil
}

type compiledRowViewScanFilter struct {
	rowViewFilter func(context.Context, RowView) (bool, error)
	rowFilter     func(Row) (bool, error)
	selectedMask  []bool
}

func (t *Table) compileRowViewScanFilter(scan Scan, selectedFields []Field) compiledRowViewScanFilter {
	if rowViewFilterSupports(t.Columns, scan.Filters) {
		return compiledRowViewScanFilter{
			rowViewFilter: compileRowViewFilterForColumns(t.Columns, t.pager, scan.Filters),
		}
	}
	return compiledRowViewScanFilter{
		rowFilter:    compileScanFilter(t.Columns, scan.Filters),
		selectedMask: selectedColumnsMask(t.Columns, selectedFields),
	}
}

func (f compiledRowViewScanFilter) accept(ctx context.Context, pager TxPager, view RowView) (bool, error) {
	if f.rowViewFilter != nil {
		return f.rowViewFilter(ctx, view)
	}
	if f.rowFilter == nil {
		return true, nil
	}
	row, err := view.MaterializeWithOverflow(ctx, pager, f.selectedMask)
	if err != nil {
		return false, err
	}
	return f.rowFilter(row)
}

func (t *Table) scanProjectedRowViews(
	ctx context.Context,
	scan Scan,
	selectedFields []Field,
	fieldIndexes []int,
	columns []Column,
	consume func(Row) error,
) error {
	filter := t.compileRowViewScanFilter(scan, selectedFields)
	if t.parallelScan {
		return t.scanParallelProjectedRowViews(ctx, filter, fieldIndexes, columns, consume)
	}

	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return err
	}
	page, err := t.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return fmt.Errorf("row view scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return err
		}
		if page.Index != cursor.PageIdx {
			page, err = t.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return fmt.Errorf("row view scan: %w", err)
			}
		}
		if cursor.CellIdx > page.LeafNode.Header.Cells-1 || len(page.LeafNode.Cells) == 0 {
			return fmt.Errorf("cell index %d out of bounds, max %d", cursor.CellIdx, page.LeafNode.Header.Cells-1)
		}

		cell := page.LeafNode.Cells[cursor.CellIdx]
		advanceLeafCursor(cursor, page)
		view := NewRowView(t.Columns, cell)

		ok, err := filter.accept(ctx, t.pager, view)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		row, err := projectRowView(ctx, t.pager, view, fieldIndexes, columns)
		if err != nil {
			return err
		}
		if err := consume(row); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) scanParallelProjectedRowViews(
	ctx context.Context,
	filter compiledRowViewScanFilter,
	fieldIndexes []int,
	columns []Column,
	consume func(Row) error,
) error {
	rowViewFilter := func(filterCtx context.Context, view RowView) (bool, error) {
		return filter.accept(filterCtx, t.pager, view)
	}
	newIter, err := t.parallelSequentialRowViewIteratorFactory(ctx, rowViewFilter, 0, 0, false, false)
	if err != nil {
		return err
	}
	views := newIter()
	defer func() {
		_ = views.Close()
	}()

	for views.Next(ctx) {
		row, err := projectRowView(ctx, t.pager, views.RowView(), fieldIndexes, columns)
		if err != nil {
			return err
		}
		if err := consume(row); err != nil {
			return err
		}
	}
	return views.Err()
}

func (t *Table) selectWithSort(stmt Statement, plan QueryPlan, allRows []Row, requestedFields []Field) (StatementResult, error) {
	var err error
	allRows, err = addOrderByOutputFields(allRows, requestedFields, plan.OrderBy)
	if err != nil {
		return StatementResult{}, err
	}

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
		Columns: t.selectResultColumns(stmt, requestedFields),
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

// selectWithSortSpill is the universal ORDER BY path that streams rows through
// plan.Execute and flushes sorted runs to disk when accumBytes exceeds
// t.sortMemLimit, then N-way merges all runs — bounding peak heap memory
// regardless of result cardinality.
//
// It is invoked for any plan.SortInMemory query where sortMemLimit > 0 and
// the heap optimisation (LIMIT without DISTINCT) is not applicable.
func (t *Table) selectWithSortSpill(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	selectedFields []Field,
	requestedFields []Field,
) (StatementResult, error) {
	outputFields := orderByOutputFields(requestedFields)

	var (
		allRows      []Row
		accumBytes   int64
		tmpRuns      []string
		spillColumns []Column
	)

	cleanupRuns := func() {
		for _, p := range tmpRuns {
			_ = os.Remove(p)
		}
	}

	flushRun := func() error {
		if len(allRows) == 0 {
			return nil
		}
		if err := t.sortRows(allRows, plan.OrderBy); err != nil {
			return err
		}
		w, err := newRunWriter()
		if err != nil {
			return err
		}
		for _, r := range allRows {
			if wErr := w.writeRow(r); wErr != nil {
				_ = w.close()
				return wErr
			}
		}
		path := w.filePath()
		if err := w.close(); err != nil {
			return err
		}
		tmpRuns = append(tmpRuns, path)
		if m := t.metrics; m != nil {
			m.sortSpillRuns.Add(1)
			m.sortSpillBytes.Add(accumBytes)
		}
		allRows = allRows[:0]
		accumBytes = 0
		return nil
	}

	err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
		var err error
		row, err = addOrderByOutputFieldsToRow(row, outputFields, plan.OrderBy)
		if err != nil {
			return err
		}
		if spillColumns == nil && len(row.Columns) > 0 {
			spillColumns = row.Columns
		}
		accumBytes += int64(row.Size()) + 16
		allRows = append(allRows, row)
		if t.sortMemLimit > 0 && accumBytes >= t.sortMemLimit {
			return flushRun()
		}
		return nil
	})
	if err != nil && !errors.Is(err, errLimitReached) {
		cleanupRuns()
		return StatementResult{}, err
	}

	if len(tmpRuns) > 0 {
		if err := t.sortRows(allRows, plan.OrderBy); err != nil {
			cleanupRuns()
			return StatementResult{}, err
		}
		merged, err := t.externalSortMerge(tmpRuns, allRows, spillColumns, plan.OrderBy)
		if err != nil {
			return StatementResult{}, err
		}
		allRows = merged
	} else {
		if err := t.sortRows(allRows, plan.OrderBy); err != nil {
			return StatementResult{}, err
		}
		if m := t.metrics; m != nil {
			m.sortsInMemory.Add(1)
		}
	}

	if stmt.Distinct {
		allRows = deduplicateRows(allRows, requestedFields)
	}

	offset := 0
	if stmt.Offset.Valid {
		offset = int(stmt.Offset.Value.(int64))
	}
	hasLimit := stmt.Limit.Valid
	var limit int
	if hasLimit {
		limit = int(stmt.Limit.Value.(int64))
	}

	if offset >= len(allRows) {
		allRows = nil
	} else {
		end := offset + limit
		if hasLimit && end < len(allRows) {
			allRows = allRows[offset:end]
		} else {
			allRows = allRows[offset:]
		}
	}

	idx := 0
	result := StatementResult{
		Columns: t.selectResultColumns(stmt, requestedFields),
	}
	result.Rows = NewIterator(func(ctx context.Context) (Row, error) {
		if idx >= len(allRows) {
			return Row{}, ErrNoMoreRows
		}
		row := allRows[idx]
		idx++
		return projectRow(row, requestedFields)
	})

	return result, nil
}

func (t *Table) selectResultColumns(stmt Statement, requestedFields []Field) []Column {
	columns := make([]Column, len(requestedFields))
	for i, field := range requestedFields {
		if field.Expr != nil {
			columns[i] = Column{Name: field.OutputName()}
		} else if colIdx := stmt.ColumnIdx(field.Name); colIdx >= 0 {
			columns[i] = t.Columns[colIdx]
		}
	}
	return columns
}

func addOrderByOutputFields(rows []Row, fields []Field, orderBy []OrderBy) ([]Row, error) {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows, nil
	}

	outputFields := orderByOutputFields(fields)
	for rowIdx, row := range rows {
		updated, err := addOrderByOutputFieldsToRow(row, outputFields, orderBy)
		if err != nil {
			return nil, err
		}
		rows[rowIdx] = updated
	}

	return rows, nil
}

func orderByOutputFields(fields []Field) map[string]Field {
	outputFields := make(map[string]Field)
	for _, field := range fields {
		outputFields[field.OutputName()] = field
	}
	return outputFields
}

func orderByHeapFields(fields []Field, orderBy []OrderBy) []Field {
	result := make([]Field, 0, len(fields)+len(orderBy))
	seen := make(map[string]struct{}, len(fields)+len(orderBy))
	for _, field := range fields {
		result = append(result, field)
		seen[field.OutputName()] = struct{}{}
		if field.Name != "" {
			seen[field.Name] = struct{}{}
		}
	}
	for _, clause := range orderBy {
		field := clause.Field
		if _, ok := seen[field.OutputName()]; ok {
			continue
		}
		if _, ok := seen[field.Name]; ok {
			continue
		}
		result = append(result, field)
		seen[field.OutputName()] = struct{}{}
		if field.Name != "" {
			seen[field.Name] = struct{}{}
		}
	}
	return result
}

func heapColumnsForFields(columns []Column, fields []Field) []Column {
	heapColumns := make([]Column, len(fields))
	for i, field := range fields {
		for _, col := range columns {
			if col.Name == field.Name {
				heapColumns[i] = col
				heapColumns[i].Name = field.OutputName()
				break
			}
		}
	}
	return heapColumns
}

func addOrderByOutputFieldsToRow(row Row, outputFields map[string]Field, orderBy []OrderBy) (Row, error) {
	if len(orderBy) == 0 {
		return row, nil
	}
	updated := row
	for _, clause := range orderBy {
		if _, found := updated.GetValue(clause.Field.Name); found {
			continue
		}

		field, ok := outputFields[clause.Field.Name]
		if !ok {
			continue
		}

		var value OptionalValue
		var extraCol Column
		if field.Expr != nil {
			result, err := field.Expr.Eval(updated)
			if err != nil {
				return Row{}, fmt.Errorf("evaluating ORDER BY expression %q: %w", field.OutputName(), err)
			}
			if result != nil {
				value = OptionalValue{Value: result, Valid: true}
			}
			// Derive column Kind from the evaluated value so rows can be serialized
			// to disk during external sort without losing the sort key.
			extraCol = Column{Name: field.OutputName(), Kind: kindFromValue(value.Value)}
		} else {
			sourceCol, _ := updated.getColumnQualified(field.AliasPrefix, field.Name)
			existing, found := updated.getValueQualified(field.AliasPrefix, field.Name)
			if !found {
				continue
			}
			value = existing
			// Preserve the source column's Kind so the extra sort key round-trips
			// through external sort run files (Marshal/UnmarshalRow).
			extraCol = Column{Name: field.OutputName(), Kind: sourceCol.Kind, Size: sourceCol.Size}
		}

		updated.Columns = append(updated.Columns, extraCol)
		updated.Values = append(updated.Values, value)
	}

	return updated, nil
}

// kindFromValue infers a ColumnKind from the Go type of a value returned by
// expression evaluation. Used when adding extra ORDER BY output columns so that
// the column carries a proper Kind for Marshal/UnmarshalRow round-trips during
// external sort.
func kindFromValue(v any) ColumnKind {
	switch v.(type) {
	case bool:
		return Boolean
	case int32:
		return Int4
	case int64:
		return Int8
	case float32:
		return Real
	case float64:
		return Double
	case string, TextPointer:
		return Varchar
	case TimestampMicros:
		return Timestamp
	case UUIDValue:
		return UUID
	default:
		return 0
	}
}

func (t *Table) indexedScanRow(
	ctx context.Context,
	key any,
	rowID RowID,
	isCovering bool,
	indexColumns []Column,
	selectedMask []bool,
	nSelected int,
	tableFilter func(Row) (bool, error),
	coveringFilter func(Row) (bool, error),
) (Row, bool, error) {
	var row Row

	switch {
	case isCovering:
		row = rowFromIndexKey(key, indexColumns, rowID)
	case nSelected == 0:
		row = NewRowWithValues(t.Columns, nil)
		row.Key = rowID
	default:
		view, err := t.rowViewByRowID(ctx, rowID)
		if err != nil {
			return Row{}, false, err
		}
		row, err = view.MaterializeWithOverflow(ctx, t.pager, selectedMask)
		if err != nil {
			return Row{}, false, fmt.Errorf("fetch row failed: %w", err)
		}

		if tableFilter != nil {
			ok, err := tableFilter(row)
			if err != nil || !ok {
				return Row{}, false, err
			}
		}
	}

	if isCovering && coveringFilter != nil {
		ok, err := coveringFilter(row)
		if err != nil || !ok {
			return Row{}, false, err
		}
	}

	return row, true, nil
}

func (t *Table) rowIDScanRow(
	ctx context.Context,
	rowID RowID,
	selectedMask []bool,
	nSelected int,
	tableFilter func(Row) (bool, error),
) (Row, bool, error) {
	var row Row
	if nSelected == 0 {
		row = NewRowWithValues(t.Columns, nil)
		row.Key = rowID
	} else {
		view, err := t.rowViewByRowID(ctx, rowID)
		if err != nil {
			return Row{}, false, err
		}
		row, err = view.MaterializeWithOverflow(ctx, t.pager, selectedMask)
		if err != nil {
			return Row{}, false, fmt.Errorf("fetch row failed: %w", err)
		}
	}

	if tableFilter != nil {
		ok, err := tableFilter(row)
		if err != nil || !ok {
			return Row{}, false, err
		}
	}

	return row, true, nil
}

func (t *Table) indexScanAll(ctx context.Context, plan QueryPlan, scan Scan, selectedFields []Field, out func(Row) error) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}
	var (
		selectedMask   = selectedColumnsMask(t.Columns, selectedFields)
		tableFilter    = compileScanFilter(t.Columns, scan.Filters)
		coveringFilter = compileScanFilter(scan.IndexColumns, scan.Filters)
		nSelected      = len(selectedFields)
	)

	// Scan index in order (or reverse order)
	if err := idx.ScanAll(ctx, plan.SortReverse, func(key any, rowID RowID) error {
		row, ok, err := t.indexedScanRow(
			ctx, key, rowID, scan.CoveringIndex, scan.IndexColumns,
			selectedMask, nSelected, tableFilter, coveringFilter,
		)
		if err != nil || !ok {
			return err
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

func (t *Table) indexRangeScan(ctx context.Context, plan QueryPlan, scan Scan, selectedFields []Field, out func(Row) error) error {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}
	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	coveringFilter := compileScanFilter(scan.IndexColumns, scan.Filters)
	nSelected := len(selectedFields)

	// Scan index within range (forward or reverse)
	if err := idx.ScanRange(ctx, scan.RangeCondition, plan.SortReverse, func(key any, rowID RowID) error {
		row, ok, err := t.indexedScanRow(
			ctx, key, rowID, scan.CoveringIndex, scan.IndexColumns,
			selectedMask, nSelected, tableFilter, coveringFilter,
		)
		if err != nil || !ok {
			return err
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

	// Lookup each key value and process matching row IDs one at a time.
	// VisitRowIDs reads overflow pages lazily, so a LIMIT sentinel returned by
	// out stops iteration before all overflow pages are loaded.
	for _, indexValue := range scan.IndexKeys {
		err := idx.VisitRowIDs(ctx, indexValue, func(rowID RowID) error {
			row, ok, err := t.indexedScanRow(
				ctx, indexValue, rowID, scan.CoveringIndex, scan.IndexColumns,
				selectedMask, len(selectedFields), tableFilter, coveringFilter,
			)
			if err != nil || !ok {
				return err
			}

			if err := ctx.Err(); err != nil {
				return err
			}
			return out(row)
		})
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return fmt.Errorf("index lookup failed: %w", err)
		}
	}

	return nil
}

// indexPointExists reports whether any row matches the index point scan and
// residual filters, without materialising matched rows.
func (t *Table) indexPointExists(ctx context.Context, scan Scan, selectedFields []Field) (bool, error) {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return false, fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}
	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	coveringFilter := compileScanFilter(scan.IndexColumns, scan.Filters)
	isCovering := scan.CoveringIndex
	idxColumns := scan.IndexColumns
	nSelected := len(selectedFields)
	hasResidualFilter := tableFilter != nil || (isCovering && coveringFilter != nil)

	for _, indexValue := range scan.IndexKeys {
		err := idx.VisitRowIDs(ctx, indexValue, func(rowID RowID) error {
			if !hasResidualFilter {
				if err := ctx.Err(); err != nil {
					return err
				}
				return errStopScan
			}

			_, ok, err := t.indexedScanRow(
				ctx, indexValue, rowID, isCovering, idxColumns,
				selectedMask, nSelected, tableFilter, coveringFilter,
			)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return errStopScan
		})
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if errors.Is(err, errStopScan) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("index lookup failed: %w", err)
		}
	}
	return false, nil
}

func (t *Table) fullTextIndexScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error) error {
	secondaryIndex, query, queryTokens, err := t.fullTextScanState(scan)
	if err != nil {
		return err
	}
	if len(queryTokens) == 0 {
		return nil
	}

	if len(queryTokens) == 1 && len(query.Phrases) == 0 {
		return t.fullTextSingleTermIndexScan(ctx, secondaryIndex, scan, queryTokens[0], selectedFields, out)
	}
	surviving, err := t.collectFullTextScanRowIDs(ctx, secondaryIndex, scan, query, queryTokens)
	if err != nil {
		return err
	}
	if len(surviving) == 0 {
		return nil
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	nSelected := len(selectedFields)
	for _, rowID := range surviving {
		if err := ctx.Err(); err != nil {
			return err
		}

		row, ok, err := t.rowIDScanRow(ctx, rowID, selectedMask, nSelected, tableFilter)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if err := out(row); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) fullTextScanState(scan Scan) (SecondaryIndex, *textSearchQuery, []string, error) {
	secondaryIndex, ok := t.SecondaryIndexes[scan.IndexName]
	if !ok || secondaryIndex.Method != IndexMethodFullText || secondaryIndex.InvertedIndex == nil {
		return SecondaryIndex{}, nil, nil, fmt.Errorf("no index found for full-text scan: %s", scan.IndexName)
	}
	if len(scan.IndexKeys) == 0 {
		return secondaryIndex, nil, nil, nil
	}

	query := scan.FullTextQuery
	if query == nil {
		terms := make([]string, 0, len(scan.IndexKeys))
		for _, key := range scan.IndexKeys {
			term, ok := key.(string)
			if !ok {
				continue
			}
			terms = appendUniqueTextSearchTerms(terms, term)
		}
		query = &textSearchQuery{Terms: terms}
	}

	queryTokens := query.allUniqueTokens()
	return secondaryIndex, query, queryTokens, nil
}

func (t *Table) collectFullTextScanRowIDs(
	ctx context.Context,
	secondaryIndex SecondaryIndex,
	scan Scan,
	query *textSearchQuery,
	queryTokens []string,
) ([]RowID, error) {
	if len(queryTokens) == 0 {
		return nil, nil
	}
	if len(query.Phrases) == 0 {
		return t.collectFullTextMultiTermRowIDs(ctx, secondaryIndex, scan.IndexName, queryTokens)
	}
	return t.collectFullTextPhraseRowIDs(ctx, secondaryIndex, scan, query, queryTokens)
}

func (t *Table) collectFullTextMultiTermRowIDs(
	ctx context.Context,
	secondaryIndex SecondaryIndex,
	indexName string,
	terms []string,
) ([]RowID, error) {
	termsByDocFreq := make([]string, 0, len(terms))
	docFreqByTerm := make(map[string]uint32, len(terms))
	for _, term := range terms {
		stats, err := secondaryIndex.InvertedIndex.Stats(ctx, term)
		if err != nil {
			return nil, fmt.Errorf("full-text stats lookup failed: %w", err)
		}
		if stats.DocFreq == 0 {
			return nil, nil
		}
		docFreqByTerm[term] = stats.DocFreq
		termsByDocFreq = append(termsByDocFreq, term)
	}
	slices.SortFunc(termsByDocFreq, func(a, b string) int {
		if docFreqByTerm[a] < docFreqByTerm[b] {
			return -1
		}
		if docFreqByTerm[a] > docFreqByTerm[b] {
			return 1
		}
		return strings.Compare(a, b)
	})

	var surviving []RowID
	for i, term := range termsByDocFreq {
		rowIDs, err := loadFullTextRowIDsForTerm(ctx, secondaryIndex, indexName, term, docFreqByTerm[term])
		if err != nil {
			return nil, err
		}
		if len(rowIDs) == 0 {
			return nil, nil
		}
		if i == 0 {
			surviving = rowIDs
			continue
		}
		surviving = intersectTwoSortedSets(surviving, rowIDs)
		if len(surviving) == 0 {
			return nil, nil
		}
	}
	return surviving, nil
}

func (t *Table) collectFullTextPhraseRowIDs(
	ctx context.Context,
	secondaryIndex SecondaryIndex,
	scan Scan,
	query *textSearchQuery,
	queryTokens []string,
) ([]RowID, error) {
	docFreqByTerm := make(map[string]uint32, len(queryTokens))
	for _, term := range queryTokens {
		stats, err := secondaryIndex.InvertedIndex.Stats(ctx, term)
		if err != nil {
			return nil, fmt.Errorf("full-text stats lookup failed: %w", err)
		}
		if stats.DocFreq == 0 {
			return nil, nil
		}
		docFreqByTerm[term] = stats.DocFreq
	}

	// Stream the rarest term and keep only the other posting lists for binary
	// lookups. This avoids retaining the candidate posting list while still
	// preserving sorted survivor output.
	candidateIdx := rarestFullTextTokenIndex(queryTokens, docFreqByTerm)
	candidateTerm := queryTokens[candidateIdx]

	postingsByTerm := make(map[string][]invertedPosting, len(queryTokens)-1)
	for i, term := range queryTokens {
		if i == candidateIdx {
			continue
		}
		postings, err := loadFullTextPostingsForTerm(ctx, secondaryIndex, scan.IndexName, term, docFreqByTerm[term])
		if err != nil {
			return nil, err
		}
		if len(postings) == 0 {
			return nil, nil
		}
		postingsByTerm[term] = postings
	}

	// Pre-compute phrase→queryToken index mapping once per query so the
	// per-row phrase check is allocation-free.
	type phraseMapping struct{ indices []int }
	phraseMappings := make([]phraseMapping, len(query.Phrases))
	for pi, phrase := range query.Phrases {
		indices := make([]int, len(phrase))
		for i, term := range phrase {
			idx := -1
			for j, qt := range queryTokens {
				if qt == term {
					idx = j
					break
				}
			}
			if idx < 0 {
				return nil, nil
			}
			indices[i] = idx
		}
		phraseMappings[pi] = phraseMapping{indices: indices}
	}

	// allPositions[i] holds the positions of queryTokens[i] in the current
	// candidate document. Allocated once and overwritten for each candidate.
	allPositions := make([][]uint32, len(queryTokens))

	iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, candidateTerm)
	if err != nil {
		return nil, fmt.Errorf("full-text lookup failed: %w", err)
	}
	surviving := make([]RowID, 0, docFreqByTerm[candidateTerm])

	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return nil, fmt.Errorf("full-text lookup failed: %w", err)
		}
		if !ok {
			break
		}
		mode, decoded, err := decodeInvertedPostingList(block.Payload)
		if err != nil {
			return nil, fmt.Errorf("full-text decode failed: %w", err)
		}
		if mode != invertedPostingModePositions {
			return nil, fmt.Errorf("full-text index %s uses posting mode %d", scan.IndexName, mode)
		}
		for _, candidatePosting := range decoded {
			rowID := candidatePosting.RowID
			matches := true
			allPositions[candidateIdx] = candidatePosting.Positions
			for i := range queryTokens {
				if i == candidateIdx {
					continue
				}
				termPostings := postingsByTerm[queryTokens[i]]
				idx := invertedPostingBinarySearch(termPostings, rowID)
				if idx < 0 {
					matches = false
					break
				}
				allPositions[i] = termPostings[idx].Positions
			}
			if !matches {
				continue
			}
			for _, pm := range phraseMappings {
				if !textSearchPhraseMatchesSorted(allPositions, pm.indices) {
					matches = false
					break
				}
			}
			if matches {
				surviving = append(surviving, rowID)
			}
		}
	}
	return surviving, nil
}

func rarestFullTextTokenIndex(queryTokens []string, docFreqByTerm map[string]uint32) int {
	bestIdx := 0
	bestFreq := docFreqByTerm[queryTokens[0]]
	for i := 1; i < len(queryTokens); i++ {
		freq := docFreqByTerm[queryTokens[i]]
		if freq < bestFreq || (freq == bestFreq && queryTokens[i] < queryTokens[bestIdx]) {
			bestIdx = i
			bestFreq = freq
		}
	}
	return bestIdx
}

func loadFullTextPostingsForTerm(
	ctx context.Context,
	secondaryIndex SecondaryIndex,
	indexName string,
	term string,
	docFreq uint32,
) ([]invertedPosting, error) {
	iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
	if err != nil {
		return nil, fmt.Errorf("full-text lookup failed: %w", err)
	}

	postings := make([]invertedPosting, 0, docFreq)
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return nil, fmt.Errorf("full-text lookup failed: %w", err)
		}
		if !ok {
			break
		}
		mode, decoded, err := decodeInvertedPostingList(block.Payload)
		if err != nil {
			return nil, fmt.Errorf("full-text decode failed: %w", err)
		}
		if mode != invertedPostingModePositions {
			return nil, fmt.Errorf("full-text index %s uses posting mode %d", indexName, mode)
		}
		postings = append(postings, decoded...)
	}
	return postings, nil
}

func loadFullTextRowIDsForTerm(ctx context.Context, secondaryIndex SecondaryIndex, indexName, term string, docFreq uint32) ([]RowID, error) {
	iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
	if err != nil {
		return nil, fmt.Errorf("full-text lookup failed: %w", err)
	}

	rowIDs := make([]RowID, 0, docFreq)
	var (
		lastRowID RowID
		haveLast  bool
	)
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return nil, fmt.Errorf("full-text lookup failed: %w", err)
		}
		if !ok {
			break
		}
		mode, err := forEachInvertedPostingRowID(block.Payload, func(rowID RowID) error {
			if haveLast && rowID == lastRowID {
				return nil
			}
			haveLast = true
			lastRowID = rowID
			rowIDs = append(rowIDs, rowID)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if mode != invertedPostingModePositions {
			return nil, fmt.Errorf("full-text index %s uses posting mode %d", indexName, mode)
		}
	}
	return rowIDs, nil
}

func (t *Table) fullTextSingleTermIndexScan(ctx context.Context, secondaryIndex SecondaryIndex, scan Scan, term string, selectedFields []Field, out func(Row) error) error {
	iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
	if err != nil {
		return fmt.Errorf("full-text lookup failed: %w", err)
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	nSelected := len(selectedFields)
	var (
		lastRowID RowID
		haveLast  bool
	)
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return fmt.Errorf("full-text lookup failed: %w", err)
		}
		if !ok {
			break
		}
		mode, err := forEachInvertedPostingRowID(block.Payload, func(rowID RowID) error {
			if haveLast && rowID == lastRowID {
				return nil
			}
			haveLast = true
			lastRowID = rowID
			if err := ctx.Err(); err != nil {
				return err
			}

			row, ok, err := t.rowIDScanRow(ctx, rowID, selectedMask, nSelected, tableFilter)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}

			return out(row)
		})
		if err != nil {
			return err
		}
		if mode != invertedPostingModePositions {
			return fmt.Errorf("full-text index %s uses posting mode %d", scan.IndexName, mode)
		}
	}
	return nil
}

func (t *Table) invertedIndexScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error) error {
	secondaryIndex, ok := t.SecondaryIndexes[scan.IndexName]
	if !ok || secondaryIndex.Method != IndexMethodInverted || secondaryIndex.InvertedIndex == nil {
		return fmt.Errorf("no index found for inverted scan: %s", scan.IndexName)
	}
	if len(scan.IndexKeys) == 0 {
		return nil
	}
	if len(scan.IndexKeys) == 1 {
		term, ok := scan.IndexKeys[0].(string)
		if !ok {
			return nil
		}
		return t.singleTermInvertedIndexScan(ctx, secondaryIndex, scan, term, selectedFields, out)
	}

	surviving, err := t.collectInvertedScanRowIDs(ctx, scan)
	if err != nil {
		return err
	}
	if len(surviving) == 0 {
		return nil
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileInvertedScanFilter(t.Columns, scan.Filters)
	nSelected := len(selectedFields)
	for _, rowID := range surviving {
		if err := ctx.Err(); err != nil {
			return err
		}

		row, ok, err := t.rowIDScanRow(ctx, rowID, selectedMask, nSelected, tableFilter)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if err := out(row); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) singleTermInvertedIndexScan(
	ctx context.Context,
	secondaryIndex SecondaryIndex,
	scan Scan,
	term string,
	selectedFields []Field,
	out func(Row) error,
) error {
	iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
	if err != nil {
		return fmt.Errorf("inverted lookup failed: %w", err)
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileInvertedScanFilter(t.Columns, scan.Filters)
	nSelected := len(selectedFields)
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return fmt.Errorf("inverted lookup failed: %w", err)
		}
		if !ok {
			break
		}
		mode, err := forEachInvertedPostingRowID(block.Payload, func(rowID RowID) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			row, ok, err := t.rowIDScanRow(ctx, rowID, selectedMask, nSelected, tableFilter)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}

			return out(row)
		})
		if err != nil {
			return err
		}
		if mode != invertedPostingModeRowIDs {
			return fmt.Errorf("inverted index %s uses posting mode %d", scan.IndexName, mode)
		}
	}
	return nil
}

func (t *Table) collectInvertedScanRowIDs(ctx context.Context, scan Scan) ([]RowID, error) {
	secondaryIndex, ok := t.SecondaryIndexes[scan.IndexName]
	if !ok || secondaryIndex.Method != IndexMethodInverted || secondaryIndex.InvertedIndex == nil {
		return nil, fmt.Errorf("no index found for inverted scan: %s", scan.IndexName)
	}
	if len(scan.IndexKeys) == 0 {
		return nil, nil
	}
	terms, err := t.invertedScanTermsByDocFreq(ctx, secondaryIndex, scan)
	if err != nil {
		return nil, err
	}
	if len(terms) == 0 {
		return nil, nil
	}

	var surviving []RowID
	for i, termStats := range terms {
		if i == 0 {
			rowIDs, err := loadInvertedRowIDsForTerm(ctx, secondaryIndex, scan.IndexName, termStats.term, termStats.docFreq)
			if err != nil {
				return nil, err
			}
			if len(rowIDs) == 0 {
				return nil, nil
			}
			surviving = rowIDs
			continue
		}

		var err error
		surviving, err = intersectInvertedRowIDsWithTerm(ctx, surviving, secondaryIndex, scan.IndexName, termStats.term)
		if err != nil {
			return nil, err
		}
		if len(surviving) == 0 {
			return nil, nil
		}
	}
	return surviving, nil
}

type invertedScanTermStats struct {
	term    string
	docFreq uint32
}

func (t *Table) invertedScanTermsByDocFreq(ctx context.Context, secondaryIndex SecondaryIndex, scan Scan) ([]invertedScanTermStats, error) {
	terms := make([]invertedScanTermStats, 0, len(scan.IndexKeys))
	for _, key := range scan.IndexKeys {
		term, ok := key.(string)
		if !ok {
			continue
		}
		var docFreq uint32
		if estimator, ok := secondaryIndex.InvertedIndex.(invertedDocFreqEstimator); ok {
			var err error
			docFreq, err = estimator.EstimateDocFreq(ctx, term)
			if err != nil {
				return nil, fmt.Errorf("inverted stats lookup failed: %w", err)
			}
		} else {
			stats, err := secondaryIndex.InvertedIndex.Stats(ctx, term)
			if err != nil {
				return nil, fmt.Errorf("inverted stats lookup failed: %w", err)
			}
			docFreq = stats.DocFreq
		}
		if docFreq == 0 {
			return nil, nil
		}
		terms = append(terms, invertedScanTermStats{term: term, docFreq: docFreq})
	}
	slices.SortFunc(terms, func(a, b invertedScanTermStats) int {
		if a.docFreq < b.docFreq {
			return -1
		}
		if a.docFreq > b.docFreq {
			return 1
		}
		return strings.Compare(a.term, b.term)
	})
	return terms, nil
}

func (t *Table) tryCountFromExactInvertedIndex(ctx context.Context, plan QueryPlan) (StatementResult, bool, error) {
	if len(plan.Scans) != 1 || plan.Scans[0].Type != ScanTypeInverted {
		return StatementResult{}, false, nil
	}
	scan := plan.Scans[0]
	if !jsonInvertedScanFilterIsExact(scan.Filters) {
		return StatementResult{}, false, nil
	}
	count, err := t.countInvertedIndexScan(ctx, scan)
	if err != nil {
		return StatementResult{}, false, err
	}
	return countResult(count), true, nil
}

// tryCountFromFullTextIndex is a count shortcut for full-text COUNT(*) queries
// with no additional post-scan filters. Single-term queries read DocFreq
// directly; multi-term and phrase queries count matching RowIDs without
// fetching or materialising table rows.
func (t *Table) tryCountFromFullTextIndex(ctx context.Context, plan QueryPlan) (StatementResult, bool, error) {
	if len(plan.Scans) != 1 || plan.Scans[0].Type != ScanTypeFullText {
		return StatementResult{}, false, nil
	}
	scan := plan.Scans[0]
	if len(scan.Filters) > 0 {
		// Additional WHERE predicates require row-level evaluation.
		return StatementResult{}, false, nil
	}

	secondaryIndex, query, queryTokens, err := t.fullTextScanState(scan)
	if err != nil {
		return StatementResult{}, false, err
	}
	if len(queryTokens) == 0 {
		return countResult(0), true, nil
	}
	if query != nil && len(queryTokens) == 1 && len(query.Phrases) == 0 {
		if counter, ok := secondaryIndex.InvertedIndex.(invertedDocFreqCounter); ok {
			docFreq, err := counter.CountDocFreq(ctx, queryTokens[0])
			if err != nil {
				return StatementResult{}, false, err
			}
			return countResult(int64(docFreq)), true, nil
		}
		stats, err := secondaryIndex.InvertedIndex.Stats(ctx, queryTokens[0])
		if err != nil {
			return StatementResult{}, false, err
		}
		return countResult(int64(stats.DocFreq)), true, nil
	}

	rowIDs, err := t.collectFullTextScanRowIDs(ctx, secondaryIndex, scan, query, queryTokens)
	if err != nil {
		return StatementResult{}, false, err
	}
	return countResult(int64(len(rowIDs))), true, nil
}

func jsonInvertedScanFilterIsExact(filters OneOrMore) bool {
	if len(filters) != 1 || len(filters[0]) != 1 {
		return false
	}
	_, query, ok := jsonContainsLiteralCondition(filters[0][0])
	if !ok {
		return false
	}
	queryValue, err := decodeJSONForInvertedIndex(query)
	if err != nil {
		return false
	}
	return jsonInvertedQueryTermsAreExact(queryValue)
}

func (t *Table) countInvertedIndexScan(ctx context.Context, scan Scan) (int64, error) {
	secondaryIndex, ok := t.SecondaryIndexes[scan.IndexName]
	if !ok || secondaryIndex.Method != IndexMethodInverted || secondaryIndex.InvertedIndex == nil {
		return 0, fmt.Errorf("no index found for inverted scan: %s", scan.IndexName)
	}
	if len(scan.IndexKeys) == 0 {
		return 0, nil
	}
	if len(scan.IndexKeys) == 1 {
		term, ok := scan.IndexKeys[0].(string)
		if !ok {
			return 0, nil
		}
		stats, err := secondaryIndex.InvertedIndex.Stats(ctx, term)
		if err != nil {
			return 0, fmt.Errorf("inverted stats lookup failed: %w", err)
		}
		return int64(stats.DocFreq), nil
	}
	terms, err := t.invertedScanTermsByDocFreq(ctx, secondaryIndex, scan)
	if err != nil {
		return 0, err
	}
	if len(terms) == 0 {
		return 0, nil
	}

	var surviving []RowID
	for i, termStats := range terms {
		if i == 0 {
			rowIDs, err := loadInvertedRowIDsForTerm(ctx, secondaryIndex, scan.IndexName, termStats.term, termStats.docFreq)
			if err != nil {
				return 0, err
			}
			if len(rowIDs) == 0 {
				return 0, nil
			}
			surviving = rowIDs
			continue
		}

		var err error
		surviving, err = intersectInvertedRowIDsWithTerm(ctx, surviving, secondaryIndex, scan.IndexName, termStats.term)
		if err != nil {
			return 0, err
		}
		if len(surviving) == 0 {
			return 0, nil
		}
	}
	return int64(len(surviving)), nil
}

func intersectInvertedRowIDsWithTerm(
	ctx context.Context,
	candidates []RowID,
	secondaryIndex SecondaryIndex,
	indexName, term string,
) ([]RowID, error) {
	if len(candidates) == 0 {
		return nil, nil
	}

	if scanner, ok := secondaryIndex.InvertedIndex.(invertedRowIDScanner); ok {
		return intersectInvertedRowIDsWithScanner(ctx, candidates, scanner, term)
	}

	iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
	if err != nil {
		return nil, fmt.Errorf("inverted lookup failed: %w", err)
	}

	out := candidates[:0]
	candidateIdx := 0
	var (
		lastRowID RowID
		haveLast  bool
	)
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return nil, fmt.Errorf("inverted lookup failed: %w", err)
		}
		if !ok {
			break
		}

		mode, err := forEachInvertedPostingRowID(block.Payload, func(rowID RowID) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if haveLast && rowID == lastRowID {
				return nil
			}
			haveLast = true
			lastRowID = rowID

			for candidateIdx < len(candidates) && candidates[candidateIdx] < rowID {
				candidateIdx += 1
			}
			if candidateIdx >= len(candidates) {
				return nil
			}
			if candidates[candidateIdx] == rowID {
				out = append(out, rowID)
				candidateIdx += 1
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if mode != invertedPostingModeRowIDs {
			return nil, fmt.Errorf("inverted index %s uses posting mode %d", indexName, mode)
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func intersectInvertedRowIDsWithScanner(
	ctx context.Context,
	candidates []RowID,
	scanner invertedRowIDScanner,
	term string,
) ([]RowID, error) {
	out := candidates[:0]
	candidateIdx := 0
	err := scanner.ForEachRowID(ctx, term, func(rowID RowID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for candidateIdx < len(candidates) && candidates[candidateIdx] < rowID {
			candidateIdx += 1
		}
		if candidateIdx >= len(candidates) {
			return nil
		}
		if candidates[candidateIdx] == rowID {
			out = append(out, rowID)
			candidateIdx += 1
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inverted row id scan failed: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func loadInvertedRowIDsForTerm(ctx context.Context, secondaryIndex SecondaryIndex, indexName, term string, docFreq uint32) ([]RowID, error) {
	if loader, ok := secondaryIndex.InvertedIndex.(invertedRowIDLoader); ok {
		rowIDs, err := loader.LoadRowIDs(ctx, term, docFreq)
		if err != nil {
			return nil, fmt.Errorf("inverted row id lookup failed: %w", err)
		}
		return rowIDs, nil
	}

	iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
	if err != nil {
		return nil, fmt.Errorf("inverted lookup failed: %w", err)
	}

	rowIDs := make([]RowID, 0, docFreq)
	var (
		lastRowID RowID
		haveLast  bool
	)
	for {
		block, ok, err := iter.NextBlock(ctx)
		if err != nil {
			return nil, fmt.Errorf("inverted lookup failed: %w", err)
		}
		if !ok {
			break
		}
		mode, err := forEachInvertedPostingRowID(block.Payload, func(rowID RowID) error {
			if haveLast && rowID == lastRowID {
				return nil
			}
			haveLast = true
			lastRowID = rowID
			rowIDs = append(rowIDs, rowID)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if mode != invertedPostingModeRowIDs {
			return nil, fmt.Errorf("inverted index %s uses posting mode %d", indexName, mode)
		}
	}
	return rowIDs, nil
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
	nSelected := len(selectedFields)

	err := idx.ScanAll(ctx, reverse, func(key any, rowID RowID) error {
		row, ok, err := t.indexedScanRow(
			ctx, key, rowID, scan.CoveringIndex, scan.IndexColumns,
			selectedMask, nSelected, nil, nil,
		)
		if err != nil || !ok {
			return err
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
			col, idx := row.getColumnQualified(f.AliasPrefix, f.Name)
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

func appendOperandSourceFields(fields []Field, operand Operand) []Field {
	switch operand.Type {
	case OperandField:
		return append(fields, operand.Value.(Field))
	case OperandExpr:
		expr, ok := operand.Value.(*Expr)
		if !ok {
			return fields
		}
		for _, colName := range expr.Columns() {
			fields = append(fields, Field{Name: colName})
		}
	}
	return fields
}

// buildGroupKey appends a collision-free group key to buf using pre-computed column indices.
// The encoding matches rowDistinctKey so the two functions are interchangeable for the same values.
// buf must be reset (zero-length) by the caller before each call; the returned slice is buf grown in place.
func buildGroupKey(buf []byte, row Row, colIndices []int) []byte {
	for i, colIdx := range colIndices {
		if i > 0 {
			buf = append(buf, '\x1f')
		}
		if colIdx < 0 || colIdx >= len(row.Values) {
			buf = append(buf, "null"...)
			continue
		}
		v := row.Values[colIdx]
		if !v.Valid {
			buf = append(buf, "null"...)
			continue
		}
		buf = appendGroupKeyValue(buf, v)
	}
	return buf
}

func buildGroupKeyFromRowView(buf []byte, view RowView, colIndices []int) ([]byte, error) {
	for i, colIdx := range colIndices {
		if i > 0 {
			buf = append(buf, '\x1f')
		}
		if colIdx < 0 || colIdx >= len(view.Columns()) {
			buf = append(buf, "null"...)
			continue
		}
		value, err := view.ValueAt(colIdx)
		if err != nil {
			return nil, err
		}
		if !value.Valid {
			buf = append(buf, "null"...)
			continue
		}
		buf = appendGroupKeyValue(buf, value)
	}
	return buf, nil
}

func appendGroupKeyValue(buf []byte, value OptionalValue) []byte {
	switch val := value.Value.(type) {
	case TextPointer:
		buf = append(buf, 't')
		buf = strconv.AppendInt(buf, int64(val.Length), 10)
		buf = append(buf, ':')
		buf = append(buf, val.Data...)
	case bool:
		buf = append(buf, "b:"...)
		buf = strconv.AppendBool(buf, val)
	case int64:
		buf = append(buf, "i64:"...)
		buf = strconv.AppendInt(buf, val, 10)
	case int32:
		buf = append(buf, "i32:"...)
		buf = strconv.AppendInt(buf, int64(val), 10)
	case float64:
		buf = append(buf, "f64:"...)
		buf = strconv.AppendFloat(buf, val, 'g', -1, 64)
	case float32:
		buf = append(buf, "f32:"...)
		buf = strconv.AppendFloat(buf, float64(val), 'g', -1, 32)
	default:
		buf = fmt.Appendf(buf, "?:%v", val)
	}
	return buf
}

// groupByColumnIndex resolves the index of field f in cols.
// Tries the alias-qualified name first (e.g. "t.age"), then falls back to the bare name.
func groupByColumnIndex(cols []Column, f Field) int {
	if f.AliasPrefix != "" {
		plen := len(f.AliasPrefix)
		total := plen + 1 + len(f.Name)
		for i, col := range cols {
			n := col.Name
			if len(n) == total && n[:plen] == f.AliasPrefix && n[plen] == '.' && n[plen+1:] == f.Name {
				return i
			}
		}
	}
	for i, col := range cols {
		if col.Name == f.Name {
			return i
		}
	}
	return -1
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
// Uses a hash set — call deduplicateSortedRows instead when the rows are already sorted
// by (at least) all of fields, which avoids the map allocation entirely.
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

// deduplicateSortedRows removes duplicate rows by comparing adjacent pairs.
// rows must be sorted such that equal projected rows (same values for fields) are
// adjacent — use distinctExtendOrderBy to guarantee this before calling sortRows.
// In-place: returns a sub-slice of the input; no heap allocation.
func deduplicateSortedRows(rows []Row, fields []Field) []Row {
	if len(rows) <= 1 {
		return rows
	}
	w := 1
	for i := 1; i < len(rows); i++ {
		equal := true
		for _, f := range fields {
			va, _ := rows[i].getValueQualified(f.AliasPrefix, f.Name)
			vb, _ := rows[w-1].getValueQualified(f.AliasPrefix, f.Name)
			if compareValues(va, vb) != 0 {
				equal = false
				break
			}
		}
		if !equal {
			rows[w] = rows[i]
			w++
		}
	}
	return rows[:w]
}

// distinctExtendOrderBy returns an extended ORDER BY slice that appends any
// requestedFields not already present in orderBy as ascending tiebreakers.
// This guarantees that equal projected rows are adjacent after sorting,
// enabling hash-free deduplication via deduplicateSortedRows.
func distinctExtendOrderBy(orderBy []OrderBy, requestedFields []Field) []OrderBy {
	if len(requestedFields) == 0 {
		return orderBy
	}
	inOrderBy := make(map[string]struct{}, len(orderBy)*2)
	for _, clause := range orderBy {
		inOrderBy[clause.Field.Name] = struct{}{}
		if clause.Field.AliasPrefix != "" {
			inOrderBy[clause.Field.AliasPrefix+"."+clause.Field.Name] = struct{}{}
		}
	}
	extended := make([]OrderBy, len(orderBy), len(orderBy)+len(requestedFields))
	copy(extended, orderBy)
	for _, f := range requestedFields {
		if f.Expr != nil {
			continue
		}
		key := f.Name
		if _, ok := inOrderBy[key]; ok {
			continue
		}
		if f.AliasPrefix != "" {
			qualified := f.AliasPrefix + "." + f.Name
			if _, ok := inOrderBy[qualified]; ok {
				continue
			}
		}
		extended = append(extended, OrderBy{Field: f, Direction: Asc})
		inOrderBy[f.Name] = struct{}{}
	}
	return extended
}

// allProjectedInOrderBy returns true when every non-expression field in
// requestedFields is covered by an ORDER BY clause.  When the plan's index
// already delivers rows in ORDER BY order (!SortInMemory), this means equal
// projected rows arrive adjacently — adjacent-compare dedup is then O(1)
// instead of an O(N) hash set.
func allProjectedInOrderBy(requestedFields []Field, orderBy []OrderBy) bool {
	if len(requestedFields) == 0 || len(orderBy) == 0 {
		return false
	}
	inOrderBy := make(map[string]struct{}, len(orderBy)*2)
	for _, clause := range orderBy {
		inOrderBy[clause.Field.Name] = struct{}{}
		if clause.Field.AliasPrefix != "" {
			inOrderBy[clause.Field.AliasPrefix+"."+clause.Field.Name] = struct{}{}
		}
	}
	for _, f := range requestedFields {
		if f.Expr != nil {
			return false
		}
		if _, ok := inOrderBy[f.Name]; ok {
			continue
		}
		if f.AliasPrefix != "" {
			if _, ok := inOrderBy[f.AliasPrefix+"."+f.Name]; ok {
				continue
			}
		}
		return false
	}
	return true
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

		// Re-read page when the cursor crossed a page boundary.
		if page.Index != cursor.PageIdx {
			page, err = t.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return fmt.Errorf("sequential scan: %w", err)
			}
		}

		if cursor.CellIdx > page.LeafNode.Header.Cells-1 || len(page.LeafNode.Cells) == 0 {
			return fmt.Errorf("cell index %d out of bounds, max %d", cursor.CellIdx, page.LeafNode.Header.Cells-1)
		}

		// Snapshot the current cell before advancing the cursor so every path
		// decodes from the same cell while matching fetchRowWithMask advancement.
		cell := page.LeafNode.Cells[cursor.CellIdx]
		advanceLeafCursor(cursor, page)
		view := NewRowView(t.Columns, cell)

		if !twoPhase {
			row, err := view.MaterializeWithOverflow(ctx, t.pager, fullMask)
			if err != nil {
				return fmt.Errorf("sequential scan materialize row: %w", err)
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

		// Phase 1: decode only the columns needed to evaluate the predicate.
		filterRow, err := view.Materialize(filterMask)
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
		row, err := view.MaterializeWithOverflow(ctx, t.pager, fullMask)
		if err != nil {
			return fmt.Errorf("sequential scan read overflow: %w", err)
		}

		if err := out(row); err != nil {
			return err
		}
	}

	return nil
}

// countSequentialScanZeroAlloc counts rows that match the scan filter without
// collecting them. Simple predicates use RowView so count scans can evaluate
// filters directly over cell bytes; predicates that still need Row evaluation
// materialise from RowView at the predicate boundary. Virtual tables fall back to
// the general sequentialScan path because their rows are already materialised.
func (t *Table) countSequentialScanZeroAlloc(ctx context.Context, scan Scan, selectedFields []Field) (StatementResult, error) {
	if t.virtualRows != nil {
		var count int64
		err := t.sequentialScan(ctx, scan, selectedFields, func(Row) error {
			count += 1
			return nil
		})
		if err != nil {
			return StatementResult{}, err
		}
		return countResult(count), nil
	}
	if rowViewFilterSupports(t.Columns, scan.Filters) {
		return t.countSequentialScanRowView(ctx, scan)
	}

	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}

	fullMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)

	page, err := t.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return StatementResult{}, fmt.Errorf("count sequential scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	var count int64
	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return StatementResult{}, err
		}

		if page.Index != cursor.PageIdx {
			page, err = t.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return StatementResult{}, fmt.Errorf("count sequential scan: %w", err)
			}
		}

		cell := page.LeafNode.Cells[cursor.CellIdx]
		advanceLeafCursor(cursor, page)

		if tableFilter != nil {
			row, err := NewRowView(t.Columns, cell).MaterializeWithOverflow(ctx, t.pager, fullMask)
			if err != nil {
				return StatementResult{}, err
			}
			ok, err := tableFilter(row)
			if err != nil {
				return StatementResult{}, err
			}
			if !ok {
				continue
			}
		}
		// No filter or filter passed: count the row without touching row data.
		count += 1
	}
	return countResult(count), nil
}

func (t *Table) tryCountFromIndexScan(ctx context.Context, plan QueryPlan) (StatementResult, bool, error) {
	if len(plan.Scans) != 1 {
		return StatementResult{}, false, nil
	}
	scan := plan.Scans[0]
	if len(scan.Filters) > 0 {
		return StatementResult{}, false, nil
	}
	switch scan.Type {
	case ScanTypeIndexAll, ScanTypeIndexRange, ScanTypeIndexPoint:
	default:
		return StatementResult{}, false, nil
	}

	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return StatementResult{}, true, fmt.Errorf("no index found for count scan: %s", scan.IndexName)
	}

	var count int64
	countRowID := func(RowID) error {
		count += 1
		return ctx.Err()
	}
	countIndexEntry := func(_ any, rowID RowID) error {
		return countRowID(rowID)
	}

	switch scan.Type {
	case ScanTypeIndexAll:
		if err := idx.ScanAll(ctx, false, countIndexEntry); err != nil {
			return StatementResult{}, true, err
		}
	case ScanTypeIndexRange:
		if err := idx.ScanRange(ctx, scan.RangeCondition, false, countIndexEntry); err != nil {
			return StatementResult{}, true, err
		}
	case ScanTypeIndexPoint:
		for _, indexValue := range scan.IndexKeys {
			err := idx.VisitRowIDs(ctx, indexValue, countRowID)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return StatementResult{}, true, fmt.Errorf("index lookup failed: %w", err)
			}
		}
	}

	return countResult(count), true, nil
}

func (t *Table) countSequentialScanRowView(ctx context.Context, scan Scan) (StatementResult, error) {
	tableFilter := compileRowViewFilterForColumns(t.Columns, t.pager, scan.Filters)
	if t.parallelScan {
		iterFactory, err := t.parallelSequentialRowViewIteratorFactory(ctx, tableFilter, 0, 0, false, false)
		if err != nil {
			return StatementResult{}, err
		}
		iter := iterFactory()
		var count int64
		for iter.Next(ctx) {
			count += 1
		}
		if err := iter.Err(); err != nil {
			return StatementResult{}, err
		}
		return countResult(count), nil
	}

	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}
	page, err := t.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return StatementResult{}, fmt.Errorf("count row view sequential scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	var count int64
	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return StatementResult{}, err
		}

		if page.Index != cursor.PageIdx {
			page, err = t.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return StatementResult{}, fmt.Errorf("count row view sequential scan: %w", err)
			}
		}

		cell := page.LeafNode.Cells[cursor.CellIdx]

		switch {
		case cursor.CellIdx < page.LeafNode.Header.Cells-1:
			cursor.CellIdx += 1
		case page.LeafNode.Header.NextLeaf == 0:
			cursor.EndOfTable = true
		default:
			cursor.PageIdx = page.LeafNode.Header.NextLeaf
			cursor.CellIdx = 0
		}

		if tableFilter != nil {
			ok, err := tableFilter(ctx, NewRowView(t.Columns, cell))
			if err != nil {
				return StatementResult{}, err
			}
			if !ok {
				continue
			}
		}
		count += 1
	}
	return countResult(count), nil
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
			// OperandExpr (e.g. JSON path): collect all column refs from the expression.
			if cond.Operand1.Type == OperandExpr {
				if expr, ok := cond.Operand1.Value.(*Expr); ok {
					for _, col := range expr.ColumnRefs() {
						filterCols[col] = struct{}{}
					}
				}
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

// compileInvertedScanFilter specializes JSON_CONTAINS rechecks for indexed
// inverted scans while preserving the generic filter path for all other cases.
func compileInvertedScanFilter(columns []Column, filters OneOrMore) func(Row) (bool, error) {
	jsonFilter, remainingFilters, ok := compileJSONContainsRecheck(columns, filters)
	if !ok {
		return compileScanFilter(columns, filters)
	}
	remainingFilter := compileScanFilter(columns, remainingFilters)
	return func(row Row) (bool, error) {
		ok, err := jsonFilter(row)
		if err != nil || !ok {
			return ok, err
		}
		if remainingFilter == nil {
			return true, nil
		}
		return remainingFilter(row)
	}
}

func compileInvertedRowViewFilter(columns []Column, pager TxPager, filters OneOrMore) (func(context.Context, RowView) (bool, error), bool) {
	jsonFilter, remainingFilters, ok := compileJSONContainsRowViewRecheck(columns, pager, filters)
	if !ok {
		if !rowViewFilterSupports(columns, filters) {
			return nil, false
		}
		return compileRowViewFilterForColumns(columns, pager, filters), true
	}
	if !rowViewFilterSupports(columns, remainingFilters) {
		return nil, false
	}
	remainingFilter := compileRowViewFilterForColumns(columns, pager, remainingFilters)
	return func(ctx context.Context, view RowView) (bool, error) {
		ok, err := jsonFilter(ctx, view)
		if err != nil || !ok {
			return ok, err
		}
		if remainingFilter == nil {
			return true, nil
		}
		return remainingFilter(ctx, view)
	}, true
}

func compileJSONContainsRecheck(columns []Column, filters OneOrMore) (func(Row) (bool, error), OneOrMore, bool) {
	if len(filters) != 1 {
		return nil, nil, false
	}
	columnIndexes := make(map[string]int, len(columns))
	for i := range columns {
		columnIndexes[columns[i].Name] = i
	}

	for condIdx, cond := range filters[0] {
		columnName, query, ok := jsonContainsLiteralCondition(cond)
		if !ok {
			continue
		}
		columnIdx, ok := columnIndexes[columnName]
		if !ok {
			return nil, nil, false
		}
		queryValue, err := decodeJSONForInvertedIndex(query)
		if err != nil {
			return nil, nil, false
		}
		exactTerms := jsonInvertedQueryTermsAreExact(queryValue)
		remaining := make(Conditions, 0, len(filters[0])-1)
		remaining = append(remaining, filters[0][:condIdx]...)
		remaining = append(remaining, filters[0][condIdx+1:]...)
		var remainingFilters OneOrMore
		if len(remaining) > 0 {
			remainingFilters = OneOrMore{remaining}
		}
		return func(row Row) (bool, error) {
			if exactTerms {
				return true, nil
			}
			if columnIdx >= len(row.Values) {
				return false, nil
			}
			value := row.Values[columnIdx]
			if !value.Valid {
				return false, nil
			}
			doc, ok := toStringVal(value.Value)
			if !ok {
				return false, fmt.Errorf("JSON_CONTAINS: first argument must be a string")
			}
			return jsonContainsDecodedQuery(doc, queryValue)
		}, remainingFilters, true
	}

	return nil, nil, false
}

func compileJSONContainsRowViewRecheck(
	columns []Column,
	pager TxPager,
	filters OneOrMore,
) (func(context.Context, RowView) (bool, error), OneOrMore, bool) {
	return compileJSONContainsRowViewFilter(columns, pager, filters, true)
}

func compileJSONContainsRowViewFilter(
	columns []Column,
	pager TxPager,
	filters OneOrMore,
	allowExactTermsShortcut bool,
) (func(context.Context, RowView) (bool, error), OneOrMore, bool) {
	if len(filters) != 1 {
		return nil, nil, false
	}
	columnIndexes := make(map[string]int, len(columns))
	for i := range columns {
		columnIndexes[columns[i].Name] = i
	}

	for condIdx, cond := range filters[0] {
		columnName, query, ok := jsonContainsLiteralCondition(cond)
		if !ok {
			continue
		}
		columnIdx, ok := columnIndexes[columnName]
		if !ok {
			return nil, nil, false
		}
		queryValue, err := decodeJSONForInvertedIndex(query)
		if err != nil {
			return nil, nil, false
		}
		exactTerms := jsonInvertedQueryTermsAreExact(queryValue)
		remaining := make(Conditions, 0, len(filters[0])-1)
		remaining = append(remaining, filters[0][:condIdx]...)
		remaining = append(remaining, filters[0][condIdx+1:]...)
		var remainingFilters OneOrMore
		if len(remaining) > 0 {
			remainingFilters = OneOrMore{remaining}
		}
		return func(ctx context.Context, view RowView) (bool, error) {
			if allowExactTermsShortcut && exactTerms {
				return true, nil
			}
			value, err := view.ValueAtWithOverflow(ctx, pager, columnIdx)
			if err != nil || !value.Valid {
				return false, err
			}
			doc, ok := toStringVal(value.Value)
			if !ok {
				return false, fmt.Errorf("JSON_CONTAINS: first argument must be a string")
			}
			return jsonContainsDecodedQuery(doc, queryValue)
		}, remainingFilters, true
	}

	return nil, nil, false
}

func jsonContainsLiteralCondition(cond Condition) (string, string, bool) {
	if cond.Operator != Eq || cond.Operand2.Type != OperandBoolean || cond.Operand2.Value != true {
		return "", "", false
	}
	if cond.Operand1.Type != OperandExpr {
		return "", "", false
	}
	expr, ok := cond.Operand1.Value.(*Expr)
	if !ok || expr.FuncName != "JSON_CONTAINS" || len(expr.Args) != 2 {
		return "", "", false
	}
	columnName := expr.Args[0].Column
	if columnName == "" {
		return "", "", false
	}
	query, ok := literalText(expr.Args[1])
	if !ok {
		return "", "", false
	}
	return columnName, query, true
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
// fetching table rows.
func (t *Table) collectRowIDsFromScan(ctx context.Context, scan Scan) ([]RowID, error) {
	if scan.Type == ScanTypeIndexIntersect || scan.Type == ScanTypeIndexUnion {
		return t.collectIndexSetScanRowIDs(ctx, scan)
	}

	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return nil, fmt.Errorf("no index found for intersect sub-scan: %s", scan.IndexName)
	}
	switch scan.Type {
	case ScanTypeIndexPoint:
		rowIDs := make([]RowID, 0, len(scan.IndexKeys)*MaxInlineRowIDs)
		for _, key := range scan.IndexKeys {
			err := idx.VisitRowIDs(ctx, key, func(rowID RowID) error {
				rowIDs = append(rowIDs, rowID)
				return ctx.Err()
			})
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return nil, fmt.Errorf("intersect point lookup: %w", err)
			}
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

func (t *Table) collectIndexSetScanRowIDs(ctx context.Context, scan Scan) ([]RowID, error) {
	switch scan.Type {
	case ScanTypeIndexIntersect, ScanTypeIndexUnion:
	default:
		return nil, fmt.Errorf("unsupported index set scan type: %s", scan.Type)
	}
	if len(scan.SubScans) == 0 {
		return nil, nil
	}

	var result []RowID
	for i, sub := range scan.SubScans {
		ids, err := t.collectRowIDsFromScan(ctx, sub)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			sortRowIDs(ids)
			result = ids
			continue
		}

		sortRowIDs(ids)
		switch scan.Type {
		case ScanTypeIndexIntersect:
			result = intersectTwoSortedSets(result, ids)
			if len(result) == 0 {
				return nil, nil
			}
		case ScanTypeIndexUnion:
			result = unionTwoSortedSets(result, ids)
		}
	}

	return result, nil
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
				j -= 1
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
	surviving, err := t.collectIndexSetScanRowIDs(ctx, scan)
	if err != nil {
		return err
	}
	if len(surviving) == 0 {
		return nil
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	nSelected := len(selectedFields)

	for _, rowID := range surviving {
		if err := ctx.Err(); err != nil {
			return err
		}

		row, ok, err := t.rowIDScanRow(ctx, rowID, selectedMask, nSelected, tableFilter)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if err := out(row); err != nil {
			return err
		}
	}
	return nil
}

// unionTwoSortedSets returns the sorted, deduplicated union of two sorted RowID slices.
func unionTwoSortedSets(a, b []RowID) []RowID {
	result := make([]RowID, 0, len(a)+len(b))
	emit := func(id RowID) {
		if len(result) == 0 || result[len(result)-1] != id {
			result = append(result, id)
		}
	}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			emit(a[i])
			i += 1
			j += 1
		case a[i] < b[j]:
			emit(a[i])
			i += 1
		default:
			emit(b[j])
			j += 1
		}
	}
	for ; i < len(a); i++ {
		emit(a[i])
	}
	for ; j < len(b); j++ {
		emit(b[j])
	}
	return result
}

// unionSortedRowIDs sorts each input slice and returns their deduplicated union.
func unionSortedRowIDs(sets [][]RowID) []RowID {
	if len(sets) == 0 {
		return nil
	}
	for i := range sets {
		sortRowIDs(sets[i])
	}
	result := sets[0]
	for _, next := range sets[1:] {
		result = unionTwoSortedSets(result, next)
	}
	return result
}

// indexUnionScan executes a ScanTypeIndexUnion plan:
//  1. Collect RowID sets from each OR-group sub-scan.
//  2. Union (deduplicate) all sets in memory.
//  3. Fetch each surviving row once and re-check the full WHERE clause.
func (t *Table) indexUnionScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error) error {
	surviving, err := t.collectIndexSetScanRowIDs(ctx, scan)
	if err != nil {
		return err
	}
	if len(surviving) == 0 {
		return nil
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	nSelected := len(selectedFields)

	var emitted int64
	scanLimit := scan.ScanLimit
	for _, rowID := range surviving {
		if err := ctx.Err(); err != nil {
			return err
		}

		row, ok, err := t.rowIDScanRow(ctx, rowID, selectedMask, nSelected, tableFilter)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if err := out(row); err != nil {
			return err
		}
		emitted += 1
		if scanLimit > 0 && emitted >= scanLimit {
			return nil
		}
	}
	return nil
}

// hnswIndexScan executes an approximate nearest-neighbor search using an HNSW
// vector index.  It returns the top-k rows ordered by distance to the query vector.
func (t *Table) hnswIndexScan(ctx context.Context, scan Scan, selectedFields []Field, out func(Row) error) error {
	secondaryIndex, ok := t.SecondaryIndexes[scan.IndexName]
	if !ok || secondaryIndex.Method != IndexMethodHNSW || secondaryIndex.HNSWIndex == nil {
		return fmt.Errorf("no HNSW index found for scan: %s", scan.IndexName)
	}

	k := int(scan.ScanLimit)
	if k <= 0 {
		k = HNSWDefaultEfSearch
	}

	distFn := makeDistFunc(ctx, secondaryIndex.HNSWIndex, t, scan.IndexColumns[0].Name, scan.HNSWQueryVec, scan.HNSWFuncName)
	rowIDs, err := secondaryIndex.HNSWIndex.Search(ctx, k, HNSWDefaultEfSearch, distFn)
	if err != nil {
		return fmt.Errorf("HNSW search failed: %w", err)
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	nSelected := len(selectedFields)
	for _, rowID := range rowIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		row, found, err := t.rowIDScanRow(ctx, rowID, selectedMask, nSelected, nil)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := out(row); err != nil {
			return err
		}
	}
	return nil
}
