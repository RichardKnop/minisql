package minisql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"go.uber.org/zap"
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

	// Create query plan
	plan, err := t.PlanQuery(ctx, stmt)
	if err != nil {
		return StatementResult{}, err
	}

	t.logger.Debug("query plan", zap.String("query type", "SELECT"), zap.Any("plan", plan))

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
				selectedFields = appendOperandSourceFields(selectedFields, cond.Operand1)
				selectedFields = appendOperandSourceFields(selectedFields, cond.Operand2)
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
		if result, ok, err := t.selectStreamingDirectRowView(ctx, stmt, plan, requestedFields); ok || err != nil {
			return result, err
		}
		return t.selectStreamingDirect(ctx, stmt, plan, selectedFields, requestedFields)
	}

	// COUNT(*) with post-scan filters: count matching rows without collecting
	// them into a []Row. For sequential scans, also reuse a single values buffer
	// across all rows to eliminate the dominant per-row heap allocation.
	if stmt.IsSelectCountAll() && len(plan.Joins) == 0 {
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

	// GROUP BY + single sequential scan: stream rows through a reuse buffer to avoid
	// one make([]OptionalValue) per row. Falls back to the general path for virtual
	// tables and parallel scans (handled inside selectGroupByZeroAlloc).
	if stmt.IsSelectGroupBy() && len(plan.Joins) == 0 && len(plan.Scans) == 1 && plan.Scans[0].Type == ScanTypeSequential {
		return t.selectGroupByZeroAlloc(ctx, stmt, plan.Scans[0], selectedFields)
	}

	// Materialising path: buffer every matching row, then dispatch.
	// Required for COUNT (needs a total), GROUP BY, aggregates, ORDER BY (sort),
	// and JOIN (goroutine-based execution inside plan.Execute).
	//
	// LIMIT pushdown: for JOIN queries with no in-memory sort and no DISTINCT we
	// can stop collecting rows after OFFSET+LIMIT rows, avoiding a full scan of
	// every matching row.  plan.Execute propagates errLimitReached from the
	// callback; the JOIN goroutine is cancelled and drained inside Execute itself.
	var joinScanLimit int64
	if len(plan.Joins) > 0 && stmt.Limit.Valid && !plan.SortInMemory && !stmt.Distinct {
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

	return countResult(count), nil
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

	for _, row := range rows {
		for i, agg := range stmt.Aggregates {
			switch agg.Kind {
			case AggregateCount:
				states[i].count += 1

			case AggregateSum, AggregateAvg:
				colIdx := aggColIdx[i]
				if colIdx < 0 || colIdx >= len(row.Values) {
					continue
				}
				val := row.Values[colIdx]
				if !val.Valid {
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
				colIdx := aggColIdx[i]
				if colIdx < 0 || colIdx >= len(row.Values) {
					continue
				}
				val := row.Values[colIdx]
				if !val.Valid {
					continue
				}
				if !states[i].hasValue || compareValues(val, states[i].min) < 0 {
					states[i].min = val
					states[i].hasValue = true
				}

			case AggregateMax:
				colIdx := aggColIdx[i]
				if colIdx < 0 || colIdx >= len(row.Values) {
					continue
				}
				val := row.Values[colIdx]
				if !val.Valid {
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

// groupEntry holds per-group state offsets into the shared flat pools.
// Using flat pools eliminates one heap allocation per group for aggStates and
// groupValues slices. Index-based access remains safe across pool reallocations
// because Go copies pool contents on grow.
type groupEntry struct {
	aggStateStart int32
	groupValStart int32
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
	groupOrder        []string
	aggStatePool      []aggState
	groupValPool      []OptionalValue
	keyBuf            []byte
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
		for j, gf := range stmt.GroupBy {
			if gf.Name == stmt.Fields[i].Name {
				fieldToGroupByIdx[i] = j
				break
			}
		}
	}

	estGroups := estRows / 10
	if estGroups < 16 {
		estGroups = 16
	} else if estGroups > 4096 {
		estGroups = 4096
	}

	return &groupByAccumulator{
		aggregates:        stmt.Aggregates,
		useIntSum:         useIntSum,
		groupByColIdx:     groupByColIdx,
		aggColIdx:         aggColIdx,
		fieldToGroupByIdx: fieldToGroupByIdx,
		groupMap:          make(map[string]int32, estGroups),
		groupEntries:      make([]groupEntry, 0, estGroups),
		groupOrder:        make([]string, 0, estGroups),
		aggStatePool:      make([]aggState, 0, estGroups*numAggs),
		groupValPool:      make([]OptionalValue, 0, estGroups*numGroupBy),
	}
}

// process accumulates one row into the group state. Safe to call with a
// reused Row.Values buffer as long as the caller does not retain the row
// after returning — values needed for grouping are copied into groupValPool.
func (acc *groupByAccumulator) process(row Row) {
	acc.keyBuf = buildGroupKey(acc.keyBuf[:0], row, acc.groupByColIdx)

	gsIdx, exists := acc.groupMap[string(acc.keyBuf)]
	if !exists {
		key := string(acc.keyBuf) // one alloc per new group

		aggStart := int32(len(acc.aggStatePool))
		for i := range len(acc.aggregates) {
			acc.aggStatePool = append(acc.aggStatePool, aggState{useIntSum: acc.useIntSum[i]})
		}

		gvStart := int32(len(acc.groupValPool))
		for _, colIdx := range acc.groupByColIdx {
			if colIdx >= 0 && colIdx < len(row.Values) {
				acc.groupValPool = append(acc.groupValPool, row.Values[colIdx])
			} else {
				acc.groupValPool = append(acc.groupValPool, OptionalValue{})
			}
		}

		gsIdx = int32(len(acc.groupEntries))
		acc.groupEntries = append(acc.groupEntries, groupEntry{
			aggStateStart: aggStart,
			groupValStart: gvStart,
		})
		acc.groupMap[key] = gsIdx
		acc.groupOrder = append(acc.groupOrder, key)
	}

	aggBase := int(acc.groupEntries[gsIdx].aggStateStart)
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
			if !acc.aggStatePool[aggBase+i].hasValue || compareValues(val, acc.aggStatePool[aggBase+i].min) < 0 {
				acc.aggStatePool[aggBase+i].min = val
				acc.aggStatePool[aggBase+i].hasValue = true
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
			if !acc.aggStatePool[aggBase+i].hasValue || compareValues(val, acc.aggStatePool[aggBase+i].max) > 0 {
				acc.aggStatePool[aggBase+i].max = val
				acc.aggStatePool[aggBase+i].hasValue = true
			}
		}
	}
}

func (t *Table) selectGroupBy(ctx context.Context, stmt Statement, rows []Row) (StatementResult, error) {
	_ = ctx // ctx kept for signature compat; no blocking ops remain
	acc := newGroupByAccumulator(stmt, t, len(rows))
	for _, row := range rows {
		acc.process(row)
	}
	return acc.buildResult(stmt, t)
}

// selectGroupByZeroAlloc handles GROUP BY over a single sequential scan without
// materialising rows. It reuses one []OptionalValue buffer across all rows,
// eliminating the per-row heap allocation from UnmarshalWithMask. Falls back to
// the general path for virtual tables and parallel scans.
func (t *Table) selectGroupByZeroAlloc(ctx context.Context, stmt Statement, scan Scan, selectedFields []Field) (StatementResult, error) {
	if t.virtualRows != nil || t.parallelScan {
		var rows []Row
		if err := t.sequentialScan(ctx, scan, selectedFields, func(row Row) error {
			rows = append(rows, row)
			return nil
		}); err != nil {
			return StatementResult{}, err
		}
		return t.selectGroupBy(ctx, stmt, rows)
	}

	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}

	fullMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)

	// Pre-allocate one reusable values buffer. Safe because process() copies the
	// GROUP BY column values it needs into groupValPool before returning — the
	// buffer is overwritten on the next row.
	reuseValues := make([]OptionalValue, len(t.Columns))

	estRows := int(t.estimatedRowCount())
	if estRows <= 0 {
		estRows = 160 // conservative default (estGroups = 16)
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

		row := t.newRow()
		row, err = row.unmarshalWithMaskInto(cell, fullMask, reuseValues)
		if err != nil {
			return StatementResult{}, err
		}

		if tableFilter != nil {
			ok, err := tableFilter(row)
			if err != nil {
				return StatementResult{}, err
			}
			if !ok {
				continue
			}
		}

		acc.process(row)
	}

	return acc.buildResult(stmt, t)
}

func (acc *groupByAccumulator) buildResult(stmt Statement, t *Table) (StatementResult, error) {
	nFields := len(stmt.Fields)
	nGroups := len(acc.groupOrder)

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

	// Preallocate one flat block for all result values — one alloc covers every group.
	allResultValues := make([]OptionalValue, nGroups*nFields)
	resultRows := make([]Row, 0, nGroups)

	for gi, key := range acc.groupOrder {
		gsIdx := acc.groupMap[key]
		aggBase := int(acc.groupEntries[gsIdx].aggStateStart)
		gvBase := int(acc.groupEntries[gsIdx].groupValStart)

		values := allResultValues[gi*nFields : (gi+1)*nFields]

		for i, agg := range acc.aggregates {
			switch agg.Kind {
			case 0:
				if j := acc.fieldToGroupByIdx[i]; j >= 0 {
					values[i] = acc.groupValPool[gvBase+j]
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
				}
			case AggregateAvg:
				st := acc.aggStatePool[aggBase+i]
				if st.hasValue && st.count > 0 {
					if st.useIntSum {
						values[i] = OptionalValue{Valid: true, Value: float64(st.sumI) / float64(st.count)}
					} else {
						values[i] = OptionalValue{Valid: true, Value: st.sumF / float64(st.count)}
					}
				}
			case AggregateMin:
				if acc.aggStatePool[aggBase+i].hasValue {
					values[i] = acc.aggStatePool[aggBase+i].min
				}
			case AggregateMax:
				if acc.aggStatePool[aggBase+i].hasValue {
					values[i] = acc.aggStatePool[aggBase+i].max
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

func (t *Table) selectStreamingDirectRowView(
	ctx context.Context,
	stmt Statement,
	plan QueryPlan,
	requestedFields []Field,
) (StatementResult, bool, error) {
	if t.virtualRows != nil || t.parallelScan || len(plan.Scans) != 1 {
		return StatementResult{}, false, nil
	}
	fieldIndexes, resultColumns, ok := rowViewProjectionPlan(t.Columns, requestedFields)
	if !ok {
		return StatementResult{}, false, nil
	}
	if stmt.Distinct {
		return StatementResult{}, false, nil
	}

	result := StatementResult{Columns: resultColumns}
	scan := plan.Scans[0]
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

	var newRowViewIter func() RowViewIterator
	switch scan.Type {
	case ScanTypeSequential:
		iterFactory, err := t.sequentialRowViewIteratorFactory(ctx, tableFilter, remaining, offset, hasLimit, hasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	case ScanTypeIndexAll, ScanTypeIndexRange:
		if scan.CoveringIndex {
			return StatementResult{}, false, nil
		}
		iterFactory, err := t.indexRowViewIteratorFactory(ctx, plan, scan, tableFilter, remaining, offset, hasLimit, hasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	case ScanTypeIndexPoint:
		if scan.CoveringIndex {
			return StatementResult{}, false, nil
		}
		if t.isUniquePointIndex(scan.IndexName) {
			iterFactory, err := t.uniqueIndexPointRowViewIteratorFactory(scan, tableFilter, remaining, offset, hasLimit, hasOffset)
			if err != nil {
				return StatementResult{}, true, err
			}
			newRowViewIter = iterFactory
			break
		}
		iterFactory, err := t.indexRowViewIteratorFactory(ctx, plan, scan, tableFilter, remaining, offset, hasLimit, hasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	case ScanTypeIndexIntersect, ScanTypeIndexUnion:
		iterFactory, err := t.indexSetRowViewIteratorFactory(ctx, scan, tableFilter, remaining, offset, hasLimit, hasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	case ScanTypeFullText:
		iterFactory, ok, err := t.fullTextRowViewIteratorFactory(scan, tableFilter, remaining, offset, hasLimit, hasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		if !ok {
			return StatementResult{}, false, nil
		}
		newRowViewIter = iterFactory
	case ScanTypeInverted:
		iterFactory, err := t.invertedRowViewIteratorFactory(ctx, scan, tableFilter, remaining, offset, hasLimit, hasOffset)
		if err != nil {
			return StatementResult{}, true, err
		}
		newRowViewIter = iterFactory
	default:
		return StatementResult{}, false, nil
	}

	result.RowViews = newRowViewIter()
	result.RowViewPager = t.pager
	result.RowViewFieldIndexes = fieldIndexes
	result.Rows = rowViewMaterializingIterator(ctx, t.pager, newRowViewIter(), fieldIndexes, resultColumns)
	return result, true, nil
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

	rowIDs, err := t.collectIndexScanRowIDs(ctx, plan, scan, tableFilter == nil)
	if err != nil {
		return nil, err
	}
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
	}, nil
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
		return nil, false, nil
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
	rowIDs, err := t.collectInvertedScanRowIDs(ctx, scan)
	if err != nil {
		return nil, err
	}
	return t.rowIDRowViewIteratorFactory(rowIDs, tableFilter, remaining, offset, hasLimit, hasOffset), nil
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

func (t *Table) collectIndexScanRowIDs(ctx context.Context, plan QueryPlan, scan Scan, canApplyScanLimit bool) ([]RowID, error) {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return nil, fmt.Errorf("no index found for row view scan: %s", scan.IndexName)
	}
	rowIDs := make([]RowID, 0, rowIDBufferCapacity(scan, canApplyScanLimit))
	var emitted int64
	appendRowID := func(_ any, rowID RowID) error {
		rowIDs = append(rowIDs, rowID)
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
		if err := idx.ScanAll(ctx, plan.SortReverse, appendRowID); err != nil && !errors.Is(err, errLimitReached) {
			return nil, err
		}
	case ScanTypeIndexRange:
		if err := idx.ScanRange(ctx, scan.RangeCondition, plan.SortReverse, appendRowID); err != nil && !errors.Is(err, errLimitReached) {
			return nil, err
		}
	case ScanTypeIndexPoint:
		for _, indexValue := range scan.IndexKeys {
			err := idx.VisitRowIDs(ctx, indexValue, func(rowID RowID) error {
				rowIDs = append(rowIDs, rowID)
				if canApplyScanLimit && scan.ScanLimit > 0 {
					emitted += 1
					if emitted >= scan.ScanLimit {
						return errLimitReached
					}
				}
				return ctx.Err()
			})
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if errors.Is(err, errLimitReached) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("index lookup failed: %w", err)
			}
			if canApplyScanLimit && scan.ScanLimit > 0 && emitted >= scan.ScanLimit {
				break
			}
		}
	default:
		return nil, fmt.Errorf("unsupported row view index scan type: %s", scan.Type)
	}
	return rowIDs, nil
}

func rowIDBufferCapacity(scan Scan, canApplyScanLimit bool) int {
	if canApplyScanLimit && scan.ScanLimit > 0 {
		return int(min(scan.ScanLimit, int64(MaxOverflowRowIDsPerPage)))
	}
	if scan.Type == ScanTypeIndexPoint {
		return len(scan.IndexKeys) * MaxInlineRowIDs
	}
	return 0
}

func (t *Table) rowViewByRowID(ctx context.Context, rowID RowID) (RowView, error) {
	cursor, err := t.Seek(ctx, rowID)
	if err != nil {
		return RowView{}, fmt.Errorf("find row failed: %w", err)
	}
	return cursor.fetchRowView(ctx)
}

func rowViewProjectionPlan(columns []Column, fields []Field) ([]int, []Column, bool) {
	indexes := make([]int, len(fields))
	resultColumns := make([]Column, len(fields))
	for i, field := range fields {
		if field.Expr != nil || field.AliasPrefix != "" {
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

func projectRowView(ctx context.Context, pager TxPager, view RowView, fieldIndexes []int, columns []Column) (Row, error) {
	values := make([]OptionalValue, len(fieldIndexes))
	for i, idx := range fieldIndexes {
		value, err := view.ValueAt(idx)
		if err != nil {
			return Row{}, err
		}
		values[i] = value
	}
	row := NewRowWithValues(columns, values)
	row.Key = view.Key()
	row, err := row.readOverflowTexts(ctx, pager)
	if err != nil {
		return Row{}, fmt.Errorf("row view projection read overflow: %w", err)
	}
	return row, nil
}

func rowViewMaterializingIterator(ctx context.Context, pager TxPager, views RowViewIterator, fieldIndexes []int, columns []Column) Iterator {
	return NewIterator(func(iterCtx context.Context) (Row, error) {
		if !views.Next(iterCtx) {
			if err := views.Err(); err != nil {
				return Row{}, err
			}
			return Row{}, ErrNoMoreRows
		}
		view := views.RowView()
		return projectRowView(ctx, pager, view, fieldIndexes, columns)
	})
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
			offset -= 1
			continue
		}
		projected = append(projected, p)
		if hasLimit {
			limit -= 1
			if limit == 0 {
				break
			}
		}
	}

	result.Rows = NewSliceIterator(projected)
	return result, nil
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

func addOrderByOutputFields(rows []Row, fields []Field, orderBy []OrderBy) ([]Row, error) {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows, nil
	}

	outputFields := make(map[string]Field)
	for _, field := range fields {
		outputFields[field.OutputName()] = field
	}

	for rowIdx, row := range rows {
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
			if field.Expr != nil {
				result, err := field.Expr.Eval(updated)
				if err != nil {
					return nil, fmt.Errorf("evaluating ORDER BY expression %q: %w", field.OutputName(), err)
				}
				if result != nil {
					value = OptionalValue{Value: result, Valid: true}
				}
			} else {
				existing, found := updated.getValueQualified(field.AliasPrefix, field.Name)
				if !found {
					continue
				}
				value = existing
			}

			updated.Columns = append(updated.Columns, Column{Name: field.OutputName()})
			updated.Values = append(updated.Values, value)
		}
		rows[rowIdx] = updated
	}

	return rows, nil
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

	// Lookup each key value and process matching row IDs one at a time.
	// VisitRowIDs reads overflow pages lazily, so a LIMIT sentinel returned by
	// out stops iteration before all overflow pages are loaded.
	for _, indexValue := range scan.IndexKeys {
		err := idx.VisitRowIDs(ctx, indexValue, func(rowID RowID) error {
			var row Row

			switch {
			case scan.CoveringIndex:
				row = rowFromIndexKey(indexValue, scan.IndexColumns, rowID)
			case len(selectedFields) == 0:
				row = NewRowWithValues(t.Columns, nil)
				row.Key = rowID
			default:
				cursor, err := t.Seek(ctx, rowID)
				if err != nil {
					return fmt.Errorf("find row failed: %w", err)
				}

				row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
				if err != nil {
					return fmt.Errorf("fetch row failed: %w", err)
				}

				if tableFilter != nil {
					ok, err := tableFilter(row)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}
			}

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

// indexPointGetAll collects all rows matching the index keys in scan and
// returns them as a slice.  Returns nil when no rows match (zero allocation).
// Unlike indexPointScan, no callback closure is required from the caller,
// which eliminates per-outer-row heap allocations in hot join paths where
// indexPointScan's callback closure would otherwise escape.
func (t *Table) indexPointGetAll(ctx context.Context, scan Scan, selectedFields []Field) ([]Row, error) {
	idx, ok := t.IndexByName(scan.IndexName)
	if !ok {
		return nil, fmt.Errorf("no index found for point scan: %s", scan.IndexName)
	}
	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	coveringFilter := compileScanFilter(scan.IndexColumns, scan.Filters)
	// Extract fields used inside the inner closure so scan does not escape to
	// the heap through the closure — the compiler can then keep scan on the stack.
	isCovering := scan.CoveringIndex
	idxColumns := scan.IndexColumns
	nSelected := len(selectedFields)

	var rows []Row
	for _, indexValue := range scan.IndexKeys {
		if err := idx.VisitRowIDs(ctx, indexValue, func(rowID RowID) error {
			var row Row
			switch {
			case isCovering:
				row = rowFromIndexKey(indexValue, idxColumns, rowID)
			case nSelected == 0:
				row = NewRowWithValues(t.Columns, nil)
				row.Key = rowID
			default:
				cursor, err := t.Seek(ctx, rowID)
				if err != nil {
					return fmt.Errorf("find row failed: %w", err)
				}
				row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
				if err != nil {
					return fmt.Errorf("fetch row failed: %w", err)
				}
				if tableFilter != nil {
					ok, err := tableFilter(row)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}
			}
			if isCovering && coveringFilter != nil {
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
			rows = append(rows, row)
			return nil
		}); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("index lookup failed: %w", err)
		}
	}
	return rows, nil
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
	if len(query.Phrases) == 0 {
		return t.fullTextMultiTermIndexScan(ctx, secondaryIndex, scan, queryTokens, selectedFields, out)
	}

	// Load postings for each term as sorted []invertedPosting — directly from
	// the decoder, no map needed. Each RowID appears in exactly one block
	// (the row-grouped codec writes one entry per document), so postings from
	// successive blocks can be concatenated in order.
	postingsByTerm := make(map[string][]invertedPosting, len(queryTokens))
	for _, key := range scan.IndexKeys {
		term, ok := key.(string)
		if !ok {
			continue
		}
		stats, err := secondaryIndex.InvertedIndex.Stats(ctx, term)
		if err != nil {
			return fmt.Errorf("full-text stats lookup failed: %w", err)
		}
		iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
		if err != nil {
			return fmt.Errorf("full-text lookup failed: %w", err)
		}
		postings := make([]invertedPosting, 0, stats.DocFreq)
		for {
			block, ok, err := iter.NextBlock(ctx)
			if err != nil {
				return fmt.Errorf("full-text lookup failed: %w", err)
			}
			if !ok {
				break
			}
			mode, decoded, err := decodeInvertedPostingList(block.Payload)
			if err != nil {
				return fmt.Errorf("full-text decode failed: %w", err)
			}
			if mode != invertedPostingModePositions {
				return fmt.Errorf("full-text index %s uses posting mode %d", scan.IndexName, mode)
			}
			postings = append(postings, decoded...)
		}
		if len(postings) == 0 {
			return nil
		}
		postingsByTerm[term] = postings
	}

	needsPositions := len(query.Phrases) > 0

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
				return nil
			}
			indices[i] = idx
		}
		phraseMappings[pi] = phraseMapping{indices: indices}
	}

	// allPositions[i] holds the positions of queryTokens[i] in the current
	// candidate document. Allocated once and overwritten for each candidate.
	var allPositions [][]uint32
	if needsPositions {
		allPositions = make([][]uint32, len(queryTokens))
	}

	// Iterate the first term's postings in sorted RowID order. Survivors come
	// out sorted so sortRowIDs is unnecessary.
	firstPostings := postingsByTerm[queryTokens[0]]
	surviving := make([]RowID, 0, len(firstPostings))

	for _, firstPosting := range firstPostings {
		rowID := firstPosting.RowID
		matches := true

		if needsPositions {
			allPositions[0] = firstPosting.Positions
		}
		for i := 1; i < len(queryTokens); i++ {
			termPostings := postingsByTerm[queryTokens[i]]
			idx := invertedPostingBinarySearch(termPostings, rowID)
			if idx < 0 {
				matches = false
				break
			}
			if needsPositions {
				allPositions[i] = termPostings[idx].Positions
			}
		}
		if !matches {
			continue
		}
		if needsPositions {
			for _, pm := range phraseMappings {
				if !textSearchPhraseMatchesSorted(allPositions, pm.indices) {
					matches = false
					break
				}
			}
		}
		if matches {
			surviving = append(surviving, rowID)
		}
	}
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
				return fmt.Errorf("full-text seek: %w", err)
			}
			row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
			if err != nil {
				return fmt.Errorf("full-text fetch: %w", err)
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

func (t *Table) fullTextMultiTermIndexScan(ctx context.Context, secondaryIndex SecondaryIndex, scan Scan, terms []string, selectedFields []Field, out func(Row) error) error {
	termsByDocFreq := make([]string, 0, len(terms))
	docFreqByTerm := make(map[string]uint32, len(terms))
	for _, term := range terms {
		stats, err := secondaryIndex.InvertedIndex.Stats(ctx, term)
		if err != nil {
			return fmt.Errorf("full-text stats lookup failed: %w", err)
		}
		if stats.DocFreq == 0 {
			return nil
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
		rowIDs, err := loadFullTextRowIDsForTerm(ctx, secondaryIndex, scan.IndexName, term, docFreqByTerm[term])
		if err != nil {
			return err
		}
		if len(rowIDs) == 0 {
			return nil
		}
		if i == 0 {
			surviving = rowIDs
			continue
		}
		surviving = intersectTwoSortedSets(surviving, rowIDs)
		if len(surviving) == 0 {
			return nil
		}
	}

	return t.emitFullTextRows(ctx, scan, selectedFields, surviving, out)
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

func (t *Table) emitFullTextRows(ctx context.Context, scan Scan, selectedFields []Field, rowIDs []RowID, out func(Row) error) error {
	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
	for _, rowID := range rowIDs {
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
				return fmt.Errorf("full-text seek: %w", err)
			}
			row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
			if err != nil {
				return fmt.Errorf("full-text fetch: %w", err)
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

func (t *Table) fullTextSingleTermIndexScan(ctx context.Context, secondaryIndex SecondaryIndex, scan Scan, term string, selectedFields []Field, out func(Row) error) error {
	iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
	if err != nil {
		return fmt.Errorf("full-text lookup failed: %w", err)
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)
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

			var row Row
			if len(selectedFields) == 0 {
				row = NewRowWithValues(t.Columns, nil)
				row.Key = rowID
			} else {
				cursor, err := t.Seek(ctx, rowID)
				if err != nil {
					return fmt.Errorf("full-text seek: %w", err)
				}
				row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
				if err != nil {
					return fmt.Errorf("full-text fetch: %w", err)
				}
			}

			if tableFilter != nil {
				ok, err := tableFilter(row)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
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
	surviving, err := t.collectInvertedScanRowIDs(ctx, scan)
	if err != nil {
		return err
	}
	if len(surviving) == 0 {
		return nil
	}

	selectedMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileInvertedScanFilter(t.Columns, scan.Filters)
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
				return fmt.Errorf("inverted seek: %w", err)
			}
			row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
			if err != nil {
				return fmt.Errorf("inverted fetch: %w", err)
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

func (t *Table) collectInvertedScanRowIDs(ctx context.Context, scan Scan) ([]RowID, error) {
	secondaryIndex, ok := t.SecondaryIndexes[scan.IndexName]
	if !ok || secondaryIndex.Method != IndexMethodInverted || secondaryIndex.InvertedIndex == nil {
		return nil, fmt.Errorf("no index found for inverted scan: %s", scan.IndexName)
	}
	if len(scan.IndexKeys) == 0 {
		return nil, nil
	}

	var surviving []RowID
	for i, key := range scan.IndexKeys {
		term, ok := key.(string)
		if !ok {
			continue
		}
		stats, err := secondaryIndex.InvertedIndex.Stats(ctx, term)
		if err != nil {
			return nil, fmt.Errorf("inverted stats lookup failed: %w", err)
		}
		iter, err := secondaryIndex.InvertedIndex.Lookup(ctx, term)
		if err != nil {
			return nil, fmt.Errorf("inverted lookup failed: %w", err)
		}
		rowIDs := make([]RowID, 0, stats.DocFreq)
		for {
			block, ok, err := iter.NextBlock(ctx)
			if err != nil {
				return nil, fmt.Errorf("inverted lookup failed: %w", err)
			}
			if !ok {
				break
			}
			mode, postings, err := decodeInvertedPostingList(block.Payload)
			if err != nil {
				return nil, fmt.Errorf("inverted decode failed: %w", err)
			}
			if mode != invertedPostingModeRowIDs {
				return nil, fmt.Errorf("inverted index %s uses posting mode %d", scan.IndexName, mode)
			}
			for _, posting := range postings {
				rowIDs = append(rowIDs, posting.RowID)
			}
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

// tryCountFromFullTextIndex is a fast-count shortcut for single-term full-text
// COUNT(*) queries with no additional post-scan filters. It reads DocFreq
// directly from the index entry (one B-tree lookup) instead of iterating the
// entire postings list.
func (t *Table) tryCountFromFullTextIndex(ctx context.Context, plan QueryPlan) (StatementResult, bool, error) {
	if len(plan.Scans) != 1 || plan.Scans[0].Type != ScanTypeFullText {
		return StatementResult{}, false, nil
	}
	scan := plan.Scans[0]
	q := scan.FullTextQuery
	if q == nil || len(q.Terms) != 1 || len(q.Phrases) != 0 {
		// Multi-term AND needs intersection; phrases need position checks.
		return StatementResult{}, false, nil
	}
	if len(scan.Filters) > 0 {
		// Additional WHERE predicates require row-level evaluation.
		return StatementResult{}, false, nil
	}
	secondaryIndex, ok := t.SecondaryIndexes[scan.IndexName]
	if !ok || secondaryIndex.Method != IndexMethodFullText || secondaryIndex.InvertedIndex == nil {
		return StatementResult{}, false, nil
	}
	stats, err := secondaryIndex.InvertedIndex.Stats(ctx, q.Terms[0])
	if err != nil {
		return StatementResult{}, false, err
	}
	return countResult(int64(stats.DocFreq)), true, nil
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

	var surviving []RowID
	for i, key := range scan.IndexKeys {
		term, ok := key.(string)
		if !ok {
			continue
		}
		stats, err := secondaryIndex.InvertedIndex.Stats(ctx, term)
		if err != nil {
			return 0, fmt.Errorf("inverted stats lookup failed: %w", err)
		}
		rowIDs, err := loadInvertedRowIDsForTerm(ctx, secondaryIndex, scan.IndexName, term, stats.DocFreq)
		if err != nil {
			return 0, err
		}
		if len(rowIDs) == 0 {
			return 0, nil
		}
		if i == 0 {
			surviving = rowIDs
			continue
		}
		surviving = intersectTwoSortedSets(surviving, rowIDs)
		if len(surviving) == 0 {
			return 0, nil
		}
	}
	return int64(len(surviving)), nil
}

func loadInvertedRowIDsForTerm(ctx context.Context, secondaryIndex SecondaryIndex, indexName, term string, docFreq uint32) ([]RowID, error) {
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
		switch val := v.Value.(type) {
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

// countSequentialScanZeroAlloc counts rows that match the scan filter without
// collecting them. It reuses a single []OptionalValue buffer across all rows,
// eliminating the per-row heap allocation in UnmarshalWithMask. Virtual tables
// and parallel scans fall back to the general sequentialScan path.
func (t *Table) countSequentialScanZeroAlloc(ctx context.Context, scan Scan, selectedFields []Field) (StatementResult, error) {
	if t.virtualRows != nil || t.parallelScan {
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

	cursor, err := t.SeekFirst(ctx)
	if err != nil {
		return StatementResult{}, err
	}

	fullMask := selectedColumnsMask(t.Columns, selectedFields)
	tableFilter := compileScanFilter(t.Columns, scan.Filters)

	// Pre-allocate a single reusable values buffer. Safe because count(*) never
	// retains a row after the predicate check — the buffer is overwritten each row.
	var reuseValues []OptionalValue
	if len(fullMask) > 0 {
		reuseValues = make([]OptionalValue, len(t.Columns))
	}

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
			// Decode into reusable buffer — safe because count never retains the row.
			row := t.newRow()
			row, err = row.unmarshalWithMaskInto(cell, fullMask, reuseValues)
			if err != nil {
				return StatementResult{}, err
			}
			row.Key = cell.Key
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
			if exactTerms {
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
// fetching table rows.  Supported sub-scan types: ScanTypeIndexPoint and ScanTypeIndexRange.
func (t *Table) collectRowIDsFromScan(ctx context.Context, scan Scan) ([]RowID, error) {
	// Intersect: recursively collect and intersect sub-scan RowID sets.
	if scan.Type == ScanTypeIndexIntersect {
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
	sets := make([][]RowID, 0, len(scan.SubScans))
	for _, sub := range scan.SubScans {
		ids, err := t.collectRowIDsFromScan(ctx, sub)
		if err != nil {
			return nil, err
		}
		sets = append(sets, ids)
	}

	switch scan.Type {
	case ScanTypeIndexIntersect:
		return intersectSortedRowIDs(sets), nil
	case ScanTypeIndexUnion:
		return unionSortedRowIDs(sets), nil
	default:
		return nil, fmt.Errorf("unsupported index set scan type: %s", scan.Type)
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
	surviving, err := t.collectIndexSetScanRowIDs(ctx, scan)
	if err != nil {
		return err
	}
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
			i++
			j++
		case a[i] < b[j]:
			emit(a[i])
			i++
		default:
			emit(b[j])
			j++
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

	var emitted int64
	scanLimit := scan.ScanLimit
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
				return fmt.Errorf("union seek: %w", err)
			}
			row, err = cursor.fetchRowWithMask(ctx, false, selectedMask)
			if err != nil {
				return fmt.Errorf("union fetch: %w", err)
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
		emitted++
		if scanLimit > 0 && emitted >= scanLimit {
			return nil
		}
	}
	return nil
}
