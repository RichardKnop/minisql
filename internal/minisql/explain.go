package minisql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

var errExplainUnsupportedStatement = errors.New("EXPLAIN currently supports SELECT statements only")

type explainRow struct {
	operation string
	detail    []byte
	estimated OptionalValue
	actual    OptionalValue
	duration  OptionalValue
}

type explainMetric struct {
	rows       int64
	durationUS int64
}

var explainColumns = []Column{
	{Name: "step", Kind: Int8},
	{Name: "operation", Kind: Text},
	{Name: "detail", Kind: Text},
	{Name: "rows_estimated", Kind: Int8},
	{Name: "rows_actual", Kind: Int8},
	{Name: "duration_us", Kind: Int8},
}

func (d *Database) executeExplain(ctx context.Context, stmt Statement) (StatementResult, error) {
	if stmt.ExplainStatement == nil {
		return StatementResult{}, errors.New("EXPLAIN requires a statement")
	}

	inner := stmt.ExplainStatement.Clone()
	if inner.Kind != Select {
		return StatementResult{}, fmt.Errorf("%w: got %s", errExplainUnsupportedStatement, inner.Kind)
	}

	// UNION/UNION ALL would require explaining multiple SELECT plans; not yet supported.
	if len(inner.Unions) > 0 {
		return StatementResult{}, errors.New("EXPLAIN does not support UNION queries; run EXPLAIN on each SELECT separately")
	}

	// Derived table: FROM (subquery) alias — delegate to specialised handler.
	if inner.FromSubquery != nil {
		return d.executeExplainDerivedTable(ctx, inner, stmt.ExplainAnalyze)
	}

	// WITH … SELECT — CTE statement.
	if len(inner.CTEs) > 0 {
		return d.executeExplainCTEs(ctx, inner, stmt.ExplainAnalyze)
	}

	table, ok := d.GetTable(ctx, inner.TableName)
	if !ok {
		return StatementResult{}, minisqlErrors.ErrNoSuchTable{Name: inner.TableName}
	}

	inner.TableName = table.Name
	inner.Columns = table.Columns

	var err error
	inner, err = inner.Prepare(d.clock())
	if err != nil {
		return StatementResult{}, err
	}

	// Lift eligible IN/NOT IN (subquery) conditions to semi-joins before planning.
	inner = liftINSubqueriesToSemiJoins(inner)

	// Resolve remaining WHERE subqueries before Validate.
	inner.Conditions, err = d.resolveSubqueries(ctx, inner.Conditions)
	if err != nil {
		return StatementResult{}, err
	}

	if err := inner.Validate(table); err != nil {
		return StatementResult{}, err
	}

	plan, err := table.PlanQuery(ctx, inner)
	if err != nil {
		return StatementResult{}, err
	}

	var metrics map[int]explainMetric
	if stmt.ExplainAnalyze {
		metrics, err = table.analyzePlan(ctx, inner, plan)
		if err != nil {
			return StatementResult{}, err
		}
	}

	return buildExplainResult(ctx, plan, table, d.lockedProvider, metrics), nil
}

// executeExplainCTEs handles EXPLAIN [ANALYZE] WITH … SELECT statements.
// Each CTE is materialised in order, emitting a "cte" step per CTE followed
// by the outer query's plan steps. Unused CTEs are pruned and eligible main
// FROM CTEs are inlined before planning, mirroring the execution path.
func (d *Database) executeExplainCTEs(ctx context.Context, inner Statement, analyze bool) (StatementResult, error) {
	// Mirror the execution-path optimisations so EXPLAIN reflects actual behaviour.
	inner = pruneUnusedCTEs(inner)

	for i, cte := range inner.CTEs {
		if cte.Name != inner.TableName {
			continue
		}
		if cteIsInlineable(cte, inner, inner.CTEs) {
			outerAlias := inner.TableAlias
			if outerAlias == "" {
				outerAlias = inner.TableName
			}
			merged := inlineCTE(inner, cte, outerAlias)
			remaining := make([]CTE, 0, len(inner.CTEs)-1)
			remaining = append(remaining, inner.CTEs[:i]...)
			remaining = append(remaining, inner.CTEs[i+1:]...)
			merged.CTEs = remaining
			if len(merged.CTEs) == 0 {
				// All CTEs eliminated — explain as a plain query.
				explainStmt := Statement{
					Kind:             Explain,
					ExplainStatement: &merged,
					ExplainAnalyze:   analyze,
				}
				return d.executeExplain(ctx, explainStmt)
			}
			// Remaining CTEs still need materialisation — recurse.
			return d.executeExplainCTEs(ctx, merged, analyze)
		}
		break
	}

	type cteStep struct {
		name     string
		rowCount int64
		duration int64
	}

	registry := make(map[string]*Table, len(inner.CTEs))
	var cteSteps []cteStep

	for _, cte := range inner.CTEs {
		start := time.Now()
		cteCtx := ctxWithCTERegistry(ctx, registry)
		result, err := d.ExecuteStatement(cteCtx, *cte.Body)
		if err != nil {
			return StatementResult{}, fmt.Errorf("CTE %q: %w", cte.Name, err)
		}
		rows, err := materializeResultRows(cteCtx, result)
		if err != nil {
			return StatementResult{}, fmt.Errorf("CTE %q: reading rows: %w", cte.Name, err)
		}
		dur := time.Since(start).Microseconds()
		vt := newVirtualTable(d.logger, cte.Name, result.Columns, rows)
		vt.provider = d.lockedProvider
		registry[cte.Name] = vt
		cteSteps = append(cteSteps, cteStep{name: cte.Name, rowCount: int64(len(rows)), duration: dur})
	}

	mainCtx := ctxWithCTERegistry(ctx, registry)
	mainStmt := inner
	mainStmt.CTEs = nil

	mainTable, ok := d.GetTable(mainCtx, mainStmt.TableName)
	if !ok {
		return StatementResult{}, minisqlErrors.ErrNoSuchTable{Name: mainStmt.TableName}
	}
	mainStmt.Columns = mainTable.Columns

	var err error
	mainStmt, err = mainStmt.Prepare(d.clock())
	if err != nil {
		return StatementResult{}, err
	}

	mainStmt.Conditions, err = d.resolveSubqueries(mainCtx, mainStmt.Conditions)
	if err != nil {
		return StatementResult{}, err
	}

	if err := mainStmt.Validate(mainTable); err != nil {
		return StatementResult{}, err
	}

	plan, err := mainTable.PlanQuery(mainCtx, mainStmt)
	if err != nil {
		return StatementResult{}, err
	}

	// Build CTE steps (always first) then outer plan steps.
	numCTEs := len(cteSteps)
	planRows := plan.explainRows(ctx, mainTable, d.lockedProvider)
	allRows := make([]explainRow, 0, numCTEs+len(planRows))
	for _, cs := range cteSteps {
		row := explainRow{operation: "cte", detail: append([]byte("name="), cs.name...)}
		if analyze {
			row.actual = OptionalValue{Valid: true, Value: cs.rowCount}
			row.duration = OptionalValue{Valid: true, Value: cs.duration}
		}
		allRows = append(allRows, row)
	}
	allRows = append(allRows, planRows...)

	var outerMetrics map[int]explainMetric
	if analyze {
		outerMetrics, err = mainTable.analyzePlan(mainCtx, mainStmt, plan)
		if err != nil {
			return StatementResult{}, err
		}
	}

	resultRows := make([]Row, 0, len(allRows))
	for idx, row := range allRows {
		step := idx + 1
		// Outer plan metrics are 1-based; shift past the CTE steps.
		if analyze && idx >= numCTEs {
			planStep := idx - numCTEs + 1
			if metric, ok := outerMetrics[planStep]; ok {
				row.actual = OptionalValue{Valid: true, Value: metric.rows}
				row.duration = OptionalValue{Valid: true, Value: metric.durationUS}
			}
		}
		resultRows = append(resultRows, NewRowWithValues(explainColumns, []OptionalValue{
			{Valid: true, Value: int64(step)},
			{Valid: true, Value: NewTextPointer([]byte(row.operation))},
			{Valid: true, Value: NewTextPointer(row.detail)},
			row.estimated,
			row.actual,
			row.duration,
		}))
	}
	return StatementResult{
		Columns: explainColumns,
		Rows:    NewSliceIterator(resultRows),
	}, nil
}

// executeExplainDerivedTable handles EXPLAIN [ANALYZE] SELECT … FROM (subquery) alias.
// It materialises the inner subquery, builds a virtual table, and explains the outer
// query — emitting a leading "derived_table" step followed by the outer plan steps.
func (d *Database) executeExplainDerivedTable(ctx context.Context, inner Statement, analyze bool) (StatementResult, error) {
	start := time.Now()
	innerResult, err := d.ExecuteStatement(ctx, *inner.FromSubquery)
	if err != nil {
		return StatementResult{}, fmt.Errorf("derived table %q: %w", inner.FromSubqueryAlias, err)
	}
	innerRows, err := materializeResultRows(ctx, innerResult)
	if err != nil {
		return StatementResult{}, fmt.Errorf("derived table %q: reading rows: %w", inner.FromSubqueryAlias, err)
	}
	innerDuration := time.Since(start).Microseconds()

	vt := newVirtualTable(d.logger, inner.FromSubqueryAlias, innerResult.Columns, innerRows)

	outer := stripDerivedTableAliasPrefix(inner, inner.FromSubqueryAlias)
	outer.FromSubquery = nil
	outer.TableName = inner.FromSubqueryAlias
	outer.Columns = vt.Columns

	outer, err = outer.Prepare(d.clock())
	if err != nil {
		return StatementResult{}, err
	}

	// Resolve any WHERE subqueries before Validate so validateWhere never sees *Statement operands.
	outer.Conditions, err = d.resolveSubqueries(ctx, outer.Conditions)
	if err != nil {
		return StatementResult{}, err
	}

	if err := outer.Validate(vt); err != nil {
		return StatementResult{}, err
	}

	plan, err := vt.PlanQuery(ctx, outer)
	if err != nil {
		return StatementResult{}, err
	}

	// Build the leading derived_table step (always step 1).
	derivedStep := explainRow{
		operation: "derived_table",
		detail:    append([]byte("alias="), inner.FromSubqueryAlias...),
	}
	if analyze {
		derivedStep.actual = OptionalValue{Valid: true, Value: int64(len(innerRows))}
		derivedStep.duration = OptionalValue{Valid: true, Value: innerDuration}
	}

	planRows := plan.explainRows(ctx, vt, d.lockedProvider)
	allRows := append([]explainRow{derivedStep}, planRows...)

	var outerMetrics map[int]explainMetric
	if analyze {
		outerMetrics, err = vt.analyzePlan(ctx, outer, plan)
		if err != nil {
			return StatementResult{}, err
		}
	}

	resultRows := make([]Row, 0, len(allRows))
	for idx, row := range allRows {
		step := idx + 1
		// Outer plan metrics are 1-based (from analyzePlan); idx maps directly to plan step.
		if analyze && idx > 0 {
			if metric, ok := outerMetrics[idx]; ok {
				row.actual = OptionalValue{Valid: true, Value: metric.rows}
				row.duration = OptionalValue{Valid: true, Value: metric.durationUS}
			}
		}
		resultRows = append(resultRows, NewRowWithValues(explainColumns, []OptionalValue{
			{Valid: true, Value: int64(step)},
			{Valid: true, Value: NewTextPointer([]byte(row.operation))},
			{Valid: true, Value: NewTextPointer(row.detail)},
			row.estimated,
			row.actual,
			row.duration,
		}))
	}
	return StatementResult{
		Columns: explainColumns,
		Rows:    NewSliceIterator(resultRows),
	}, nil
}

func buildExplainResult(ctx context.Context, plan QueryPlan, table *Table, provider TableProvider, metrics map[int]explainMetric) StatementResult {
	rows := plan.explainRows(ctx, table, provider)
	resultRows := make([]Row, 0, len(rows))
	for idx, row := range rows {
		step := idx + 1
		if metric, ok := metrics[step]; ok {
			row.actual = OptionalValue{Valid: true, Value: metric.rows}
			row.duration = OptionalValue{Valid: true, Value: metric.durationUS}
		}
		resultRows = append(resultRows, NewRowWithValues(explainColumns, []OptionalValue{
			{Valid: true, Value: int64(step)},
			{Valid: true, Value: NewTextPointer([]byte(row.operation))},
			{Valid: true, Value: NewTextPointer(row.detail)},
			row.estimated,
			row.actual,
			row.duration,
		}))
	}
	return StatementResult{
		Columns: explainColumns,
		Rows:    NewSliceIterator(resultRows),
	}
}

func (p QueryPlan) explainRows(ctx context.Context, table *Table, provider TableProvider) []explainRow {
	rows := make([]explainRow, 0, len(p.Scans)+len(p.Joins)+1)
	for _, scan := range p.Scans {
		// Use the scan's own table for row-count estimates so join table scans
		// are not incorrectly estimated using the base table's statistics.
		scanTable := table
		if provider != nil && scan.TableName != "" && scan.TableName != table.Name {
			if t, ok := provider.GetTable(ctx, scan.TableName); ok {
				scanTable = t
			}
		}
		rows = append(rows, explainRow{
			operation: scanOperation(scan),
			detail:    scanDetail(scan),
			estimated: estimateScanRows(scanTable, scan),
		})
	}
	for _, join := range p.Joins {
		rows = append(rows, explainRow{
			operation: "join",
			detail:    joinDetail(p, join),
		})
	}
	if p.SortInMemory {
		rows = append(rows, explainRow{
			operation: "sort",
			detail:    orderByDetail(p.OrderBy),
		})
	}
	return rows
}

func (t *Table) analyzePlan(ctx context.Context, stmt Statement, plan QueryPlan) (map[int]explainMetric, error) {
	selectedFields := explainSelectedFields(t, stmt)
	metrics := make(map[int]explainMetric)

	if len(plan.Joins) > 0 {
		step := len(plan.Scans) + 1
		start := time.Now()
		var count int64
		if err := plan.Execute(ctx, t.provider, selectedFields, func(row Row) error {
			count += 1
			return nil
		}); err != nil {
			return nil, err
		}
		metrics[step] = explainMetric{
			rows:       count,
			durationUS: time.Since(start).Microseconds(),
		}
		return metrics, nil
	}

	var rows []Row
	for idx, scan := range plan.Scans {
		step := idx + 1
		start := time.Now()
		var count int64
		if err := t.executeExplainScan(ctx, plan, scan, selectedFields, func(row Row) error {
			count += 1
			if plan.SortInMemory {
				rows = append(rows, row)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		metrics[step] = explainMetric{
			rows:       count,
			durationUS: time.Since(start).Microseconds(),
		}
	}

	if plan.SortInMemory {
		step := len(plan.Scans) + 1
		start := time.Now()
		if err := t.sortRows(rows, plan.OrderBy); err != nil {
			return nil, err
		}
		metrics[step] = explainMetric{
			rows:       int64(len(rows)),
			durationUS: time.Since(start).Microseconds(),
		}
	}

	return metrics, nil
}

func (t *Table) executeExplainScan(ctx context.Context, plan QueryPlan, scan Scan, selectedFields []Field, out func(Row) error) error {
	switch scan.Type {
	case ScanTypeIndexAll:
		return t.indexScanAll(ctx, plan, scan, selectedFields, out)
	case ScanTypeIndexRange:
		return t.indexRangeScan(ctx, plan, scan, selectedFields, out)
	case ScanTypeIndexPoint:
		return t.indexPointScan(ctx, scan, selectedFields, out)
	case ScanTypeIndexFirst:
		return t.indexEndpointScan(ctx, scan, selectedFields, out, false)
	case ScanTypeIndexLast:
		return t.indexEndpointScan(ctx, scan, selectedFields, out, true)
	case ScanTypeSequential:
		return t.sequentialScan(ctx, scan, selectedFields, out)
	case ScanTypeIndexIntersect:
		return t.indexIntersectScan(ctx, scan, selectedFields, out)
	case ScanTypeIndexUnion:
		return t.indexUnionScan(ctx, scan, selectedFields, out)
	case ScanTypeFullText:
		return t.fullTextIndexScan(ctx, scan, selectedFields, out)
	case ScanTypeInverted:
		return t.invertedIndexScan(ctx, scan, selectedFields, out)
	case ScanTypeHNSW:
		return t.hnswIndexScan(ctx, scan, selectedFields, out)
	default:
		return fmt.Errorf("unhandled scan type in EXPLAIN ANALYZE: %d", scan.Type)
	}
}

func explainSelectedFields(t *Table, stmt Statement) []Field {
	switch {
	case stmt.IsSelectAll():
		return fieldsFromColumns(t.Columns...)
	case stmt.IsSelectAggregate():
		colSet := make(map[string]struct{})
		for _, field := range stmt.GroupBy {
			colSet[field.Name] = struct{}{}
		}
		for _, aggregate := range stmt.Aggregates {
			if aggregate.Column != "" {
				colSet[aggregate.Column] = struct{}{}
			}
		}
		fields := make([]Field, 0, len(colSet))
		for colName := range colSet {
			fields = append(fields, Field{Name: colName})
		}
		return appendConditionFields(fields, stmt.Conditions)
	default:
		selectedFields := make([]Field, 0, len(stmt.Fields))
		if !stmt.IsSelectCountAll() {
			selectedFields = exprSourceFields(stmt.Fields)
		}
		return appendConditionFields(selectedFields, stmt.Conditions)
	}
}

func appendConditionFields(fields []Field, conditions OneOrMore) []Field {
	for _, group := range conditions {
		for _, cond := range group {
			fields = appendOperandSourceFields(fields, cond.Operand1)
			fields = appendOperandSourceFields(fields, cond.Operand2)
		}
	}
	return fields
}

func scanOperation(scan Scan) string {
	if scan.CoveringIndex && scan.Type != ScanTypeSequential {
		return "covering_" + scan.Type.String()
	}
	return scan.Type.String()
}

func scanDetail(scan Scan) []byte {
	b := make([]byte, 0, 64)
	if scan.Type == ScanTypeIndexIntersect || scan.Type == ScanTypeIndexUnion {
		sep := byte('+')
		if scan.Type == ScanTypeIndexUnion {
			sep = '|'
		}
		b = append(b, "table="...)
		b = append(b, scan.TableName...)
		b = append(b, " indexes="...)
		for i, sub := range scan.SubScans {
			if i > 0 {
				b = append(b, sep)
			}
			if sub.IndexName != "" {
				b = append(b, sub.IndexName...)
			} else {
				b = append(b, sub.Type.String()...)
			}
		}
		if len(scan.Filters) > 0 {
			b = append(b, " filters="...)
			b = strconv.AppendInt(b, int64(conditionCount(scan.Filters)), 10)
		}
		return b
	}
	b = append(b, "table="...)
	b = append(b, scan.TableName...)
	if scan.TableAlias != "" && scan.TableAlias != scan.TableName {
		b = append(b, " alias="...)
		b = append(b, scan.TableAlias...)
	}
	if scan.IndexName != "" {
		b = append(b, " index="...)
		b = append(b, scan.IndexName...)
	}
	if len(scan.IndexColumns) > 0 {
		b = append(b, " columns="...)
		b = append(b, columnNames(scan.IndexColumns)...)
	}
	if len(scan.IndexKeys) > 0 {
		b = append(b, ' ')
		b = append(b, fmt.Sprintf("keys=%v", scan.IndexKeys)...)
	}
	if scan.RangeCondition.Lower != nil || scan.RangeCondition.Upper != nil {
		b = append(b, " range="...)
		b = append(b, rangeDetail(scan.RangeCondition)...)
	}
	if len(scan.Filters) > 0 {
		b = append(b, " filters="...)
		b = strconv.AppendInt(b, int64(conditionCount(scan.Filters)), 10)
	}
	if scan.CoveringIndex {
		b = append(b, " covering=true"...)
	}
	return b
}

func joinDetail(plan QueryPlan, join JoinPlan) []byte {
	algo := "nested_loop"
	if join.Algorithm == JoinAlgorithmHash {
		algo = "hash"
	}
	b := make([]byte, 0, 64)
	b = append(b, "type="...)
	b = append(b, joinTypeString(join.Type)...)
	b = append(b, " algorithm="...)
	b = append(b, algo...)
	if join.LeftScanIndex >= 0 && join.LeftScanIndex < len(plan.Scans) {
		b = append(b, " left="...)
		b = append(b, plan.Scans[join.LeftScanIndex].TableAlias...)
	}
	if join.RightScanIndex >= 0 && join.RightScanIndex < len(plan.Scans) {
		b = append(b, " right="...)
		b = append(b, plan.Scans[join.RightScanIndex].TableAlias...)
	}
	if len(join.JoinColumnPairs) > 0 {
		b = append(b, " on="...)
		for i, pair := range join.JoinColumnPairs {
			if i > 0 {
				b = append(b, ',')
			}
			b = append(b, pair.BaseTableColumn.String()...)
			b = append(b, '=')
			b = append(b, pair.JoinTableColumn.String()...)
		}
	} else if join.OuterJoinColumn != "" || join.InnerJoinColumn != "" {
		b = append(b, " on="...)
		b = append(b, join.OuterJoinColumn...)
		b = append(b, '=')
		b = append(b, join.InnerJoinColumn...)
	}
	return b
}

func joinTypeString(joinType JoinType) string {
	switch joinType {
	case Inner:
		return "inner"
	case Left:
		return "left"
	case Right:
		return "right"
	case FullOuter:
		return "full outer"
	case Semi:
		return "semi"
	case AntiSemi:
		return "anti_semi"
	default:
		return "unknown"
	}
}

func orderByDetail(orderBy []OrderBy) []byte {
	b := make([]byte, 0, 32)
	b = append(b, "order_by="...)
	for i, order := range orderBy {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, order.Field.String()...)
		if order.Direction == Desc {
			b = append(b, " DESC"...)
		} else {
			b = append(b, " ASC"...)
		}
	}
	return b
}

func rangeDetail(condition RangeCondition) []byte {
	b := make([]byte, 0, 32)
	if condition.Lower != nil {
		if condition.Lower.Inclusive {
			b = append(b, ">="...)
		} else {
			b = append(b, '>')
		}
		b = append(b, ' ')
		b = append(b, fmt.Sprintf("%v", condition.Lower.Value)...)
	}
	if condition.Upper != nil {
		if condition.Lower != nil {
			b = append(b, " and "...)
		}
		if condition.Upper.Inclusive {
			b = append(b, "<="...)
		} else {
			b = append(b, '<')
		}
		b = append(b, ' ')
		b = append(b, fmt.Sprintf("%v", condition.Upper.Value)...)
	}
	return b
}

func conditionCount(conditions OneOrMore) int {
	count := 0
	for _, group := range conditions {
		count += len(group)
	}
	return count
}

func estimateScanRows(table *Table, scan Scan) OptionalValue {
	switch scan.Type {
	case ScanTypeSequential, ScanTypeIndexAll:
		if table.getRowCount != nil {
			return OptionalValue{Valid: true, Value: table.getRowCount()}
		}
	case ScanTypeIndexFirst, ScanTypeIndexLast:
		return OptionalValue{Valid: true, Value: int64(1)}
	case ScanTypeIndexPoint:
		if stats, ok := table.indexStats[scan.IndexName]; ok {
			estimated := estimateFilteredRows(&stats, nil)
			if estimated >= 0 {
				return OptionalValue{Valid: true, Value: estimated * int64(max(1, len(scan.IndexKeys)))}
			}
		}
		if len(scan.IndexKeys) > 0 {
			return OptionalValue{Valid: true, Value: int64(len(scan.IndexKeys))}
		}
	case ScanTypeIndexRange:
		if stats, ok := table.indexStats[scan.IndexName]; ok {
			estimated := estimateFilteredRows(&stats, &scan.RangeCondition)
			if estimated >= 0 {
				return OptionalValue{Valid: true, Value: estimated}
			}
		}
	case ScanTypeFullText:
		if len(scan.IndexKeys) > 0 {
			return OptionalValue{Valid: true, Value: int64(len(scan.IndexKeys))}
		}
	case ScanTypeInverted:
		if len(scan.IndexKeys) > 0 {
			return OptionalValue{Valid: true, Value: int64(len(scan.IndexKeys))}
		}
	case ScanTypeIndexIntersect:
		// Estimate as the minimum across sub-scans (intersection can only shrink the result).
		var minEstimate int64 = -1
		for _, sub := range scan.SubScans {
			est := estimateScanRows(table, sub)
			if !est.Valid {
				continue
			}
			v := est.Value.(int64)
			if minEstimate < 0 || v < minEstimate {
				minEstimate = v
			}
		}
		if minEstimate >= 0 {
			return OptionalValue{Valid: true, Value: minEstimate}
		}
	}
	return OptionalValue{}
}
