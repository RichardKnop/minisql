package minisql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var errExplainUnsupportedStatement = errors.New("EXPLAIN currently supports SELECT statements only")

type explainRow struct {
	operation string
	detail    string
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

	table, ok := d.GetTable(ctx, inner.TableName)
	if !ok {
		return StatementResult{}, fmt.Errorf("%w: %s", errTableDoesNotExist, inner.TableName)
	}

	inner.TableName = table.Name
	inner.Columns = table.Columns

	var err error
	inner, err = inner.Prepare(d.clock())
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

	return buildExplainResult(plan, table, metrics), nil
}

func buildExplainResult(plan QueryPlan, table *Table, metrics map[int]explainMetric) StatementResult {
	rows := plan.explainRows(table)
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
			{Valid: true, Value: NewTextPointer([]byte(row.detail))},
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

func (p QueryPlan) explainRows(table *Table) []explainRow {
	rows := make([]explainRow, 0, len(p.Scans)+len(p.Joins)+1)
	for _, scan := range p.Scans {
		rows = append(rows, explainRow{
			operation: scanOperation(scan),
			detail:    scanDetail(scan),
			estimated: estimateScanRows(table, scan),
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
			if cond.Operand1.Type == OperandField {
				fields = append(fields, cond.Operand1.Value.(Field))
			}
			if cond.Operand2.Type == OperandField {
				fields = append(fields, cond.Operand2.Value.(Field))
			}
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

func scanDetail(scan Scan) string {
	parts := []string{"table=" + scan.TableName}
	if scan.TableAlias != "" && scan.TableAlias != scan.TableName {
		parts = append(parts, "alias="+scan.TableAlias)
	}
	if scan.IndexName != "" {
		parts = append(parts, "index="+scan.IndexName)
	}
	if len(scan.IndexColumns) > 0 {
		parts = append(parts, "columns="+columnNames(scan.IndexColumns))
	}
	if len(scan.IndexKeys) > 0 {
		parts = append(parts, fmt.Sprintf("keys=%v", scan.IndexKeys))
	}
	if scan.RangeCondition.Lower != nil || scan.RangeCondition.Upper != nil {
		parts = append(parts, "range="+rangeDetail(scan.RangeCondition))
	}
	if len(scan.Filters) > 0 {
		parts = append(parts, fmt.Sprintf("filters=%d", conditionCount(scan.Filters)))
	}
	if scan.CoveringIndex {
		parts = append(parts, "covering=true")
	}
	return strings.Join(parts, " ")
}

func joinDetail(plan QueryPlan, join JoinPlan) string {
	parts := []string{"type=" + joinTypeString(join.Type)}
	if join.LeftScanIndex >= 0 && join.LeftScanIndex < len(plan.Scans) {
		parts = append(parts, "left="+plan.Scans[join.LeftScanIndex].TableAlias)
	}
	if join.RightScanIndex >= 0 && join.RightScanIndex < len(plan.Scans) {
		parts = append(parts, "right="+plan.Scans[join.RightScanIndex].TableAlias)
	}
	if len(join.JoinColumnPairs) > 0 {
		pairs := make([]string, 0, len(join.JoinColumnPairs))
		for _, pair := range join.JoinColumnPairs {
			pairs = append(pairs, pair.BaseTableColumn.String()+"="+pair.JoinTableColumn.String())
		}
		parts = append(parts, "on="+strings.Join(pairs, ","))
	} else if join.OuterJoinColumn != "" || join.InnerJoinColumn != "" {
		parts = append(parts, "on="+join.OuterJoinColumn+"="+join.InnerJoinColumn)
	}
	return strings.Join(parts, " ")
}

func joinTypeString(joinType JoinType) string {
	switch joinType {
	case Inner:
		return "inner"
	case Left:
		return "left"
	case Right:
		return "right"
	default:
		return "unknown"
	}
}

func orderByDetail(orderBy []OrderBy) string {
	parts := make([]string, 0, len(orderBy))
	for _, order := range orderBy {
		direction := "ASC"
		if order.Direction == Desc {
			direction = "DESC"
		}
		parts = append(parts, order.Field.String()+" "+direction)
	}
	return "order_by=" + strings.Join(parts, ",")
}

func rangeDetail(condition RangeCondition) string {
	parts := make([]string, 0, 2)
	if condition.Lower != nil {
		op := ">"
		if condition.Lower.Inclusive {
			op = ">="
		}
		parts = append(parts, fmt.Sprintf("%s %v", op, condition.Lower.Value))
	}
	if condition.Upper != nil {
		op := "<"
		if condition.Upper.Inclusive {
			op = "<="
		}
		parts = append(parts, fmt.Sprintf("%s %v", op, condition.Upper.Value))
	}
	return strings.Join(parts, " and ")
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
	}
	return OptionalValue{}
}
