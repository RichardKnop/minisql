package minisql

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// executeSelectFromDerivedTable handles SELECT … FROM (subquery) alias.
// It executes the inner subquery, materialises its rows, wraps them in a
// virtual *Table, then re-runs the outer SELECT against that table.
func (d *Database) executeSelectFromDerivedTable(ctx context.Context, stmt Statement) (StatementResult, error) {
	// Execute inner subquery.
	innerResult, err := d.ExecuteStatement(ctx, *stmt.FromSubquery)
	if err != nil {
		return StatementResult{}, fmt.Errorf("derived table %q: %w", stmt.FromSubqueryAlias, err)
	}

	// Materialise all rows from the inner result.
	var innerRows []Row
	for innerResult.Rows.Next(ctx) {
		innerRows = append(innerRows, innerResult.Rows.Row())
	}
	if err := innerResult.Rows.Err(); err != nil {
		return StatementResult{}, fmt.Errorf("derived table %q: reading rows: %w", stmt.FromSubqueryAlias, err)
	}

	// Build a virtual table holding the materialised rows.
	vt := newVirtualTable(d.logger, stmt.FromSubqueryAlias, innerResult.Columns, innerRows)

	// Normalise the outer statement: strip the derived-table alias prefix from
	// all field references so that plain column names in the virtual table match.
	outer := stripDerivedTableAliasPrefix(stmt, stmt.FromSubqueryAlias)
	outer.FromSubquery = nil
	outer.TableName = stmt.FromSubqueryAlias

	return vt.Select(ctx, outer)
}

// newVirtualTable creates an in-memory *Table backed by materialised rows.
// It has no pager, no indexes, and no transaction manager — only enough
// scaffolding for Table.Select() to run virtualSequentialScan.
func newVirtualTable(logger *zap.Logger, name string, columns []Column, rows []Row) *Table {
	if rows == nil {
		rows = []Row{}
	}
	cache := make(map[string]int, len(columns))
	for i, col := range columns {
		cache[col.Name] = i
	}
	t := &Table{
		Name:                 name,
		Columns:              columns,
		columnCache:          cache,
		virtualRows:          rows,
		UniqueIndexes:        make(map[string]UniqueIndex),
		SecondaryIndexes:     make(map[string]SecondaryIndex),
		columnIndexInfoCache: make(map[string]IndexInfo),
		indexStats:           make(map[string]IndexStats),
		logger:               logger,
	}
	t.rightmostTablePage.Store(-1)
	t.provider = &singleTableProvider{table: t}
	return t
}

// stripDerivedTableAliasPrefix returns a copy of stmt with alias prefix
// stripped from all field references where AliasPrefix == alias.
// This lets the outer query address virtual-table columns by plain name.
func stripDerivedTableAliasPrefix(stmt Statement, alias string) Statement {
	stmt.Fields = stripFieldsAlias(stmt.Fields, alias)
	stmt.Conditions = stripConditionsAlias(stmt.Conditions, alias)
	stmt.GroupBy = stripFieldsAlias(stmt.GroupBy, alias)
	stmt.Having = stripConditionsAlias(stmt.Having, alias)
	stmt.OrderBy = stripOrderByAlias(stmt.OrderBy, alias)
	stmt.Aggregates = stripAggregatesAlias(stmt.Aggregates, alias)
	return stmt
}

func stripFieldsAlias(fields []Field, alias string) []Field {
	if len(fields) == 0 {
		return fields
	}
	out := make([]Field, len(fields))
	copy(out, fields)
	for i, f := range out {
		if f.AliasPrefix == alias {
			out[i].AliasPrefix = ""
		}
	}
	return out
}

func stripConditionsAlias(conds OneOrMore, alias string) OneOrMore {
	if len(conds) == 0 {
		return conds
	}
	out := make(OneOrMore, len(conds))
	for i, group := range conds {
		out[i] = make(Conditions, len(group))
		copy(out[i], group)
		for j, cond := range out[i] {
			out[i][j] = stripConditionAlias(cond, alias)
		}
	}
	return out
}

func stripConditionAlias(cond Condition, alias string) Condition {
	if cond.Operand1.Type == OperandField {
		f := cond.Operand1.Value.(Field)
		if f.AliasPrefix == alias {
			f.AliasPrefix = ""
			cond.Operand1.Value = f
		}
	}
	if cond.Operand2.Type == OperandField {
		f := cond.Operand2.Value.(Field)
		if f.AliasPrefix == alias {
			f.AliasPrefix = ""
			cond.Operand2.Value = f
		}
	}
	return cond
}

func stripOrderByAlias(orderBy []OrderBy, alias string) []OrderBy {
	if len(orderBy) == 0 {
		return orderBy
	}
	out := make([]OrderBy, len(orderBy))
	copy(out, orderBy)
	for i, ob := range out {
		if ob.Field.AliasPrefix == alias {
			out[i].Field.AliasPrefix = ""
		}
	}
	return out
}

func stripAggregatesAlias(aggs []AggregateExpr, alias string) []AggregateExpr {
	if len(aggs) == 0 {
		return aggs
	}
	out := make([]AggregateExpr, len(aggs))
	copy(out, aggs)
	prefix := alias + "."
	for i, agg := range out {
		if strings.HasPrefix(agg.Column, prefix) {
			out[i].Column = agg.Column[len(prefix):]
		}
	}
	return out
}
