package minisql

import (
	"context"
	"errors"
	"fmt"
)

var errUnknownPragma = errors.New("unknown pragma")

var pragmaResultColumns = []Column{
	{Kind: Text, Name: "check"},
	{Kind: Text, Name: "code"},
	{Kind: Int8, Size: 8, Name: "page", Nullable: true},
	{Kind: Text, Name: "object", Nullable: true},
	{Kind: Text, Name: "message"},
}

func (d *Database) executePragmaStatement(ctx context.Context, stmt Statement) (StatementResult, error) {
	if err := stmt.validatePragma(); err != nil {
		return StatementResult{}, err
	}

	switch stmt.PragmaName {
	case "quick_check":
		report, err := d.QuickCheck(ctx)
		if err != nil {
			return StatementResult{}, err
		}
		return integrityReportResult("quick_check", report), nil
	case "integrity_check":
		report, err := d.IntegrityCheck(ctx)
		if err != nil {
			return StatementResult{}, err
		}
		return integrityReportResult("integrity_check", report), nil
	default:
		return StatementResult{}, fmt.Errorf("%w: %s", errUnknownPragma, stmt.PragmaName)
	}
}

func integrityReportResult(checkName string, report IntegrityReport) StatementResult {
	rows := make([]Row, 0, max(1, len(report.Issues)))
	if report.Ok() {
		rows = append(rows, integrityOKRow(checkName))
	} else {
		for _, issue := range report.Issues {
			rows = append(rows, integrityIssueRow(checkName, issue))
		}
	}

	return StatementResult{
		Columns: pragmaResultColumns,
		Rows:    rowsIterator(rows),
	}
}

func integrityOKRow(checkName string) Row {
	return NewRowWithValues(pragmaResultColumns, []OptionalValue{
		{Value: NewTextPointer([]byte(checkName)), Valid: true},
		{Value: NewTextPointer([]byte("ok")), Valid: true},
		{},
		{},
		{Value: NewTextPointer([]byte("ok")), Valid: true},
	})
}

func integrityIssueRow(checkName string, issue IntegrityIssue) Row {
	row := NewRowWithValues(pragmaResultColumns, []OptionalValue{
		{Value: NewTextPointer([]byte(checkName)), Valid: true},
		{Value: NewTextPointer([]byte(issue.Code)), Valid: true},
		{},
		{},
		{Value: NewTextPointer([]byte(issue.Message)), Valid: true},
	})
	if issue.Page != nil {
		row.Values[2] = OptionalValue{Value: int64(*issue.Page), Valid: true}
	}
	if issue.Object != "" {
		row.Values[3] = OptionalValue{Value: NewTextPointer([]byte(issue.Object)), Valid: true}
	}
	return row
}

func rowsIterator(rows []Row) Iterator {
	var index int
	return NewIterator(func(ctx context.Context) (Row, error) {
		if index >= len(rows) {
			return Row{}, ErrNoMoreRows
		}
		row := rows[index]
		index += 1
		return row, nil
	})
}
