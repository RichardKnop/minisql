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
	case "wal_checkpoint":
		if err := d.Checkpoint(ctx); err != nil {
			return StatementResult{}, fmt.Errorf("WAL checkpoint failed: %w", err)
		}
		return walCheckpointResult(), nil
	case "synchronous":
		return d.executeSynchronousPragma(stmt)
	default:
		return StatementResult{}, fmt.Errorf("%w: %s", errUnknownPragma, stmt.PragmaName)
	}
}

func (d *Database) executeSynchronousPragma(stmt Statement) (StatementResult, error) {
	if stmt.PragmaValue == "" {
		// Read: return current synchronous mode as integer (SQLite convention).
		mode := SynchronousNormal
		if d.wal != nil {
			mode = d.wal.Synchronous()
		}
		return synchronousResult(mode), nil
	}

	// Write: parse and apply the new mode.
	mode, err := parseSynchronousMode(stmt.PragmaValue)
	if err != nil {
		return StatementResult{}, err
	}
	if d.wal != nil {
		d.wal.SetSynchronous(mode)
	}
	return synchronousResult(mode), nil
}

func parseSynchronousMode(s string) (SynchronousMode, error) {
	switch s {
	case "off", "0":
		return SynchronousOff, nil
	case "normal", "1":
		return SynchronousNormal, nil
	case "full", "2":
		return SynchronousFull, nil
	default:
		return 0, fmt.Errorf("invalid synchronous value %q: expected off, normal, or full", s)
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

var walCheckpointResultColumns = []Column{
	{Kind: Text, Name: "status"},
}

var synchronousResultColumns = []Column{
	{Kind: Int4, Size: 4, Name: "synchronous"},
}

func synchronousResult(mode SynchronousMode) StatementResult {
	row := NewRowWithValues(synchronousResultColumns, []OptionalValue{
		{Value: int32(mode), Valid: true},
	})
	return StatementResult{
		Columns: synchronousResultColumns,
		Rows:    rowsIterator([]Row{row}),
	}
}

func walCheckpointResult() StatementResult {
	row := NewRowWithValues(walCheckpointResultColumns, []OptionalValue{
		{Value: NewTextPointer([]byte("ok")), Valid: true},
	})
	return StatementResult{
		Columns: walCheckpointResultColumns,
		Rows:    rowsIterator([]Row{row}),
	}
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
