package minisql

import (
	"context"
	"encoding/hex"
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
	case "rekey":
		return d.executeRekeyPragma(ctx, stmt)
	case "synchronous":
		return d.executeSynchronousPragma(stmt)
	case "parallel_scan":
		return d.executeParallelScanPragma(stmt)
	case "foreign_keys":
		return d.executeForeignKeysPragma(stmt)
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

var parallelScanResultColumns = []Column{
	{Kind: Int4, Size: 4, Name: "parallel_scan"},
}

var foreignKeysResultColumns = []Column{
	{Kind: Int4, Size: 4, Name: "foreign_keys"},
}

func (d *Database) executeForeignKeysPragma(stmt Statement) (StatementResult, error) {
	if stmt.PragmaValue == "" {
		d.dbLock.RLock()
		enabled := d.foreignKeysEnabled
		d.dbLock.RUnlock()
		return boolPragmaResult(foreignKeysResultColumns, enabled), nil
	}

	enabled, err := parseBoolPragma(stmt.PragmaValue)
	if err != nil {
		return StatementResult{}, err
	}

	d.dbLock.Lock()
	d.foreignKeysEnabled = enabled
	d.dbLock.Unlock()

	return boolPragmaResult(foreignKeysResultColumns, enabled), nil
}

func (d *Database) executeParallelScanPragma(stmt Statement) (StatementResult, error) {
	if stmt.PragmaValue == "" {
		// Read: return current state.
		d.dbLock.RLock()
		enabled := d.parallelScan
		d.dbLock.RUnlock()
		return boolPragmaResult(parallelScanResultColumns, enabled), nil
	}

	enabled, err := parseBoolPragma(stmt.PragmaValue)
	if err != nil {
		return StatementResult{}, err
	}

	d.dbLock.Lock()
	d.parallelScan = enabled
	for _, table := range d.tables {
		table.parallelScan = enabled
	}
	d.dbLock.Unlock()

	return boolPragmaResult(parallelScanResultColumns, enabled), nil
}

func parseBoolPragma(s string) (bool, error) {
	switch s {
	case "on", "1", "true":
		return true, nil
	case "off", "0", "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean pragma value %q: expected on or off", s)
	}
}

func boolPragmaResult(cols []Column, enabled bool) StatementResult {
	v := int32(0)
	if enabled {
		v = 1
	}
	row := NewRowWithValues(cols, []OptionalValue{{Value: v, Valid: true}})
	return StatementResult{
		Columns: cols,
		Rows:    rowsIterator([]Row{row}),
	}
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

var rekeyResultColumns = []Column{
	{Kind: Text, Name: "rekey"},
}

// executeRekeyPragma handles PRAGMA rekey = '<hex-encoded-key>'.
// It re-encrypts the database with the supplied key (key rotation or adding
// encryption).  Pass an empty string to remove encryption.
func (d *Database) executeRekeyPragma(ctx context.Context, stmt Statement) (StatementResult, error) {
	if stmt.PragmaValue == "" {
		return StatementResult{}, fmt.Errorf("PRAGMA rekey: a hex-encoded key value is required; " +
			"to remove encryption use the Go API db.ReKey(ctx, nil)")
	}
	newKey, err := hex.DecodeString(stmt.PragmaValue)
	if err != nil {
		return StatementResult{}, fmt.Errorf("PRAGMA rekey: key must be hex-encoded: %w", err)
	}
	if len(newKey) == 0 {
		return StatementResult{}, fmt.Errorf("PRAGMA rekey: decoded key must not be empty")
	}
	if err := d.ReKey(ctx, newKey); err != nil {
		return StatementResult{}, fmt.Errorf("PRAGMA rekey: %w", err)
	}
	row := NewRowWithValues(rekeyResultColumns, []OptionalValue{
		{Value: NewTextPointer([]byte("ok")), Valid: true},
	})
	return StatementResult{
		Columns: rekeyResultColumns,
		Rows:    rowsIterator([]Row{row}),
	}, nil
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
