package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBoolPragma(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		want    bool
		wantErr bool
	}{
		{"on", true, false},
		{"1", true, false},
		{"true", true, false},
		{"off", false, false},
		{"0", false, false},
		{"false", false, false},
		{"yes", false, true},
		{"", false, true},
		{"ON", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseBoolPragma(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseSynchronousMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		want    SynchronousMode
		wantErr bool
	}{
		{"off", SynchronousOff, false},
		{"0", SynchronousOff, false},
		{"normal", SynchronousNormal, false},
		{"1", SynchronousNormal, false},
		{"full", SynchronousFull, false},
		{"2", SynchronousFull, false},
		{"FULL", 0, true},
		{"fast", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSynchronousMode(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestBoolPragmaResult(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cols := foreignKeysResultColumns

	res := boolPragmaResult(cols, true)
	require.Equal(t, cols, res.Columns)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(1), res.Rows.Row().Values[0].AsAny())

	res = boolPragmaResult(cols, false)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(0), res.Rows.Row().Values[0].AsAny())
}

func TestSynchronousResult(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, mode := range []SynchronousMode{SynchronousOff, SynchronousNormal, SynchronousFull} {
		res := synchronousResult(mode)
		require.Equal(t, synchronousResultColumns, res.Columns)
		require.True(t, res.Rows.Next(ctx))
		assert.Equal(t, int32(mode), res.Rows.Row().Values[0].AsAny())
	}
}

func TestWalCheckpointResult(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	res := walCheckpointResult()
	require.Equal(t, walCheckpointResultColumns, res.Columns)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, "ok", res.Rows.Row().Values[0].AsTextPointer().String())
}

func TestExecuteForeignKeysPragma(t *testing.T) {
	ctx := context.Background()
	pager, dbFile := initTest(t)
	db, err := NewDatabase(ctx, testLogger, dbFile.Name(), nil, pager, pager, nil)
	require.NoError(t, err)

	// Default: enabled.
	res, err := db.executeForeignKeysPragma(Statement{PragmaName: "foreign_keys"})
	require.NoError(t, err)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(1), res.Rows.Row().Values[0].AsAny())

	// Disable.
	res, err = db.executeForeignKeysPragma(Statement{PragmaName: "foreign_keys", PragmaValue: "off"})
	require.NoError(t, err)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(0), res.Rows.Row().Values[0].AsAny())
	assert.False(t, db.foreignKeysEnabled)

	// Re-enable.
	res, err = db.executeForeignKeysPragma(Statement{PragmaName: "foreign_keys", PragmaValue: "on"})
	require.NoError(t, err)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(1), res.Rows.Row().Values[0].AsAny())
	assert.True(t, db.foreignKeysEnabled)

	// Invalid value.
	_, err = db.executeForeignKeysPragma(Statement{PragmaName: "foreign_keys", PragmaValue: "maybe"})
	require.Error(t, err)
}

func TestExecuteParallelScanPragma(t *testing.T) {
	ctx := context.Background()
	pager, dbFile := initTest(t)
	db, err := NewDatabase(ctx, testLogger, dbFile.Name(), nil, pager, pager, nil)
	require.NoError(t, err)

	// Default: disabled.
	res, err := db.executeParallelScanPragma(Statement{PragmaName: "parallel_scan"})
	require.NoError(t, err)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(0), res.Rows.Row().Values[0].AsAny())

	// Enable.
	res, err = db.executeParallelScanPragma(Statement{PragmaName: "parallel_scan", PragmaValue: "on"})
	require.NoError(t, err)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(1), res.Rows.Row().Values[0].AsAny())
	assert.True(t, db.parallelScan)

	// Disable again.
	res, err = db.executeParallelScanPragma(Statement{PragmaName: "parallel_scan", PragmaValue: "off"})
	require.NoError(t, err)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(0), res.Rows.Row().Values[0].AsAny())
	assert.False(t, db.parallelScan)

	// Invalid value.
	_, err = db.executeParallelScanPragma(Statement{PragmaName: "parallel_scan", PragmaValue: "maybe"})
	require.Error(t, err)
}

func TestExecutePragmaStatement_UnknownPragma(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := &Database{}
	_, err := db.executePragmaStatement(ctx, Statement{Kind: Pragma, PragmaName: "unknown_pragma"})
	require.Error(t, err)
	assert.ErrorIs(t, err, errUnknownPragma)
}

func TestIntegrityReportResult(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// OK report emits a single "ok" row.
	okReport := IntegrityReport{}
	res := integrityReportResult("integrity_check", okReport)
	require.Equal(t, pragmaResultColumns, res.Columns)
	require.True(t, res.Rows.Next(ctx))
	row := res.Rows.Row()
	assert.Equal(t, "integrity_check", row.Values[0].AsTextPointer().String())
	assert.Equal(t, "ok", row.Values[1].AsTextPointer().String())
	assert.False(t, res.Rows.Next(ctx))

	// Report with issues emits one row per issue.
	issueReport := IntegrityReport{
		Issues: []IntegrityIssue{
			{Code: "E001", Message: "bad page"},
			{Code: "E002", Message: "orphan page"},
		},
	}
	res = integrityReportResult("integrity_check", issueReport)
	require.True(t, res.Rows.Next(ctx))
	row = res.Rows.Row()
	assert.Equal(t, "E001", row.Values[1].AsTextPointer().String())
	assert.Equal(t, "bad page", row.Values[4].AsTextPointer().String())
	require.True(t, res.Rows.Next(ctx))
	row = res.Rows.Row()
	assert.Equal(t, "E002", row.Values[1].AsTextPointer().String())
	assert.False(t, res.Rows.Next(ctx))
}

func TestExecuteSynchronousPragma(t *testing.T) {
	ctx := context.Background()
	pager, dbFile := initTest(t)
	db, err := NewDatabase(ctx, testLogger, dbFile.Name(), nil, pager, pager, nil)
	require.NoError(t, err)

	// Read (no WAL wired in test DB): returns NormalMode as default.
	res, err := db.executeSynchronousPragma(Statement{PragmaName: "synchronous"})
	require.NoError(t, err)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(SynchronousNormal), res.Rows.Row().Values[0].AsAny())

	// Write invalid value returns error.
	_, err = db.executeSynchronousPragma(Statement{PragmaName: "synchronous", PragmaValue: "fast"})
	require.Error(t, err)

	// Write valid value succeeds.
	res, err = db.executeSynchronousPragma(Statement{PragmaName: "synchronous", PragmaValue: "off"})
	require.NoError(t, err)
	require.True(t, res.Rows.Next(ctx))
	assert.Equal(t, int32(SynchronousOff), res.Rows.Row().Values[0].AsAny())
}
