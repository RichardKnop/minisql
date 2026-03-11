package minisql

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// vacuumColumns is a simple two-column schema used across vacuum tests.
var vacuumColumns = []Column{
	{Kind: Int8, Size: 8, Name: "id"},
	{Kind: Varchar, Size: 100, Name: "name", Nullable: true},
}

// vacuumCreateStmt returns a CREATE TABLE statement for the given table name
// using vacuumColumns.
func vacuumCreateStmt(tableName string) Statement {
	return Statement{
		Kind:      CreateTable,
		TableName: tableName,
		Columns:   append([]Column{}, vacuumColumns...),
	}
}

// newVacuumTestDB creates a fresh temp-file-backed Database with the given
// mockParser.  The file is automatically removed on test cleanup.
func newVacuumTestDB(t *testing.T, aParser Parser) (*Database, string) {
	t.Helper()

	f, err := os.CreateTemp("", "vacuum_test_*.db")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(f.Name())
		os.Remove(f.Name() + ".tmp")
		os.Remove(f.Name() + ".bak")
	})

	aPager, err := NewPager(f, PageSize, PageCacheSize)
	require.NoError(t, err)

	db, err := NewDatabase(context.Background(), testLogger, f.Name(), aParser, aPager, aPager)
	require.NoError(t, err)

	return db, f.Name()
}

// execInTx is a shorthand for running fn inside an auto-commit transaction.
func execInTx(t *testing.T, db *Database, fn func(ctx context.Context)) {
	t.Helper()
	ctx := context.Background()
	err := db.txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
		fn(txCtx)
		return nil
	})
	require.NoError(t, err)
}

// countRowsInDB returns the number of rows in tableName using a direct table
// scan (bypasses the parser/planner entirely).
func countRowsInDB(t *testing.T, db *Database, tableName string) int {
	t.Helper()
	ctx := context.Background()
	tbl, ok := db.tables[tableName]
	require.True(t, ok, "table %q not found", tableName)

	count := 0
	err := db.txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
		result, err := tbl.Select(txCtx, Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(tbl.Columns...),
		})
		if err != nil {
			return err
		}
		for result.Rows.Next(txCtx) {
			_ = result.Rows.Row()
			count++
		}
		return result.Rows.Err()
	})
	require.NoError(t, err)
	return count
}

// insertRowInDB inserts a single row with the given id+name into tableName.
func insertRowInDB(t *testing.T, db *Database, tableName string, id int64, name string) {
	t.Helper()
	ctx := context.Background()
	tbl, ok := db.tables[tableName]
	require.True(t, ok, "table %q not found", tableName)

	err := db.txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
		_, err := tbl.Insert(txCtx, Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(tbl.Columns...),
			Inserts: [][]OptionalValue{{
				{Value: id, Valid: true},
				{Value: NewTextPointer([]byte(name)), Valid: true},
			}},
		})
		return err
	})
	require.NoError(t, err)
}

// ---- Tests ----

func TestVacuum_StatementKindString(t *testing.T) {
	assert.Equal(t, "VACUUM", Vacuum.String())
}

func TestVacuum_EmptyDatabase(t *testing.T) {
	t.Parallel()
	// nil parser is fine: there are no user tables so vacuum never calls Parse.
	db, _ := newVacuumTestDB(t, nil)

	ctx := context.Background()
	require.NoError(t, db.Vacuum(ctx))

	assert.Contains(t, db.ListTableNames(ctx), SchemaTableName)
}

func TestVacuum_TempAndBackupFilesRemovedOnSuccess(t *testing.T) {
	t.Parallel()
	db, fileName := newVacuumTestDB(t, nil)

	ctx := context.Background()
	require.NoError(t, db.Vacuum(ctx))

	_, errTmp := os.Stat(fileName + ".tmp")
	assert.True(t, os.IsNotExist(errTmp), ".tmp file must be removed after successful vacuum")

	_, errBak := os.Stat(fileName + ".bak")
	assert.True(t, os.IsNotExist(errBak), ".bak file must be removed after successful vacuum")
}

func TestVacuum_SingleTableDataPreserved(t *testing.T) {
	t.Parallel()
	const tableName = "items"
	createStmt := vacuumCreateStmt(tableName)

	mockParser := new(MockParser)
	// Both vacuum (schema recreation in temp DB) and Reopen (schema reload)
	// call db.parser.Parse with the table's DDL.
	mockParser.On("Parse", mock.Anything, createStmt.DDL()).Return([]Statement{createStmt}, nil)

	db, _ := newVacuumTestDB(t, mockParser)

	// Create table + insert rows
	execInTx(t, db, func(ctx context.Context) {
		_, err := db.ExecuteStatement(ctx, createStmt)
		require.NoError(t, err)
	})

	for i := int64(1); i <= 5; i++ {
		insertRowInDB(t, db, tableName, i, fmt.Sprintf("row-%d", i))
	}
	require.Equal(t, 5, countRowsInDB(t, db, tableName))

	ctx := context.Background()
	require.NoError(t, db.Vacuum(ctx))

	// All rows must survive vacuum
	assert.Equal(t, 5, countRowsInDB(t, db, tableName))
}

func TestVacuum_DatabaseWritableAfterVacuum(t *testing.T) {
	t.Parallel()
	const tableName = "nums"
	createStmt := vacuumCreateStmt(tableName)

	mockParser := new(MockParser)
	mockParser.On("Parse", mock.Anything, createStmt.DDL()).Return([]Statement{createStmt}, nil)

	db, _ := newVacuumTestDB(t, mockParser)

	execInTx(t, db, func(ctx context.Context) {
		_, err := db.ExecuteStatement(ctx, createStmt)
		require.NoError(t, err)
	})

	insertRowInDB(t, db, tableName, 1, "before")
	require.Equal(t, 1, countRowsInDB(t, db, tableName))

	ctx := context.Background()
	require.NoError(t, db.Vacuum(ctx))

	// Insert after vacuum must work
	insertRowInDB(t, db, tableName, 2, "after")
	assert.Equal(t, 2, countRowsInDB(t, db, tableName))
}

func TestVacuum_MultipleTablesPreserved(t *testing.T) {
	t.Parallel()
	createA := vacuumCreateStmt("table_a")
	createB := vacuumCreateStmt("table_b")

	mockParser := new(MockParser)
	mockParser.On("Parse", mock.Anything, createA.DDL()).Return([]Statement{createA}, nil)
	mockParser.On("Parse", mock.Anything, createB.DDL()).Return([]Statement{createB}, nil)

	db, _ := newVacuumTestDB(t, mockParser)

	execInTx(t, db, func(ctx context.Context) {
		_, err := db.ExecuteStatement(ctx, createA)
		require.NoError(t, err)
	})
	execInTx(t, db, func(ctx context.Context) {
		_, err := db.ExecuteStatement(ctx, createB)
		require.NoError(t, err)
	})

	for i := int64(1); i <= 3; i++ {
		insertRowInDB(t, db, "table_a", i, fmt.Sprintf("a%d", i))
	}
	for i := int64(1); i <= 7; i++ {
		insertRowInDB(t, db, "table_b", i, fmt.Sprintf("b%d", i))
	}

	ctx := context.Background()
	require.NoError(t, db.Vacuum(ctx))

	assert.Equal(t, 3, countRowsInDB(t, db, "table_a"))
	assert.Equal(t, 7, countRowsInDB(t, db, "table_b"))
}

func TestVacuum_RepeatedVacuumsAreIdempotent(t *testing.T) {
	t.Parallel()
	const tableName = "data"
	createStmt := vacuumCreateStmt(tableName)

	mockParser := new(MockParser)
	mockParser.On("Parse", mock.Anything, createStmt.DDL()).Return([]Statement{createStmt}, nil)

	db, _ := newVacuumTestDB(t, mockParser)

	execInTx(t, db, func(ctx context.Context) {
		_, err := db.ExecuteStatement(ctx, createStmt)
		require.NoError(t, err)
	})
	for i := int64(1); i <= 10; i++ {
		insertRowInDB(t, db, tableName, i, fmt.Sprintf("row-%d", i))
	}

	ctx := context.Background()
	for range 3 {
		require.NoError(t, db.Vacuum(ctx), "vacuum %d failed", 3)
	}

	assert.Equal(t, 10, countRowsInDB(t, db, tableName))
}
