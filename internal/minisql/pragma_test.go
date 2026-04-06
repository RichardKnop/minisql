package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPragmaStatementKindAndValidation(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "PRAGMA", Pragma.String())
	assert.True(t, Statement{Kind: Pragma}.ReadOnly())
	assert.False(t, Statement{Kind: Pragma}.IsDDL())

	err := (Statement{Kind: Pragma}).Validate(nil)
	require.Error(t, err)
	assert.EqualError(t, err, "pragma name is required")

	err = (Statement{Kind: Pragma, PragmaName: "quick_check"}).Validate(nil)
	require.NoError(t, err)
}

func TestIntegrityReportResult(t *testing.T) {
	t.Parallel()

	t.Run("ok result returns a single ok row", func(t *testing.T) {
		result := integrityReportResult("quick_check", IntegrityReport{})
		rows := collectRows(context.Background(), result)
		require.Len(t, rows, 1)
		assert.Equal(t, pragmaResultColumns, result.Columns)
		assert.Equal(t, "quick_check", rows[0].Values[0].Value.(TextPointer).String())
		assert.Equal(t, "ok", rows[0].Values[1].Value.(TextPointer).String())
		assert.False(t, rows[0].Values[2].Valid)
		assert.False(t, rows[0].Values[3].Valid)
		assert.Equal(t, "ok", rows[0].Values[4].Value.(TextPointer).String())
	})

	t.Run("issue result includes page and object when present", func(t *testing.T) {
		page := PageIndex(17)
		result := integrityReportResult("integrity_check", IntegrityReport{
			Issues: []IntegrityIssue{{
				Code:    "orphan_page",
				Message: "page 17 is orphaned",
				Page:    &page,
				Object:  "table users",
			}},
		})
		rows := collectRows(context.Background(), result)
		require.Len(t, rows, 1)
		assert.Equal(t, "integrity_check", rows[0].Values[0].Value.(TextPointer).String())
		assert.Equal(t, "orphan_page", rows[0].Values[1].Value.(TextPointer).String())
		assert.Equal(t, int64(17), rows[0].Values[2].Value)
		assert.Equal(t, "table users", rows[0].Values[3].Value.(TextPointer).String())
		assert.Equal(t, "page 17 is orphaned", rows[0].Values[4].Value.(TextPointer).String())
	})
}

func TestRowsIterator(t *testing.T) {
	t.Parallel()

	row := integrityOKRow("quick_check")
	iter := rowsIterator([]Row{row})

	require.True(t, iter.Next(context.Background()))
	assert.Equal(t, row, iter.Row())
	assert.False(t, iter.Next(context.Background()))
	assert.NoError(t, iter.Err())
}

func TestDatabase_ExecutePragmaStatement(t *testing.T) {
	t.Parallel()

	t.Run("quick_check returns ok row on healthy database", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager)
		require.NoError(t, err)

		var result StatementResult
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			result, err = db.ExecuteStatement(ctx, Statement{Kind: Pragma, PragmaName: "quick_check"})
			return err
		})
		require.NoError(t, err)

		rows := collectRows(context.Background(), result)
		require.Len(t, rows, 1)
		assert.Equal(t, "quick_check", rows[0].Values[0].Value.(TextPointer).String())
		assert.Equal(t, "ok", rows[0].Values[1].Value.(TextPointer).String())
	})

	t.Run("integrity_check returns issues as rows", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager)
		require.NoError(t, err)

		orphanPageIdx := PageIndex(1)
		for len(pager.pages) <= int(orphanPageIdx) {
			pager.pages = append(pager.pages, nil)
		}
		pager.pages[orphanPageIdx] = &Page{
			Index: orphanPageIdx,
			LeafNode: &LeafNode{
				Header: LeafNodeHeader{
					Header: Header{IsRoot: true},
				},
			},
		}
		pager.totalPages = 2

		var result StatementResult
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			result, err = db.ExecuteStatement(ctx, Statement{Kind: Pragma, PragmaName: "integrity_check"})
			return err
		})
		require.NoError(t, err)

		rows := collectRows(context.Background(), result)
		require.NotEmpty(t, rows)
		assert.Contains(t, rowCodes(rows), "orphan_page")
	})

	t.Run("unknown pragma returns error", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager)
		require.NoError(t, err)

		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.ExecuteStatement(ctx, Statement{Kind: Pragma, PragmaName: "mystery"})
			return err
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, errUnknownPragma)
	})
}

func rowCodes(rows []Row) []string {
	codes := make([]string, 0, len(rows))
	for _, row := range rows {
		codes = append(codes, row.Values[1].Value.(TextPointer).String())
	}
	return codes
}
