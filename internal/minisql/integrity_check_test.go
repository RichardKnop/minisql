package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabase_QuickCheck(t *testing.T) {
	t.Parallel()

	t.Run("healthy database has no issues", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager)
		require.NoError(t, err)

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.True(t, report.Ok())
		assert.NotZero(t, report.CheckedRootPages)
	})

	t.Run("free page count mismatch is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager)
		require.NoError(t, err)

		pager.dbHeader.FreePageCount = 1

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "free_page_count_mismatch")
	})

	t.Run("invalid free list entry is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager)
		require.NoError(t, err)
		userTable := addQuickCheckTestTable(db, pager, "users", 1)
		pager.dbHeader.FirstFreePage = userTable.GetRootPageIdx()
		pager.dbHeader.FreePageCount = 1

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "free_list_page_not_free")
	})

	t.Run("invalid table root page type is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager)
		require.NoError(t, err)

		pager.pages[0] = &Page{
			Index:    0,
			FreePage: &FreePage{},
		}

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "table_root_invalid_type")
	})
}

func issueCodes(report IntegrityReport) []string {
	codes := make([]string, 0, len(report.Issues))
	for _, issue := range report.Issues {
		codes = append(codes, issue.Code)
	}
	return codes
}

func addQuickCheckTestTable(db *Database, pager *pagerImpl, name string, rootPageIdx PageIndex) *Table {
	for len(pager.pages) <= int(rootPageIdx) {
		pager.pages = append(pager.pages, nil)
	}
	pager.pages[rootPageIdx] = &Page{
		Index: rootPageIdx,
		LeafNode: &LeafNode{
			Header: LeafNodeHeader{
				Header: Header{
					IsRoot: true,
				},
			},
		},
	}
	if pager.totalPages <= uint32(rootPageIdx) {
		pager.totalPages = uint32(rootPageIdx) + 1
	}

	columns := []Column{{
		Kind: Int8,
		Size: 8,
		Name: "id",
	}}
	table := NewTable(
		testLogger,
		NewTransactionalPager(pager.ForTable(columns), db.txManager, name, ""),
		db.txManager,
		name,
		columns,
		rootPageIdx,
		db.lockedProvider,
	)
	db.tables[name] = table
	return table
}
