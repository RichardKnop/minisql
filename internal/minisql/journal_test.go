package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateJournal(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", testDbName)
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	aJournal, err := CreateJournal(tempFile.Name(), PageSize)
	require.NoError(t, err)
	aJournal.Close()

	assert.Equal(t, tempFile.Name()+"-journal", aJournal.filepath)
	assert.Equal(t, PageSize, int(aJournal.pageSize))

	// Reopen journal file to verify initial header
	journalFile, err := os.Open(aJournal.filepath)
	require.NoError(t, err)
	defer aJournal.Close()
	header, err := readJournalHeader(journalFile)
	require.NoError(t, err)

	assert.Equal(t, []byte(JournalMagic), header.Magic[:])
	assert.Equal(t, JournalVersion, header.Version)
	assert.Equal(t, uint32(PageSize), header.PageSize)
	assert.False(t, header.DbHeader)
	assert.Equal(t, 0, int(header.NumPages))
}

func TestJournal_NoDBHeader(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDbName)
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())

	var (
		ctx     = context.Background()
		columns = []Column{
			{
				Kind: Varchar,
				Size: MaxInlineVarchar,
				Name: "foo",
			},
		}
		aRootPage, internalPages, leafPages = newTestBtree()
		numPages                            = 1 + len(internalPages) + len(leafPages)
		originalPages                       = make([]*Page, 0, numPages)
	)
	originalPages = append(originalPages, aRootPage.Clone())
	for _, aPage := range internalPages {
		originalPages = append(originalPages, aPage.Clone())
	}
	for _, aPage := range leafPages {
		originalPages = append(originalPages, aPage.Clone())
	}

	aPager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	aPager.pages = make([]*Page, 0, numPages)
	aPager.pages = append(aPager.pages, aRootPage)
	aPager.pages = append(aPager.pages, internalPages...)
	aPager.pages = append(aPager.pages, leafPages...)
	aPager.totalPages = uint32(numPages)

	t.Run("create journal without db header change", func(t *testing.T) {
		aJournal, err := CreateJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)

		for _, aPage := range originalPages {
			err = aJournal.WritePageBefore(context.Background(), aPage.Index, aPage)
			require.NoError(t, err)
		}

		// Sync to disk
		err = aJournal.Finalize(false, numPages)
		require.NoError(t, err)

		err = aJournal.Close()
		require.NoError(t, err)
	})

	t.Run("simulate a partial flush", func(t *testing.T) {
		// Modify one leaf page
		leafPages[0].LeafNode.Cells[0].Value = prefixWithLength([]byte("xyz"))
		// Flush pages
		for _, aPage := range aPager.pages {
			err = aPager.Flush(ctx, aPage.Index)
			require.NoError(t, err)
		}

		// Recreate pager to clear page cache and read from disk
		dbFile.Seek(0, 0)
		aPager, err = NewPager(dbFile, PageSize, 1000)
		require.NoError(t, err)

		modifiedLeafPage, err := aPager.ForTable(columns).GetPage(ctx, leafPages[0].Index)
		require.NoError(t, err)
		assert.Equal(t, prefixWithLength([]byte("xyz")), modifiedLeafPage.LeafNode.Cells[0].Value)
	})

	t.Run("restore journal without db header", func(t *testing.T) {
		recovered, err := RecoverFromJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)
		assert.True(t, recovered)

		// Recreate pager to clear page cache and read from disk
		dbFile.Seek(0, 0)
		aPager, err = NewPager(dbFile, PageSize, 1000)
		require.NoError(t, err)

		tablePager := aPager.ForTable(columns)

		// All pages should match original pages
		assert.Equal(t, uint32(numPages), aPager.TotalPages())
		for _, aPage := range originalPages {
			restoredPage, err := tablePager.GetPage(ctx, aPage.Index)
			require.NoError(t, err)
			assert.Equal(t, aPage, restoredPage)
		}
	})
}

func TestJournal_WithDBHeader(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDbName)
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())

	var (
		ctx     = context.Background()
		columns = []Column{
			{
				Kind: Varchar,
				Size: MaxInlineVarchar,
				Name: "foo",
			},
		}
		aRootPage, internalPages, leafPages = newTestBtree()
		numPages                            = 1 + len(internalPages) + len(leafPages)
		originalPages                       = make([]*Page, 0, numPages)
		originalDBHeader                    = DatabaseHeader{
			FirstFreePage: 42,
			FreePageCount: 100,
		}
	)
	originalPages = append(originalPages, aRootPage.Clone())
	for _, aPage := range internalPages {
		originalPages = append(originalPages, aPage.Clone())
	}
	for _, aPage := range leafPages {
		originalPages = append(originalPages, aPage.Clone())
	}

	aPager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	aPager.pages = make([]*Page, 0, numPages)
	aPager.pages = append(aPager.pages, aRootPage)
	aPager.pages = append(aPager.pages, internalPages...)
	aPager.pages = append(aPager.pages, leafPages...)
	aPager.totalPages = uint32(numPages)

	t.Run("create journal with changes including db header", func(t *testing.T) {
		aJournal, err := CreateJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)

		err = aJournal.WriteDBHeaderBefore(ctx, originalDBHeader)
		require.NoError(t, err)

		for _, aPage := range originalPages {
			err = aJournal.WritePageBefore(context.Background(), aPage.Index, aPage)
			require.NoError(t, err)
		}

		// Sync to disk
		err = aJournal.Finalize(true, numPages)
		require.NoError(t, err)

		err = aJournal.Close()
		require.NoError(t, err)
	})

	t.Run("simulate a partial flush", func(t *testing.T) {
		// Modify one leaf page
		leafPages[0].LeafNode.Cells[0].Value = prefixWithLength([]byte("xyz"))

		// Modify db header
		aPager.SaveHeader(ctx, DatabaseHeader{
			FirstFreePage: 84,
			FreePageCount: 200,
		})
		require.NoError(t, err)

		// Flush pages - this will also flush page 0 with modified db header
		for _, aPage := range aPager.pages {
			err = aPager.Flush(ctx, aPage.Index)
			require.NoError(t, err)
		}

		// Recreate pager to clear page cache and read from disk
		dbFile.Seek(0, 0)
		aPager, err = NewPager(dbFile, PageSize, 1000)
		require.NoError(t, err)

		modifiedLeafPage, err := aPager.ForTable(columns).GetPage(ctx, leafPages[0].Index)
		require.NoError(t, err)
		assert.Equal(t, prefixWithLength([]byte("xyz")), modifiedLeafPage.LeafNode.Cells[0].Value)

		modifiedDBHeader := aPager.ForTable(columns).GetHeader(ctx)
		assert.Equal(t, DatabaseHeader{
			FirstFreePage: 84,
			FreePageCount: 200,
		}, modifiedDBHeader)
	})

	t.Run("restore journal without db header", func(t *testing.T) {
		recovered, err := RecoverFromJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)
		assert.True(t, recovered)

		// Recreate pager to clear page cache and read from disk
		dbFile.Seek(0, 0)
		aPager, err = NewPager(dbFile, PageSize, 1000)
		require.NoError(t, err)

		tablePager := aPager.ForTable(columns)

		// All pages should match original pages
		assert.Equal(t, uint32(numPages), aPager.TotalPages())
		for _, aPage := range originalPages {
			restoredPage, err := tablePager.GetPage(ctx, aPage.Index)
			require.NoError(t, err)
			assert.Equal(t, aPage, restoredPage)
		}

		// DB header should match original header
		restoredDBHeader := tablePager.GetHeader(ctx)
		assert.Equal(t, originalDBHeader, restoredDBHeader)
	})
}
