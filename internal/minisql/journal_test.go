package minisql

import (
	"context"
	"hash/crc32"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateJournal(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	aJournal, err := CreateJournal(tempFile.Name(), PageSize)
	require.NoError(t, err)
	require.NoError(t, aJournal.Close())

	assert.Equal(t, tempFile.Name()+"-journal", aJournal.filepath)
	assert.Equal(t, PageSize, int(aJournal.pageSize))

	// Reopen journal file to verify initial header
	journalFile, err := os.Open(aJournal.filepath)
	require.NoError(t, err)
	defer func() { _ = aJournal.Close() }()
	header, err := readJournalHeader(journalFile)
	require.NoError(t, err)

	assert.Equal(t, []byte(JournalMagic), header.Magic[:])
	assert.Equal(t, JournalVersion, header.Version)
	assert.Equal(t, uint32(PageSize), header.PageSize)
	assert.False(t, header.DBHeader)
	assert.Equal(t, 0, int(header.NumPages))
}

func TestJournal_NoDBHeader(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
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
		rootPage, internalPages, leafPages = newTestBtree()
		numPages                           = 1 + len(internalPages) + len(leafPages)
		originalPages                      = make([]*Page, 0, numPages)
	)
	originalPages = append(originalPages, rootPage.Clone())
	for _, page := range internalPages {
		originalPages = append(originalPages, page.Clone())
	}
	for _, page := range leafPages {
		originalPages = append(originalPages, page.Clone())
	}

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	pager.pages = make([]*Page, 0, numPages)
	pager.pages = append(pager.pages, rootPage)
	pager.pages = append(pager.pages, internalPages...)
	pager.pages = append(pager.pages, leafPages...)
	pager.totalPages = uint32(numPages)

	t.Run("create journal without db header change", func(t *testing.T) {
		aJournal, err := CreateJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)

		for _, page := range originalPages {
			err = aJournal.WritePageBefore(context.Background(), page.Index, page)
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
		for _, page := range pager.pages {
			err = pager.Flush(ctx, page.Index)
			require.NoError(t, err)
		}

		// Recreate pager to clear page cache and read from disk
		dbFile.Seek(0, 0)
		pager, err = NewPager(dbFile, PageSize, 1000)
		require.NoError(t, err)

		modifiedLeafPage, err := pager.ForTable(columns).GetPage(ctx, leafPages[0].Index)
		require.NoError(t, err)
		assert.Equal(t, prefixWithLength([]byte("xyz")), modifiedLeafPage.LeafNode.Cells[0].Value)
	})

	t.Run("restore journal without db header", func(t *testing.T) {
		recovered, err := RecoverFromJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)
		assert.True(t, recovered)

		// Recreate pager to clear page cache and read from disk
		dbFile.Seek(0, 0)
		pager, err = NewPager(dbFile, PageSize, 1000)
		require.NoError(t, err)

		tablePager := pager.ForTable(columns)

		// All pages should match original pages
		assert.Equal(t, uint32(numPages), pager.TotalPages())
		for _, page := range originalPages {
			restoredPage, err := tablePager.GetPage(ctx, page.Index)
			require.NoError(t, err)
			assert.Equal(t, page, restoredPage)
		}
	})
}

func TestJournal_WithDBHeader(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
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
		rootPage, internalPages, leafPages = newTestBtree()
		numPages                           = 1 + len(internalPages) + len(leafPages)
		originalPages                      = make([]*Page, 0, numPages)
		originalDBHeader                   = DatabaseHeader{
			FirstFreePage: 42,
			FreePageCount: 100,
		}
	)
	originalPages = append(originalPages, rootPage.Clone())
	for _, page := range internalPages {
		originalPages = append(originalPages, page.Clone())
	}
	for _, page := range leafPages {
		originalPages = append(originalPages, page.Clone())
	}

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	pager.pages = make([]*Page, 0, numPages)
	pager.pages = append(pager.pages, rootPage)
	pager.pages = append(pager.pages, internalPages...)
	pager.pages = append(pager.pages, leafPages...)
	pager.totalPages = uint32(numPages)

	t.Run("create journal with changes including db header", func(t *testing.T) {
		aJournal, err := CreateJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)

		err = aJournal.WriteDBHeaderBefore(ctx, originalDBHeader)
		require.NoError(t, err)

		for _, page := range originalPages {
			err = aJournal.WritePageBefore(context.Background(), page.Index, page)
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
		pager.SaveHeader(ctx, DatabaseHeader{
			FirstFreePage: 84,
			FreePageCount: 200,
		})
		require.NoError(t, err)

		// Flush pages - this will also flush page 0 with modified db header
		for _, page := range pager.pages {
			err = pager.Flush(ctx, page.Index)
			require.NoError(t, err)
		}

		// Recreate pager to clear page cache and read from disk
		dbFile.Seek(0, 0)
		pager, err = NewPager(dbFile, PageSize, 1000)
		require.NoError(t, err)

		modifiedLeafPage, err := pager.ForTable(columns).GetPage(ctx, leafPages[0].Index)
		require.NoError(t, err)
		assert.Equal(t, prefixWithLength([]byte("xyz")), modifiedLeafPage.LeafNode.Cells[0].Value)

		modifiedDBHeader := pager.ForTable(columns).GetHeader(ctx)
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
		pager, err = NewPager(dbFile, PageSize, 1000)
		require.NoError(t, err)

		tablePager := pager.ForTable(columns)

		// All pages should match original pages
		assert.Equal(t, uint32(numPages), pager.TotalPages())
		for _, page := range originalPages {
			restoredPage, err := tablePager.GetPage(ctx, page.Index)
			require.NoError(t, err)
			assert.Equal(t, page, restoredPage)
		}

		// DB header should match original header
		restoredDBHeader := tablePager.GetHeader(ctx)
		assert.Equal(t, originalDBHeader, restoredDBHeader)
	})
}

func TestReadJournalHeader_Validation(t *testing.T) {
	t.Parallel()

	t.Run("truncated header returns error", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		_, err = tempFile.Write([]byte("short"))
		require.NoError(t, err)
		_, err = tempFile.Seek(0, io.SeekStart)
		require.NoError(t, err)

		_, err = readJournalHeader(tempFile)
		require.Error(t, err)
		assert.ErrorContains(t, err, "read header")
	})

	t.Run("invalid magic returns error", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		header := make([]byte, JournalHeaderSize)
		copy(header[0:8], []byte("invalid\n"))
		marshalUint32(header, JournalVersion, 8)
		marshalUint32(header, PageSize, 12)
		marshalBool(header, false, 16)
		marshalUint32(header, 0, 17)
		marshalUint32(header, crc32.ChecksumIEEE(header[0:21]), 21)

		_, err = tempFile.Write(header)
		require.NoError(t, err)
		_, err = tempFile.Seek(0, io.SeekStart)
		require.NoError(t, err)

		_, err = readJournalHeader(tempFile)
		require.Error(t, err)
		assert.ErrorContains(t, err, "invalid journal magic")
	})

	t.Run("checksum mismatch returns error", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		header := make([]byte, JournalHeaderSize)
		copy(header[0:8], []byte(JournalMagic))
		marshalUint32(header, JournalVersion, 8)
		marshalUint32(header, PageSize, 12)
		marshalBool(header, false, 16)
		marshalUint32(header, 1, 17)
		marshalUint32(header, 12345, 21)

		_, err = tempFile.Write(header)
		require.NoError(t, err)
		_, err = tempFile.Seek(0, io.SeekStart)
		require.NoError(t, err)

		_, err = readJournalHeader(tempFile)
		require.Error(t, err)
		assert.ErrorContains(t, err, "checksum mismatch")
	})
}

func TestRecoverFromJournal_ErrorCases(t *testing.T) {
	t.Parallel()

	t.Run("page size mismatch returns error", func(t *testing.T) {
		dbFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(dbFile.Name())

		journal, err := CreateJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)
		require.NoError(t, journal.Close())

		recovered, err := RecoverFromJournal(dbFile.Name(), PageSize/2)
		require.Error(t, err)
		assert.False(t, recovered)
		assert.ErrorContains(t, err, "journal page size mismatch")
	})

	t.Run("truncated journal body returns error", func(t *testing.T) {
		dbFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(dbFile.Name())

		journalPath := dbFile.Name() + "-journal"
		journalFile, err := os.OpenFile(journalPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		require.NoError(t, err)
		defer os.Remove(journalPath)

		header := make([]byte, JournalHeaderSize)
		copy(header[0:8], []byte(JournalMagic))
		marshalUint32(header, JournalVersion, 8)
		marshalUint32(header, PageSize, 12)
		marshalBool(header, false, 16)
		marshalUint32(header, 1, 17)
		marshalUint32(header, crc32.ChecksumIEEE(header[0:21]), 21)

		_, err = journalFile.Write(header)
		require.NoError(t, err)
		_, err = journalFile.Write(marshalUint32(make([]byte, 4), 0, 0))
		require.NoError(t, err)
		// Deliberately omit page body
		require.NoError(t, journalFile.Close())

		recovered, err := RecoverFromJournal(dbFile.Name(), PageSize)
		require.Error(t, err)
		assert.False(t, recovered)
		assert.ErrorContains(t, err, "read page data")
		_, statErr := os.Stat(journalPath)
		assert.NoError(t, statErr, "journal should remain for inspection after failed recovery")
	})

	t.Run("trailing journal data returns error", func(t *testing.T) {
		dbFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(dbFile.Name())

		journal, err := CreateJournal(dbFile.Name(), PageSize)
		require.NoError(t, err)
		require.NoError(t, journal.Finalize(false, 0))
		_, err = journal.file.Write([]byte{0xAA})
		require.NoError(t, err)
		require.NoError(t, journal.Close())
		defer os.Remove(dbFile.Name() + "-journal")

		recovered, err := RecoverFromJournal(dbFile.Name(), PageSize)
		require.Error(t, err)
		assert.False(t, recovered)
		assert.ErrorContains(t, err, "trailing data")
		_, statErr := os.Stat(dbFile.Name() + "-journal")
		assert.NoError(t, statErr, "journal should remain for inspection after failed recovery")
	})

	t.Run("truncated db header payload returns error", func(t *testing.T) {
		dbFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(dbFile.Name())

		journalPath := dbFile.Name() + "-journal"
		journalFile, err := os.OpenFile(journalPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		require.NoError(t, err)
		defer os.Remove(journalPath)

		header := make([]byte, JournalHeaderSize)
		copy(header[0:8], []byte(JournalMagic))
		marshalUint32(header, JournalVersion, 8)
		marshalUint32(header, PageSize, 12)
		marshalBool(header, true, 16)
		marshalUint32(header, 0, 17)
		marshalUint32(header, crc32.ChecksumIEEE(header[0:21]), 21)

		_, err = journalFile.Write(header)
		require.NoError(t, err)
		_, err = journalFile.Write(make([]byte, RootPageConfigSize/2))
		require.NoError(t, err)
		require.NoError(t, journalFile.Close())

		recovered, err := RecoverFromJournal(dbFile.Name(), PageSize)
		require.Error(t, err)
		assert.False(t, recovered)
		assert.ErrorContains(t, err, "read db header")
	})

	t.Run("missing database file returns error", func(t *testing.T) {
		dbFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		dbPath := dbFile.Name()
		require.NoError(t, dbFile.Close())
		require.NoError(t, os.Remove(dbPath))

		journal, err := CreateJournal(dbPath, PageSize)
		require.NoError(t, err)
		require.NoError(t, journal.Close())
		defer os.Remove(dbPath + "-journal")

		recovered, err := RecoverFromJournal(dbPath, PageSize)
		require.Error(t, err)
		assert.False(t, recovered)
		assert.ErrorContains(t, err, "open database for recovery")
	})
}

func TestValidateJournalEOF(t *testing.T) {
	t.Parallel()

	t.Run("clean eof is accepted", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		_, err = tempFile.Write([]byte("ok"))
		require.NoError(t, err)
		_, err = tempFile.Seek(2, io.SeekStart)
		require.NoError(t, err)

		err = validateJournalEOF(tempFile)
		require.NoError(t, err)
	})

	t.Run("extra bytes are rejected", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		_, err = tempFile.Write([]byte("extra"))
		require.NoError(t, err)
		_, err = tempFile.Seek(4, io.SeekStart)
		require.NoError(t, err)

		err = validateJournalEOF(tempFile)
		require.Error(t, err)
		assert.ErrorContains(t, err, "trailing data")
	})
}
