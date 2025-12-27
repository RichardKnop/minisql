package minisql

import (
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
	assert.Equal(t, 0, int(header.NumPages))
}
