package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFullTextPostingPackedEncoding(t *testing.T) {
	t.Parallel()

	encoded, err := encodeFullTextPosting(fullTextPosting{RowID: 42, Position: 7})
	require.NoError(t, err)
	assert.Equal(t, fullTextPosting{RowID: 42, Position: 7}, decodeFullTextPosting(encoded))

	_, err = encodeFullTextPosting(fullTextPosting{RowID: RowID(maxFullTextPostingComponent + 1)})
	require.ErrorContains(t, err, "exceeds positional posting limit")
}

func TestFullTextPostingListDeltaEncoding(t *testing.T) {
	t.Parallel()

	postings := []fullTextPosting{
		{RowID: 10, Position: 5},
		{RowID: 1, Position: 2},
		{RowID: 1, Position: 0},
		{RowID: 10, Position: 9},
	}
	encoded, err := encodeFullTextPostingList(postings)
	require.NoError(t, err)
	assert.NotEmpty(t, encoded)

	decoded, err := decodeFullTextPostingList(encoded)
	require.NoError(t, err)
	assert.Equal(t, []fullTextPosting{
		{RowID: 1, Position: 0},
		{RowID: 1, Position: 2},
		{RowID: 10, Position: 5},
		{RowID: 10, Position: 9},
	}, decoded)
}

func TestFullTextPostingListEmptyAndInvalidInput(t *testing.T) {
	t.Parallel()

	encoded, err := encodeFullTextPostingList(nil)
	require.NoError(t, err)
	assert.Nil(t, encoded)

	decoded, err := decodeFullTextPostingList(nil)
	require.NoError(t, err)
	assert.Nil(t, decoded)

	_, err = decodeFullTextPostingList([]byte{0x80})
	require.ErrorContains(t, err, "row delta")
}

func TestCompareFullTextPostings(t *testing.T) {
	t.Parallel()

	assert.Negative(t, compareFullTextPostings(fullTextPosting{RowID: 1, Position: 9}, fullTextPosting{RowID: 2, Position: 0}))
	assert.Negative(t, compareFullTextPostings(fullTextPosting{RowID: 1, Position: 1}, fullTextPosting{RowID: 1, Position: 2}))
	assert.Zero(t, compareFullTextPostings(fullTextPosting{RowID: 1, Position: 1}, fullTextPosting{RowID: 1, Position: 1}))
	assert.Positive(t, compareFullTextPostings(fullTextPosting{RowID: 2, Position: 0}, fullTextPosting{RowID: 1, Position: 9}))
}
