package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvertedPostingCodec_RowIDs(t *testing.T) {
	t.Parallel()

	encoded, err := encodeInvertedPostingList(invertedPostingModeRowIDs, []invertedPosting{
		{RowID: 10},
		{RowID: 3},
		{RowID: 10},
		{RowID: 1000},
	})
	require.NoError(t, err)
	assert.Less(t, len(encoded), 4*8)

	mode, decoded, err := decodeInvertedPostingList(encoded)
	require.NoError(t, err)
	assert.Equal(t, invertedPostingModeRowIDs, mode)
	assert.Equal(t, []invertedPosting{
		{RowID: 3},
		{RowID: 10},
		{RowID: 1000},
	}, decoded)
}

func TestInvertedPostingCodec_Positions(t *testing.T) {
	t.Parallel()

	encoded, err := encodeInvertedPostingList(invertedPostingModePositions, []invertedPosting{
		{RowID: 7, Positions: []uint32{12, 3}},
		{RowID: 2, Positions: []uint32{1}},
		{RowID: 7, Positions: []uint32{3, 20}},
	})
	require.NoError(t, err)

	mode, decoded, err := decodeInvertedPostingList(encoded)
	require.NoError(t, err)
	assert.Equal(t, invertedPostingModePositions, mode)
	assert.Equal(t, []invertedPosting{
		{RowID: 2, Positions: []uint32{1}},
		{RowID: 7, Positions: []uint32{3, 12, 20}},
	}, decoded)
}

func TestForEachInvertedPostingRowIDSkipsPositions(t *testing.T) {
	t.Parallel()

	encoded, err := encodeInvertedPostingList(invertedPostingModePositions, []invertedPosting{
		{RowID: 7, Positions: []uint32{12, 3}},
		{RowID: 2, Positions: []uint32{1}},
		{RowID: 7, Positions: []uint32{20}},
	})
	require.NoError(t, err)

	var rowIDs []RowID
	mode, err := forEachInvertedPostingRowID(encoded, func(rowID RowID) error {
		rowIDs = append(rowIDs, rowID)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, invertedPostingModePositions, mode)
	assert.Equal(t, []RowID{2, 7}, rowIDs)
}

func TestForEachInvertedPostingPositionStreamsPostings(t *testing.T) {
	t.Parallel()

	encoded, err := encodeInvertedPostingList(invertedPostingModePositions, []invertedPosting{
		{RowID: 7, Positions: []uint32{12, 3}},
		{RowID: 2, Positions: []uint32{1}},
		{RowID: 7, Positions: []uint32{20}},
	})
	require.NoError(t, err)

	var postings []invertedPosting
	mode, err := forEachInvertedPostingPosition(encoded, func(rowID RowID, positions []uint32) error {
		postings = append(postings, invertedPosting{
			RowID:     rowID,
			Positions: positions,
		})
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, invertedPostingModePositions, mode)
	assert.Equal(t, []invertedPosting{
		{RowID: 2, Positions: []uint32{1}},
		{RowID: 7, Positions: []uint32{3, 12, 20}},
	}, postings)
}

func TestInvertedPostingCodec_Empty(t *testing.T) {
	t.Parallel()

	encoded, err := encodeInvertedPostingList(invertedPostingModePositions, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte{invertedPostingCodecVersion, byte(invertedPostingModePositions)}, encoded)

	mode, decoded, err := decodeInvertedPostingList(encoded)
	require.NoError(t, err)
	assert.Equal(t, invertedPostingModePositions, mode)
	assert.Empty(t, decoded)
}

func TestInvertedPostingCodec_Invalid(t *testing.T) {
	t.Parallel()

	_, err := encodeInvertedPostingList(invertedPostingMode(99), []invertedPosting{{RowID: 1}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown inverted posting mode")

	_, _, err = decodeInvertedPostingList(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "short buffer")

	_, _, err = decodeInvertedPostingList([]byte{99, byte(invertedPostingModeRowIDs)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported codec version")

	_, _, err = decodeInvertedPostingList([]byte{invertedPostingCodecVersion, 99})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown mode")

	_, _, err = decodeInvertedPostingList([]byte{invertedPostingCodecVersion, byte(invertedPostingModeRowIDs), 0x80})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row delta")
}
