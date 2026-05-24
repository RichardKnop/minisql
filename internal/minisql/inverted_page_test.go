package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvertedEntryPageHeader_Marshal(t *testing.T) {
	t.Parallel()

	header := invertedEntryPageHeader{
		FormatVersion: invertedPageFormatVersion,
		IsLeaf:        true,
		KeyCount:      7,
		FreeStart:     128,
		FreeEnd:       3900,
		Parent:        2,
		RightChild:    9,
		NextLeaf:      10,
	}
	buf := make([]byte, header.size())
	require.NoError(t, header.Marshal(buf))

	var decoded invertedEntryPageHeader
	require.NoError(t, decoded.Unmarshal(buf))
	assert.Equal(t, header, decoded)

	buf[0] = PageTypeLeaf
	require.ErrorContains(t, decoded.Unmarshal(buf), "invalid page type")
}

func TestInvertedEntryCell_Marshal(t *testing.T) {
	t.Parallel()

	cell := invertedEntryCell{
		Term:         `kv:type:s:"click"`,
		PostingKind:  invertedPostingKindInline,
		CodecVersion: invertedPostingCodecVersion,
		DocFreq:      12,
		PostingCount: 20,
		Child:        44,
		Payload:      []byte{1, 2, 3, 4},
	}
	buf := make([]byte, cell.size())
	require.NoError(t, cell.Marshal(buf))

	var decoded invertedEntryCell
	n, err := decoded.Unmarshal(buf)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(buf)), n)
	assert.Equal(t, cell, decoded)
}

func TestInvertedEntryPage_Marshal(t *testing.T) {
	t.Parallel()

	page := NewInvertedEntryPage(true)
	page.Header.Parent = 4
	page.Cells = []invertedEntryCell{
		{
			Term:         "database",
			PostingKind:  invertedPostingKindInline,
			CodecVersion: invertedPostingCodecVersion,
			DocFreq:      2,
			PostingCount: 3,
			Payload:      []byte{1, 2},
		},
		{
			Term:         "pages",
			PostingKind:  invertedPostingKindTree,
			CodecVersion: invertedPostingCodecVersion,
			DocFreq:      100,
			PostingCount: 150,
			Child:        99,
			Payload:      []byte{3, 4, 5},
		},
	}
	buf := make([]byte, PageSize)
	require.NoError(t, page.Marshal(buf))

	var decoded invertedEntryPage
	require.NoError(t, decoded.Unmarshal(buf))
	assert.Equal(t, page.Header.Parent, decoded.Header.Parent)
	assert.Equal(t, uint16(len(page.Cells)), decoded.Header.KeyCount)
	assert.Equal(t, page.Cells, decoded.Cells)

	clone := decoded.Clone()
	require.NotNil(t, clone)
	clone.Cells[0].Payload[0] = 99
	assert.NotEqual(t, clone.Cells[0].Payload[0], decoded.Cells[0].Payload[0])
}

func TestInvertedPostingPageHeader_Marshal(t *testing.T) {
	t.Parallel()

	header := invertedPostingPageHeader{
		FormatVersion: invertedPageFormatVersion,
		Level:         0,
		ItemCount:     4,
		FreeStart:     96,
		FreeEnd:       4000,
		Parent:        3,
		RightChild:    8,
		NextLeaf:      9,
	}
	buf := make([]byte, header.size())
	require.NoError(t, header.Marshal(buf))

	var decoded invertedPostingPageHeader
	require.NoError(t, decoded.Unmarshal(buf))
	assert.Equal(t, header, decoded)

	buf[1] = 99
	require.ErrorContains(t, decoded.Unmarshal(buf), "unsupported format version")
}

func TestInvertedPostingBlock_Marshal(t *testing.T) {
	t.Parallel()

	block := invertedPostingBlock{
		FirstRowID:   10,
		LastRowID:    99,
		PostingCount: 42,
		CodecVersion: invertedPostingCodecVersion,
		Child:        12,
		Payload:      []byte{9, 8, 7},
	}
	buf := make([]byte, block.size())
	require.NoError(t, block.Marshal(buf))

	var decoded invertedPostingBlock
	n, err := decoded.Unmarshal(buf)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(buf)), n)
	assert.Equal(t, block, decoded)
}

func TestInvertedPostingPage_Marshal(t *testing.T) {
	t.Parallel()

	page := NewInvertedPostingPage(0)
	page.Header.Parent = 8
	page.Blocks = []invertedPostingBlock{
		{
			FirstRowID:   1,
			LastRowID:    10,
			PostingCount: 5,
			CodecVersion: invertedPostingCodecVersion,
			Payload:      []byte{1, 1, 1},
		},
		{
			FirstRowID:   20,
			LastRowID:    40,
			PostingCount: 7,
			CodecVersion: invertedPostingCodecVersion,
			Child:        77,
			Payload:      []byte{2, 2},
		},
	}
	buf := make([]byte, PageSize)
	require.NoError(t, page.Marshal(buf))

	var decoded invertedPostingPage
	require.NoError(t, decoded.Unmarshal(buf))
	assert.Equal(t, page.Header.Parent, decoded.Header.Parent)
	assert.Equal(t, uint16(len(page.Blocks)), decoded.Header.ItemCount)
	assert.Equal(t, page.Blocks, decoded.Blocks)

	clone := decoded.Clone()
	require.NotNil(t, clone)
	clone.Blocks[0].Payload[0] = 99
	assert.NotEqual(t, clone.Blocks[0].Payload[0], decoded.Blocks[0].Payload[0])
}

func TestInvertedMetaPage_Marshal(t *testing.T) {
	t.Parallel()

	page := NewInvertedMetaPage(invertedPostingModePositions, 42)
	page.NextGeneration = 9
	page.Segments = []invertedSegmentDescriptor{
		{
			Generation:   1,
			RootPage:     77,
			PostingCount: 120,
			Kind:         invertedSegmentKindInsert,
		},
		{
			Generation:   2,
			RootPage:     88,
			PostingCount: 7,
			Kind:         invertedSegmentKindDelete,
		},
	}
	buf := make([]byte, PageSize)
	require.NoError(t, page.Marshal(buf))

	var decoded invertedMetaPage
	require.NoError(t, decoded.Unmarshal(buf))
	assert.Equal(t, page.FormatVersion, decoded.FormatVersion)
	assert.Equal(t, page.Mode, decoded.Mode)
	assert.Equal(t, page.BaseRoot, decoded.BaseRoot)
	assert.Equal(t, page.NextGeneration, decoded.NextGeneration)
	assert.Equal(t, page.Segments, decoded.Segments)

	clone := decoded.Clone()
	require.NotNil(t, clone)
	clone.Segments[0].RootPage = 99
	assert.NotEqual(t, clone.Segments[0].RootPage, decoded.Segments[0].RootPage)
}

func TestInvertedMetaPage_UnmarshalRejectsUnknownSegmentKind(t *testing.T) {
	t.Parallel()

	page := NewInvertedMetaPage(invertedPostingModeRowIDs, 12)
	page.Segments = []invertedSegmentDescriptor{{
		Generation:   1,
		RootPage:     99,
		PostingCount: 10,
		Kind:         invertedSegmentKindInsert,
	}}
	buf := make([]byte, PageSize)
	require.NoError(t, page.Marshal(buf))
	buf[page.headerSize()] = 99

	var decoded invertedMetaPage
	require.ErrorContains(t, decoded.Unmarshal(buf), "unknown kind")
}
