package minisql

import (
	"context"
	"testing"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// trivialUnmarshaler is a no-op PageUnmarshaler used in checksum tests where
// the in-memory page representation is irrelevant — we only care about whether
// the checksum gate fires before unmarshaling is attempted.
func trivialUnmarshaler(_ uint32, pageIdx PageIndex, _ []byte) (*Page, error) {
	return &Page{Index: pageIdx, LeafNode: NewLeafNode()}, nil
}

// TestPager_Checksum_CorruptionDetected verifies that bit-flipping a byte inside
// a flushed page returns ErrPageChecksumMismatch on the next DB-file read.
func TestPager_Checksum_CorruptionDetected(t *testing.T) {
	pager, dbFile := initTest(t)

	// Create a root leaf page and flush it so the CRC32 is written to disk.
	aRootLeaf := NewLeafNode()
	aRootLeaf.Header.IsRoot = true
	pager.pages = append(pager.pages, &Page{LeafNode: aRootLeaf})
	pager.totalPages = 1

	ctx := context.Background()
	require.NoError(t, pager.Flush(ctx, 0))

	// Corrupt byte 50 inside page 0 (deep in the B-tree data region, far from
	// the 4-byte checksum stored at bytes 4092-4095).
	_, err := dbFile.WriteAt([]byte{0xFF}, 50)
	require.NoError(t, err)

	// Evict the cached page so the next GetPage reads from disk.
	pager.InvalidatePage(0)

	// The checksum gate must fire before unmarshaling is attempted.
	_, err = pager.GetPage(ctx, 0, trivialUnmarshaler)
	require.Error(t, err)
	assert.ErrorIs(t, err, minisqlErrors.ErrPageChecksumMismatch,
		"expected ErrPageChecksumMismatch, got: %v", err)
}

// TestPager_Checksum_CleanRoundtrip verifies that a page written to disk and
// read back without corruption passes the checksum check silently.
func TestPager_Checksum_CleanRoundtrip(t *testing.T) {
	pager, _ := initTest(t)

	aRootLeaf := NewLeafNode()
	aRootLeaf.Header.IsRoot = true
	pager.pages = append(pager.pages, &Page{LeafNode: aRootLeaf})
	pager.totalPages = 1

	ctx := context.Background()
	require.NoError(t, pager.Flush(ctx, 0))

	// Evict so the next read goes to disk.
	pager.InvalidatePage(0)

	page, err := pager.GetPage(ctx, 0, trivialUnmarshaler)
	require.NoError(t, err)
	assert.NotNil(t, page)
}
