package minisql

import (
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTestPage returns a page-sized byte slice filled with the given byte value.
func makeTestPage(fill byte) []byte {
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = fill
	}
	return data
}

func TestCreateWAL(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	assert.Equal(t, tmp.Name()+"-wal", w.filepath)
	assert.Equal(t, uint32(PageSize), w.pageSize)
	assert.NotZero(t, w.salt1)
	assert.NotZero(t, w.salt2)
	assert.Equal(t, int64(WALFileHeaderSize), w.nextOffset)
}

func TestOpenWAL_NoFile(t *testing.T) {
	t.Parallel()

	w, err := OpenWAL("/tmp/nonexistent_wal_test_db", PageSize)
	require.NoError(t, err)
	assert.Nil(t, w)
}

func TestWAL_FileHeaderRoundtrip(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	salt1, salt2 := w.salt1, w.salt2
	require.NoError(t, w.Close())

	// Reopen and verify salts + checksum survive round-trip.
	w2, err := OpenWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w2.Close()) }()

	assert.Equal(t, salt1, w2.salt1)
	assert.Equal(t, salt2, w2.salt2)
	assert.Equal(t, uint32(PageSize), w2.pageSize)
}

func TestWAL_FileHeader_BadMagic(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	walPath := tmp.Name() + "-wal"
	defer os.Remove(walPath)

	// Write a WAL header with wrong magic.
	walFile, err := os.Create(walPath)
	require.NoError(t, err)
	header := make([]byte, WALFileHeaderSize)
	copy(header[0:8], []byte("wrongmag"))
	_, err = walFile.Write(header)
	require.NoError(t, err)
	require.NoError(t, walFile.Close())

	_, err = OpenWAL(tmp.Name(), PageSize)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid WAL magic")
}

func TestWAL_FileHeader_BadChecksum(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Corrupt the checksum byte.
	walFile, err := os.OpenFile(tmp.Name()+"-wal", os.O_RDWR, 0o644)
	require.NoError(t, err)
	buf := make([]byte, 1)
	_, err = walFile.ReadAt(buf, 24)
	require.NoError(t, err)
	buf[0] ^= 0xFF
	_, err = walFile.WriteAt(buf, 24)
	require.NoError(t, err)
	require.NoError(t, walFile.Close())

	_, err = OpenWAL(tmp.Name(), PageSize)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checksum mismatch")
}

func TestWAL_AppendTransaction_SinglePage(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	page := makeTestPage(0xAB)
	require.NoError(t, w.AppendTransaction([]WALPage{{Index: 3, Data: page}}))

	assert.Equal(t, int64(1), w.FrameCount())

	frames, err := w.ReadAllFrames()
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, PageIndex(3), frames[0].PageIndex)
	assert.Equal(t, page, frames[0].Data)
}

func TestWAL_AppendTransaction_MultiPage(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	pages := []WALPage{
		{Index: 1, Data: makeTestPage(0x11)},
		{Index: 2, Data: makeTestPage(0x22)},
		{Index: 5, Data: makeTestPage(0x55)},
	}
	require.NoError(t, w.AppendTransaction(pages))

	assert.Equal(t, int64(3), w.FrameCount())

	frames, err := w.ReadAllFrames()
	require.NoError(t, err)
	require.Len(t, frames, 3)

	for i, f := range frames {
		assert.Equal(t, pages[i].Index, f.PageIndex)
		assert.Equal(t, pages[i].Data, f.Data)
	}
}

func TestWAL_AppendTransaction_Multiple(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	// Transaction 1: pages 0 and 1.
	tx1 := []WALPage{
		{Index: 0, Data: makeTestPage(0xAA)},
		{Index: 1, Data: makeTestPage(0xBB)},
	}
	require.NoError(t, w.AppendTransaction(tx1))

	// Transaction 2: page 1 overwritten, plus page 2.
	tx2 := []WALPage{
		{Index: 1, Data: makeTestPage(0xCC)},
		{Index: 2, Data: makeTestPage(0xDD)},
	}
	require.NoError(t, w.AppendTransaction(tx2))

	frames, err := w.ReadAllFrames()
	require.NoError(t, err)
	// 2 + 2 = 4 committed frames total.
	assert.Len(t, frames, 4)
}

func TestWAL_ReadAllFrames_PartialTransactionDiscarded(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)

	// Commit transaction 1.
	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 0, Data: makeTestPage(0x01)},
	}))

	// Simulate a crash mid-transaction: write frame header + page data directly
	// without a commit frame.
	frameSize := WALFrameHeaderSize + int(PageSize)
	crashBuf := make([]byte, frameSize)
	fh := crashBuf[:WALFrameHeaderSize]
	marshalUint32(fh, 1, 0) // pageIndex = 1
	marshalUint32(fh, 0, 4) // commitSize = 0 (no commit)
	marshalUint32(fh, w.salt1, 8)
	marshalUint32(fh, w.salt2, 12)
	crc1 := crc32.ChecksumIEEE(fh[0:16])
	marshalUint32(fh, crc1, 16)
	pageBytes := makeTestPage(0xFF)
	crc2 := crc32.ChecksumIEEE(pageBytes)
	marshalUint32(fh, crc2, 20)
	copy(crashBuf[WALFrameHeaderSize:], pageBytes)
	_, err = w.file.WriteAt(crashBuf, w.nextOffset)
	require.NoError(t, err)
	// Do NOT update nextOffset or fsync — simulating crash.

	require.NoError(t, w.Close())

	// Reopen and read frames.
	w2, err := OpenWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w2.Close()) }()

	frames, err := w2.ReadAllFrames()
	require.NoError(t, err)

	// Only the single committed frame from transaction 1 should be visible.
	require.Len(t, frames, 1)
	assert.Equal(t, PageIndex(0), frames[0].PageIndex)
	assert.Equal(t, makeTestPage(0x01), frames[0].Data)
}

func TestWAL_ReadAllFrames_SaltMismatch(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)

	// Write one committed transaction.
	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 0, Data: makeTestPage(0x01)},
	}))

	// Corrupt salt1 in the frame (simulate stale frames from prior WAL incarnation).
	walFile, err := os.OpenFile(tmp.Name()+"-wal", os.O_RDWR, 0o644)
	require.NoError(t, err)
	// Frame starts at WALFileHeaderSize. Salt1 is at offset +8 within frame header.
	saltOffset := int64(WALFileHeaderSize + 8)
	badSalt := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = walFile.WriteAt(badSalt, saltOffset)
	require.NoError(t, err)
	require.NoError(t, walFile.Close())

	require.NoError(t, w.Close())

	w2, err := OpenWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w2.Close()) }()

	frames, err := w2.ReadAllFrames()
	require.NoError(t, err)
	// Salt mismatch stops scanning — no committed frames.
	assert.Empty(t, frames)
}

func TestWAL_ReadAllFrames_CRCMismatch(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)

	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 0, Data: makeTestPage(0x01)},
	}))
	require.NoError(t, w.Close())

	// Corrupt one byte of the page data inside the frame.
	walFile, err := os.OpenFile(tmp.Name()+"-wal", os.O_RDWR, 0o644)
	require.NoError(t, err)
	dataOffset := int64(WALFileHeaderSize + WALFrameHeaderSize) // first byte of page data
	_, err = walFile.WriteAt([]byte{0xFF}, dataOffset)
	require.NoError(t, err)
	require.NoError(t, walFile.Close())

	w2, err := OpenWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w2.Close()) }()

	frames, err := w2.ReadAllFrames()
	require.NoError(t, err)
	// CRC mismatch stops scanning — no committed frames.
	assert.Empty(t, frames)
}

func TestWAL_Checkpoint(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	page0 := makeTestPage(0xAA)
	page1 := makeTestPage(0xBB)
	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 0, Data: page0},
		{Index: 1, Data: page1},
	}))

	// Create a DB file large enough to receive the pages.
	dbFile, err := os.OpenFile(tmp.Name(), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer dbFile.Close()

	// Pre-allocate two pages in the DB file.
	require.NoError(t, dbFile.Truncate(int64(2*PageSize)))

	require.NoError(t, w.Checkpoint(dbFile))

	// Verify the DB file contains the correct page data.
	buf := make([]byte, PageSize)

	_, err = dbFile.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(page0, buf), "page 0 mismatch after checkpoint")

	_, err = dbFile.ReadAt(buf, PageSize)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(page1, buf), "page 1 mismatch after checkpoint")
}

func TestWAL_Checkpoint_LastWriteWins(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	// Transaction 1: write page 0 with 0xAA.
	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 0, Data: makeTestPage(0xAA)},
	}))
	// Transaction 2: overwrite page 0 with 0xBB.
	page0v2 := makeTestPage(0xBB)
	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 0, Data: page0v2},
	}))

	dbFile, err := os.OpenFile(tmp.Name(), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer dbFile.Close()
	require.NoError(t, dbFile.Truncate(int64(PageSize)))

	require.NoError(t, w.Checkpoint(dbFile))

	buf := make([]byte, PageSize)
	_, err = dbFile.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(page0v2, buf), "page 0 should reflect last write")
}

func TestWAL_Truncate(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	originalSalt1, originalSalt2 := w.salt1, w.salt2

	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 0, Data: makeTestPage(0xAA)},
	}))
	assert.Equal(t, int64(1), w.FrameCount())

	require.NoError(t, w.Truncate())

	assert.Equal(t, int64(0), w.FrameCount())
	assert.Equal(t, int64(WALFileHeaderSize), w.nextOffset)

	// Salts must be refreshed after truncation.
	assert.False(t, w.salt1 == originalSalt1 && w.salt2 == originalSalt2,
		"salts should change after truncation")

	// No committed frames after truncation.
	frames, err := w.ReadAllFrames()
	require.NoError(t, err)
	assert.Empty(t, frames)
}

func TestWAL_AppendTransaction_Empty(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	err = w.AppendTransaction(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestWAL_AppendTransaction_WrongPageSize(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	err = w.AppendTransaction([]WALPage{
		{Index: 0, Data: make([]byte, PageSize-1)}, // wrong size
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "data length")
}

func TestWAL_Checkpoint_EmptyWAL(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	dbFile, err := os.OpenFile(tmp.Name(), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer dbFile.Close()

	// Checkpoint on an empty WAL should be a no-op.
	require.NoError(t, w.Checkpoint(dbFile))
}

func TestRecoverFromWAL(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	// Write two committed transactions into the WAL.
	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)

	page0 := makeTestPage(0x10)
	page1 := makeTestPage(0x20)
	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 0, Data: page0},
		{Index: 1, Data: page1},
	}))
	require.NoError(t, w.Close())

	// Prepare DB file.
	dbFile, err := os.OpenFile(tmp.Name(), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer dbFile.Close()
	require.NoError(t, dbFile.Truncate(int64(2*PageSize)))

	recovered, err := RecoverFromWAL(tmp.Name(), dbFile, PageSize)
	require.NoError(t, err)
	assert.True(t, recovered)

	// Verify the DB file was updated.
	buf := make([]byte, PageSize)
	_, err = dbFile.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(page0, buf))

	_, err = dbFile.ReadAt(buf, PageSize)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(page1, buf))

	// WAL should be empty after recovery.
	w2, err := OpenWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	if w2 != nil {
		defer func() { require.NoError(t, w2.Close()) }()
		frames, err := w2.ReadAllFrames()
		require.NoError(t, err)
		assert.Empty(t, frames)
	}
}

// ---------------------------------------------------------------------------
// Write-buffer batching tests
// ---------------------------------------------------------------------------

func TestWAL_WriteBuffering_AccumulatesUntilThreshold(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	frameSize := WALFrameHeaderSize + int(PageSize)
	// Buffer exactly two frames before flushing.
	w.flushThreshold = 2 * frameSize

	// First transaction: below threshold — frames stay in the buffer.
	require.NoError(t, w.AppendTransaction([]WALPage{{Index: 1, Data: makeTestPage(0x01)}}))
	assert.Equal(t, frameSize, w.pendingLen, "frame should be buffered, not yet flushed")
	assert.Equal(t, int64(WALFileHeaderSize), w.nextOffset, "no bytes written to file yet")

	// FrameCount must include the pending frame.
	assert.Equal(t, int64(1), w.FrameCount())

	// ReadAllFrames reads from the file — pending frame is not there yet.
	frames, err := w.ReadAllFrames()
	require.NoError(t, err)
	assert.Empty(t, frames, "frames should not be on disk until threshold is reached")

	// Second transaction: crosses threshold — both frames are flushed together.
	require.NoError(t, w.AppendTransaction([]WALPage{{Index: 2, Data: makeTestPage(0x02)}}))
	assert.Equal(t, 0, w.pendingLen, "buffer should be empty after auto-flush")

	frames, err = w.ReadAllFrames()
	require.NoError(t, err)
	require.Len(t, frames, 2)
	assert.Equal(t, PageIndex(1), frames[0].PageIndex)
	assert.Equal(t, PageIndex(2), frames[1].PageIndex)
}

func TestWAL_WriteBuffering_ExplicitFlush(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	w.flushThreshold = 1 << 20 // 1 MiB — won't auto-flush in this test

	require.NoError(t, w.AppendTransaction([]WALPage{{Index: 0, Data: makeTestPage(0xAA)}}))
	assert.Positive(t, w.pendingLen, "frame should be buffered")

	// Explicit flush makes the frame visible on disk.
	require.NoError(t, w.Flush())
	assert.Equal(t, 0, w.pendingLen)

	frames, err := w.ReadAllFrames()
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, PageIndex(0), frames[0].PageIndex)
	assert.Equal(t, makeTestPage(0xAA), frames[0].Data)
}

func TestWAL_WriteBuffering_CheckpointFlushes(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	w.flushThreshold = 1 << 20 // large — won't auto-flush

	page0 := makeTestPage(0xCC)
	require.NoError(t, w.AppendTransaction([]WALPage{{Index: 0, Data: page0}}))
	assert.Positive(t, w.pendingLen)

	dbFile, err := os.OpenFile(tmp.Name(), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer dbFile.Close()
	require.NoError(t, dbFile.Truncate(int64(PageSize)))

	// Checkpoint must flush pending frames before writing to the DB file.
	require.NoError(t, w.Checkpoint(dbFile))
	assert.Equal(t, 0, w.pendingLen, "checkpoint must flush the write buffer")

	buf := make([]byte, PageSize)
	_, err = dbFile.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, page0, buf, "checkpoint must have written the buffered page")
}

func TestWAL_WriteBuffering_SynchronousFullBypassesBuffer(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	w.flushThreshold = 1 << 20 // large — would buffer in Normal mode
	w.SetSynchronous(SynchronousFull)

	require.NoError(t, w.AppendTransaction([]WALPage{{Index: 0, Data: makeTestPage(0xDD)}}))

	// SynchronousFull must flush + fsync immediately, bypassing the buffer.
	assert.Equal(t, 0, w.pendingLen, "SynchronousFull must flush immediately")

	frames, err := w.ReadAllFrames()
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, PageIndex(0), frames[0].PageIndex)
}

func TestRecoverFromWAL_NoFile(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())
	defer dbFile.Close()

	// No WAL file — recovery should be a clean no-op.
	recovered, err := RecoverFromWAL(dbFile.Name(), dbFile, PageSize)
	require.NoError(t, err)
	assert.False(t, recovered)
}

func TestWAL_FrameHeaderLayout(t *testing.T) {
	t.Parallel()

	// Verify the frame header encoding by manually inspecting bytes written.
	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	defer os.Remove(tmp.Name() + "-wal")

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	pageData := makeTestPage(0x42)
	require.NoError(t, w.AppendTransaction([]WALPage{
		{Index: 7, Data: pageData},
	}))

	// Read the raw frame from the file.
	walFile, err := os.Open(tmp.Name() + "-wal")
	require.NoError(t, err)
	defer walFile.Close()

	_, err = walFile.Seek(WALFileHeaderSize, io.SeekStart)
	require.NoError(t, err)

	fhBuf := make([]byte, WALFrameHeaderSize)
	_, err = io.ReadFull(walFile, fhBuf)
	require.NoError(t, err)

	assert.Equal(t, uint32(7), unmarshalUint32(fhBuf, 0), "page index")
	assert.Equal(t, uint32(1), unmarshalUint32(fhBuf, 4), "commit size (single-page txn = 1)")
	assert.Equal(t, w.salt1, unmarshalUint32(fhBuf, 8), "salt1")
	assert.Equal(t, w.salt2, unmarshalUint32(fhBuf, 12), "salt2")

	expectedCRC1 := crc32.ChecksumIEEE(fhBuf[0:16])
	assert.Equal(t, expectedCRC1, unmarshalUint32(fhBuf, 16), "crc1")

	expectedCRC2 := crc32.ChecksumIEEE(pageData)
	assert.Equal(t, expectedCRC2, unmarshalUint32(fhBuf, 20), "crc2")
}

func TestOpenWALAndRebuildIndex_NoExistingWAL(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		_ = os.Remove(tmp.Name() + "-wal")
	}()

	walIndex := NewWALIndex()
	wal, recovered, err := OpenWALAndRebuildIndex(tmp.Name(), PageSize, walIndex)
	require.NoError(t, err)
	require.NotNil(t, wal)
	defer func() { require.NoError(t, wal.Close()) }()

	// No existing WAL → fresh file, nothing recovered.
	assert.False(t, recovered)
	assert.Equal(t, 0, walIndex.Size())
	assert.Equal(t, int64(0), wal.FrameCount())
}

func TestOpenWALAndRebuildIndex_ExistingWALWithFrames(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		_ = os.Remove(tmp.Name() + "-wal")
	}()

	// Create a WAL and write a committed transaction with two pages.
	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	pages := []WALPage{
		{Index: PageIndex(1), Data: makeTestPage(0xAA)},
		{Index: PageIndex(2), Data: makeTestPage(0xBB)},
	}
	require.NoError(t, w.AppendTransaction(pages))
	require.NoError(t, w.Close())

	// Now simulate a fresh open (crash recovery path).
	walIndex := NewWALIndex()
	wal, recovered, err := OpenWALAndRebuildIndex(tmp.Name(), PageSize, walIndex)
	require.NoError(t, err)
	require.NotNil(t, wal)
	defer func() { require.NoError(t, wal.Close()) }()

	assert.True(t, recovered, "existing WAL with committed frames must set recovered=true")
	assert.Equal(t, 2, walIndex.Size())

	data1, ok := walIndex.Lookup(PageIndex(1))
	require.True(t, ok)
	assert.Equal(t, byte(0xAA), data1[0])

	data2, ok := walIndex.Lookup(PageIndex(2))
	require.True(t, ok)
	assert.Equal(t, byte(0xBB), data2[0])
}

func TestOpenWALAndRebuildIndex_ExistingWALEmpty(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		_ = os.Remove(tmp.Name() + "-wal")
	}()

	// Create a WAL with no frames written (empty after header).
	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	walIndex := NewWALIndex()
	wal, recovered, err := OpenWALAndRebuildIndex(tmp.Name(), PageSize, walIndex)
	require.NoError(t, err)
	require.NotNil(t, wal)
	defer func() { require.NoError(t, wal.Close()) }()

	// Empty WAL — nothing to recover.
	assert.False(t, recovered)
	assert.Equal(t, 0, walIndex.Size())
}

func TestWAL_SynchronousAndWriteBufferSize(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp("", "wal_sync_test_*.wal")
	require.NoError(t, err)
	tmp.Close()
	defer os.Remove(tmp.Name())

	w, err := CreateWAL(tmp.Name(), PageSize)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	// Default synchronous mode is Normal.
	assert.Equal(t, SynchronousNormal, w.Synchronous())

	// SetWriteBufferSize is accepted without error.
	w.SetWriteBufferSize(64 * 1024)
}
