package minisql

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// WAL file-format constants.
const (
	// WALMagic is the 8-byte magic string at the start of every WAL file.
	WALMagic = "miniwal\n"
	// WALVersion is the current on-disk WAL format version.
	WALVersion = uint32(1)
	// WALFileHeaderSize is the fixed byte size of the WAL file header.
	WALFileHeaderSize = 32
	// WALFrameHeaderSize is the fixed byte size of each WAL frame header.
	WALFrameHeaderSize = 24
)

// WAL file header layout (32 bytes):
//
//	[0..7]   Magic    (8 bytes)  "miniwal\n"
//	[8..11]  Version  (4 bytes)  uint32 LE
//	[12..15] PageSize (4 bytes)  uint32 LE
//	[16..19] Salt1    (4 bytes)  uint32 LE — refreshed on each new/truncated WAL
//	[20..23] Salt2    (4 bytes)  uint32 LE
//	[24..27] Checksum (4 bytes)  CRC32-IEEE of bytes 0..23
//	[28..31] Reserved (4 bytes)
//
// WAL frame header layout (24 bytes):
//
//	[0..3]   PageIndex  (4 bytes) uint32 LE — index of the page in the DB file
//	[4..7]   CommitSize (4 bytes) uint32 LE — 0 = non-commit frame; >0 = commit (total pages in txn)
//	[8..11]  Salt1      (4 bytes) must equal the WAL file header Salt1
//	[12..15] Salt2      (4 bytes) must equal the WAL file header Salt2
//	[16..19] CRC1       (4 bytes) CRC32-IEEE of frame header bytes 0..15
//	[20..23] CRC2       (4 bytes) CRC32-IEEE of page data
//
// Immediately following each frame header is the raw page data (pageSize bytes).

// WALPage holds the page index and raw serialised content for one page to be
// appended to the WAL.
type WALPage struct {
	Data  []byte
	Index PageIndex
}

// WALReadFrame is a single validated, committed frame returned by ReadAllFrames.
type WALReadFrame struct {
	Data      []byte
	PageIndex PageIndex
}

// WAL is a write-ahead log providing crash-safe durability for the database.
//
// On each commit the transaction manager serialises all modified pages as WAL
// frames and calls AppendTransaction. A final "commit frame" (CommitSize > 0)
// marks the transaction as durable.  Readers check the WAL index (see
// WALIndex) before reading the main DB file so they always see the latest
// committed version of any page.
//
// Periodically, or on clean open, committed frames are copied back to the main
// DB file via Checkpoint and the WAL is truncated.
type WAL struct {
	file       *os.File
	filepath   string
	writeBuf   []byte
	nextOffset int64
	pageSize   uint32
	salt1      uint32
	salt2      uint32
}

// CreateWAL creates a new WAL file (truncating any existing file at that path).
func CreateWAL(dbPath string, pageSize uint32) (*WAL, error) {
	walPath := dbPath + "-wal"

	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create WAL file: %w", err)
	}

	w := &WAL{
		file:     file,
		filepath: walPath,
		pageSize: pageSize,
	}

	if err := w.refreshSalts(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("generate WAL salts: %w", err)
	}

	if err := w.writeFileHeader(); err != nil {
		return nil, errors.Join(fmt.Errorf("write WAL file header: %w", err), file.Close())
	}

	w.nextOffset = WALFileHeaderSize
	return w, nil
}

// OpenWAL opens an existing WAL file for reading and appending.
// Returns (nil, nil) when the WAL file does not exist (clean state).
func OpenWAL(dbPath string, pageSize uint32) (*WAL, error) {
	walPath := dbPath + "-wal"

	file, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open WAL file: %w", err)
	}

	w := &WAL{
		file:     file,
		filepath: walPath,
		pageSize: pageSize,
	}

	if err := w.readFileHeader(); err != nil {
		return nil, errors.Join(fmt.Errorf("read WAL file header: %w", err), file.Close())
	}

	// Position nextOffset at the end of the file for appending.
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("seek WAL end: %w", err), file.Close())
	}
	w.nextOffset = offset

	return w, nil
}

// AppendTransaction writes all modified pages as WAL frames and syncs the file.
// The last frame is marked as a commit frame (CommitSize = len(pages)).
// pages must be non-empty; each Data slice must be exactly pageSize bytes.
func (w *WAL) AppendTransaction(pages []WALPage) error {
	if len(pages) == 0 {
		return errors.New("cannot append empty transaction to WAL")
	}

	commitSize := uint32(len(pages))
	frameSize := int(WALFrameHeaderSize) + int(w.pageSize)
	need := frameSize * len(pages)
	if cap(w.writeBuf) < need {
		w.writeBuf = make([]byte, need)
	}
	buf := w.writeBuf[:need]

	for i, page := range pages {
		if uint32(len(page.Data)) != w.pageSize {
			return fmt.Errorf("WAL page %d: data length %d != page size %d", page.Index, len(page.Data), w.pageSize)
		}

		base := i * frameSize
		fh := buf[base : base+WALFrameHeaderSize]

		isCommit := i == len(pages)-1
		cs := uint32(0)
		if isCommit {
			cs = commitSize
		}

		// Frame header fields
		marshalUint32(fh, uint32(page.Index), 0)
		marshalUint32(fh, cs, 4)
		marshalUint32(fh, w.salt1, 8)
		marshalUint32(fh, w.salt2, 12)

		crc1 := crc32.ChecksumIEEE(fh[0:16])
		marshalUint32(fh, crc1, 16)

		crc2 := crc32.ChecksumIEEE(page.Data)
		marshalUint32(fh, crc2, 20)

		// Page data immediately follows the frame header
		copy(buf[base+WALFrameHeaderSize:base+frameSize], page.Data)
	}

	if _, err := w.file.WriteAt(buf, w.nextOffset); err != nil {
		return fmt.Errorf("write WAL frames: %w", err)
	}

	// fsync ensures the commit is durable before returning.
	if err := syscallFsync(w.file); err != nil {
		return fmt.Errorf("sync WAL after append: %w", err)
	}

	w.nextOffset += int64(len(buf))
	return nil
}

// ReadAllFrames scans the WAL file from the beginning and returns every frame
// that belongs to a committed transaction.  Frames whose salts or CRCs do not
// match are treated as the end of the valid region; any pending (uncommitted)
// frames before the scan stops are silently discarded.
func (w *WAL) ReadAllFrames() ([]WALReadFrame, error) {
	if _, err := w.file.Seek(WALFileHeaderSize, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to first WAL frame: %w", err)
	}

	var committed []WALReadFrame
	var pending []WALReadFrame

	fhBuf := make([]byte, WALFrameHeaderSize)
	pageData := make([]byte, w.pageSize)

	for {
		// Read frame header.
		if _, err := io.ReadFull(w.file, fhBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("read WAL frame header: %w", err)
		}

		// Read page data.
		if _, err := io.ReadFull(w.file, pageData); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("read WAL frame page data: %w", err)
		}

		// Salt check: stale frames from a previous WAL incarnation are rejected.
		salt1 := unmarshalUint32(fhBuf, 8)
		salt2 := unmarshalUint32(fhBuf, 12)
		if salt1 != w.salt1 || salt2 != w.salt2 {
			break
		}

		// CRC1 covers the first 16 bytes of the frame header.
		expectedCRC1 := crc32.ChecksumIEEE(fhBuf[0:16])
		if unmarshalUint32(fhBuf, 16) != expectedCRC1 {
			break
		}

		// CRC2 covers the page data.
		expectedCRC2 := crc32.ChecksumIEEE(pageData)
		if unmarshalUint32(fhBuf, 20) != expectedCRC2 {
			break
		}

		pageIdx := PageIndex(unmarshalUint32(fhBuf, 0))
		commitSize := unmarshalUint32(fhBuf, 4)

		dataCopy := make([]byte, w.pageSize)
		copy(dataCopy, pageData)

		pending = append(pending, WALReadFrame{
			PageIndex: pageIdx,
			Data:      dataCopy,
		})

		if commitSize > 0 {
			// Commit frame reached: all pending frames are now committed.
			committed = append(committed, pending...)
			pending = pending[:0]
		}
	}

	// Any remaining pending frames have no commit marker — discard them.
	return committed, nil
}

// Checkpoint copies all committed WAL pages to the database file.
// It writes the latest version of each page (last write wins when a page
// appears in multiple transactions) and then fsyncs the database file.
// Callers should call Truncate after a successful checkpoint.
func (w *WAL) Checkpoint(dbFile DBFile) error {
	// Build a latest-page map directly from the WAL file without an intermediate
	// []WALReadFrame slice.  A single shared read buffer (pageData) is reused for
	// every frame; we only allocate a fresh 4 KB slice when we need to keep (or
	// overwrite) the data for a given page index.
	if _, err := w.file.Seek(WALFileHeaderSize, io.SeekStart); err != nil {
		return fmt.Errorf("seek to first WAL frame for checkpoint: %w", err)
	}

	latest := make(map[PageIndex][]byte)
	fhBuf := make([]byte, WALFrameHeaderSize)
	pageData := make([]byte, w.pageSize)
	var pending []PageIndex // page indices in the current uncommitted group

	for {
		if _, err := io.ReadFull(w.file, fhBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("read WAL frame header for checkpoint: %w", err)
		}
		if _, err := io.ReadFull(w.file, pageData); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("read WAL frame data for checkpoint: %w", err)
		}

		// Salt validation
		if unmarshalUint32(fhBuf, 8) != w.salt1 || unmarshalUint32(fhBuf, 12) != w.salt2 {
			break
		}
		if crc32.ChecksumIEEE(fhBuf[0:16]) != unmarshalUint32(fhBuf, 16) {
			break
		}
		if crc32.ChecksumIEEE(pageData) != unmarshalUint32(fhBuf, 20) {
			break
		}

		pageIdx := PageIndex(unmarshalUint32(fhBuf, 0))
		commitSize := unmarshalUint32(fhBuf, 4)

		// Store page data: reuse an existing buffer for this page if one exists,
		// otherwise allocate a fresh one.
		buf, exists := latest[pageIdx]
		if !exists {
			buf = make([]byte, w.pageSize)
		}
		copy(buf, pageData)
		latest[pageIdx] = buf
		pending = append(pending, pageIdx)

		if commitSize > 0 {
			// Commit frame: all pending frames are confirmed; reset tracking slice.
			pending = pending[:0]
		}
	}

	// Discard pages that belong to an uncommitted trailing group.
	for _, idx := range pending {
		delete(latest, idx)
	}

	if len(latest) == 0 {
		return nil
	}

	// Sort page indices so that consecutive pages can be coalesced into a single
	// WriteAt call, replacing one syscall per page with one syscall per contiguous
	// run.  For append-heavy workloads new leaf pages are allocated sequentially, so
	// most pages end up in a single run.
	pageIndices := make([]PageIndex, 0, len(latest))
	for idx := range latest {
		pageIndices = append(pageIndices, idx)
	}
	sort.Slice(pageIndices, func(i, j int) bool { return pageIndices[i] < pageIndices[j] })

	psz := int64(w.pageSize)
	i := 0
	for i < len(pageIndices) {
		// Extend the run while pages are consecutive.
		j := i + 1
		for j < len(pageIndices) && pageIndices[j] == pageIndices[j-1]+1 {
			j++
		}

		var buf []byte
		if j == i+1 {
			// Single page — use its buffer directly (no copy).
			buf = latest[pageIndices[i]]
		} else {
			// Multiple consecutive pages — concatenate into one buffer.
			runLen := j - i
			buf = make([]byte, int64(runLen)*psz)
			for k := 0; k < runLen; k++ {
				copy(buf[int64(k)*psz:], latest[pageIndices[i+k]])
			}
		}

		offset := int64(pageIndices[i]) * psz
		if _, err := dbFile.WriteAt(buf, offset); err != nil {
			return fmt.Errorf("checkpoint pages %d..%d: %w", pageIndices[i], pageIndices[j-1], err)
		}
		i = j
	}

	if err := fastSync(dbFile); err != nil {
		return fmt.Errorf("sync database after WAL checkpoint: %w", err)
	}

	return nil
}

// Truncate resets the WAL to an empty state after a successful checkpoint.
// The file header is rewritten with fresh salts so that any unreachable frames
// left behind by a partial truncation are automatically invalidated.
func (w *WAL) Truncate() error {
	if err := w.file.Truncate(WALFileHeaderSize); err != nil {
		return fmt.Errorf("truncate WAL file: %w", err)
	}

	if err := w.refreshSalts(); err != nil {
		return fmt.Errorf("generate new WAL salts on truncate: %w", err)
	}

	if err := w.writeFileHeader(); err != nil {
		return fmt.Errorf("rewrite WAL header after truncate: %w", err)
	}

	if err := syscallFsync(w.file); err != nil {
		return fmt.Errorf("sync WAL after truncate: %w", err)
	}

	w.nextOffset = WALFileHeaderSize
	return nil
}

// Delete closes and removes the WAL file. Used when transitioning back to a
// clean (no-WAL) state, e.g. after a full checkpoint on clean shutdown.
func (w *WAL) Delete() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close WAL file before delete: %w", err)
	}
	if err := os.Remove(w.filepath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete WAL: %w", err)
	}
	return nil
}

// Close closes the underlying WAL file handle without removing it.
func (w *WAL) Close() error {
	return w.file.Close()
}

// FrameCount returns the total number of frame slots written since the last
// Truncate (including both committed and uncommitted frames).
func (w *WAL) FrameCount() int64 {
	var (
		frameSize = int64(WALFrameHeaderSize) + int64(w.pageSize)
		available = w.nextOffset - WALFileHeaderSize
	)
	if available <= 0 {
		return 0
	}
	return available / frameSize
}

// OpenWALAndRebuildIndex is the startup routine for WAL mode.
//
// It checks for an existing WAL file at dbPath+"-wal":
//
//   - If the file exists, it opens it for appending, reads all committed
//     frames, and rebuilds walIndex from those frames.  This is the crash-
//     recovery path: committed writes that had not yet been checkpointed to
//     the main DB file are reinstated in the in-memory index so reads see the
//     correct data immediately.
//
//   - If no WAL file exists, a fresh WAL file is created.
//
// The caller owns the returned *WAL and must call WAL.Close() when done.
// walIndex is populated in-place; it must be non-nil.
// recovered is true when an existing WAL file with committed frames was found.
func OpenWALAndRebuildIndex(dbPath string, pageSize uint32, walIndex *WALIndex) (wal *WAL, recovered bool, err error) {
	wal, err = OpenWAL(dbPath, pageSize)
	if err != nil {
		return nil, false, fmt.Errorf("open WAL for startup: %w", err)
	}

	if wal != nil {
		// Existing WAL — read committed frames and rebuild the index.
		frames, err := wal.ReadAllFrames()
		if err != nil {
			_ = wal.Close()
			return nil, false, fmt.Errorf("read WAL frames on startup: %w", err)
		}
		if len(frames) > 0 {
			walIndex.Rebuild(frames)
			recovered = true
		}
		return wal, recovered, nil
	}

	// No WAL file — create a fresh one.
	wal, err = CreateWAL(dbPath, pageSize)
	if err != nil {
		return nil, false, fmt.Errorf("create WAL on startup: %w", err)
	}
	return wal, false, nil
}

// RecoverFromWAL checks for a WAL file at dbPath+"-wal" and, if found,
// replays all committed frames into dbFile. The WAL is truncated on success.
// Returns true when recovery was performed.
func RecoverFromWAL(dbPath string, dbFile DBFile, pageSize uint32) (bool, error) {
	w, err := OpenWAL(dbPath, pageSize)
	if err != nil {
		return false, fmt.Errorf("open WAL for recovery: %w", err)
	}
	if w == nil {
		// No WAL file — nothing to recover.
		return false, nil
	}
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()

	if err := w.Checkpoint(dbFile); err != nil {
		return false, fmt.Errorf("WAL recovery checkpoint: %w", err)
	}

	if err := w.Truncate(); err != nil {
		return false, fmt.Errorf("WAL recovery truncate: %w", err)
	}

	_ = w.Close()
	w = nil
	return true, nil
}

// refreshSalts generates new random salt values for the WAL.
func (w *WAL) refreshSalts() error {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return err
	}
	w.salt1 = binary.LittleEndian.Uint32(buf[0:4])
	w.salt2 = binary.LittleEndian.Uint32(buf[4:8])
	return nil
}

func (w *WAL) writeFileHeader() error {
	buf := make([]byte, WALFileHeaderSize)
	copy(buf[0:8], []byte(WALMagic))
	marshalUint32(buf, WALVersion, 8)
	marshalUint32(buf, w.pageSize, 12)
	marshalUint32(buf, w.salt1, 16)
	marshalUint32(buf, w.salt2, 20)
	checksum := crc32.ChecksumIEEE(buf[0:24])
	marshalUint32(buf, checksum, 24)
	// bytes 28..31: reserved, remain zero

	if _, err := w.file.WriteAt(buf, 0); err != nil {
		return err
	}
	return nil
}

func (w *WAL) readFileHeader() error {
	buf := make([]byte, WALFileHeaderSize)
	if _, err := w.file.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("read WAL file header bytes: %w", err)
	}

	if string(buf[0:8]) != WALMagic {
		return fmt.Errorf("invalid WAL magic: got %q, want %q", string(buf[0:8]), WALMagic)
	}

	version := unmarshalUint32(buf, 8)
	if version != WALVersion {
		return fmt.Errorf("unsupported WAL version %d", version)
	}

	storedPageSize := unmarshalUint32(buf, 12)
	if storedPageSize != w.pageSize {
		return fmt.Errorf("WAL page size mismatch: WAL=%d, DB=%d", storedPageSize, w.pageSize)
	}

	w.salt1 = unmarshalUint32(buf, 16)
	w.salt2 = unmarshalUint32(buf, 20)

	expectedChecksum := crc32.ChecksumIEEE(buf[0:24])
	if unmarshalUint32(buf, 24) != expectedChecksum {
		return errors.New("WAL file header checksum mismatch")
	}

	return nil
}
