package minisql

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	JournalMagic      = "minisql\n"
	JournalVersion    = uint32(1)
	JournalHeaderSize = 28
	CommitMagic       = uint32(0xDEADBEEF)
)

// RollbackJournal implements a write-ahead rollback journal for crash recovery.
// Before modifying the database, original page contents are written to the journal.
// On crash, the journal is replayed to restore the database to its pre-transaction state.
type RollbackJournal struct {
	file     *os.File
	filepath string
	pageSize uint32
}

type JournalHeader struct {
	Magic      [8]byte
	Version    uint32
	PageSize   uint32
	NumPages   uint32
	Checksum   uint32
	PageHashes map[PageIndex]uint32 // Optional: per-page checksums
}

// CreateJournal creates a new journal file for the transaction.
func CreateJournal(dbPath string, pageSize uint32) (*RollbackJournal, error) {
	journalPath := dbPath + "-journal"

	// Create journal file (truncate if exists)
	file, err := os.OpenFile(journalPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("create journal file: %w", err)
	}

	journal := &RollbackJournal{
		file:     file,
		filepath: journalPath,
		pageSize: pageSize,
	}

	// Write initial header (will update NumPages later)
	if err := journal.writeHeader(0); err != nil {
		journal.Close()
		return nil, fmt.Errorf("write journal header: %w", err)
	}

	return journal, nil
}

// WritePageBefore writes the ORIGINAL page content to the journal before modification.
func (j *RollbackJournal) WritePageBefore(ctx context.Context, pageIdx PageIndex, originalPage *Page, pager Pager) error {
	// Marshal the original page
	buf := make([]byte, j.pageSize)
	_, err := marshalPage(originalPage, buf)
	if err != nil {
		return fmt.Errorf("marshal page: %w", err)
	}

	// Write page index (4 bytes)
	indexBuf := make([]byte, 4)
	indexBuf = marshalUint32(indexBuf, uint32(pageIdx), 0)
	if _, err := j.file.Write(indexBuf); err != nil {
		return fmt.Errorf("write page index: %w", err)
	}

	// Write page content
	if _, err := j.file.Write(buf); err != nil {
		return fmt.Errorf("write page data: %w", err)
	}

	return nil
}

// WriteCommitRecord writes a commit marker to the journal.
// This indicates that all pages have been written and the transaction can be committed.
func (j *RollbackJournal) WriteCommitRecord() error {
	commitBuf := make([]byte, 4)
	commitBuf = marshalUint32(commitBuf, CommitMagic, 0)
	if _, err := j.file.Write(commitBuf); err != nil {
		return fmt.Errorf("write commit record: %w", err)
	}

	// Sync to ensure commit record is on disk
	return j.file.Sync()
}

// Finalize updates the header with final page count and syncs the journal to disk.
func (j *RollbackJournal) Finalize(numPages int) error {
	// Seek back to header
	if _, err := j.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to header: %w", err)
	}

	// Write updated header
	if err := j.writeHeader(numPages); err != nil {
		return fmt.Errorf("update header: %w", err)
	}

	// Sync journal to disk - CRITICAL for crash recovery
	if err := j.file.Sync(); err != nil {
		return fmt.Errorf("sync journal: %w", err)
	}

	return nil
}

// Delete removes the journal file, signaling successful commit.
func (j *RollbackJournal) Delete() error {
	j.file.Close()
	if err := os.Remove(j.filepath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete journal: %w", err)
	}
	return nil
}

func (j *RollbackJournal) Close() error {
	return j.file.Close()
}

func (j *RollbackJournal) writeHeader(numPages int) error {
	header := make([]byte, JournalHeaderSize)

	i := uint64(0)

	// Magic bytes
	n := copy(header[i:8], []byte(JournalMagic))
	i += uint64(n)

	// Version
	header = marshalUint32(header, JournalVersion, i)
	i += 4

	// Page size
	header = marshalUint32(header, j.pageSize, i)
	i += 4

	// Number of pages in journal
	header = marshalUint32(header, uint32(numPages), i)
	i += 4

	// Checksum (simple CRC32 of header fields)
	checksum := crc32.ChecksumIEEE(header[0:i])
	header = marshalUint32(header, checksum, i)
	i += 4

	// Reserved bytes
	header = marshalUint32(header, 0, i)
	i += 4

	_, err := j.file.Write(header)
	return err
}

// RecoverFromJournal checks if a journal file exists and recovers the database if needed.
// This should be called when opening a database.
func RecoverFromJournal(dbPath string, pageSize int) error {
	journalPath := dbPath + "-journal"

	// Check if journal exists
	journalFile, err := os.Open(journalPath)
	if os.IsNotExist(err) {
		// No journal = clean shutdown, nothing to recover
		return nil
	}
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer journalFile.Close()

	// Read and validate journal header
	header, err := readJournalHeader(journalFile)
	if err != nil {
		return fmt.Errorf("read journal header: %w", err)
	}

	if header.PageSize != uint32(pageSize) {
		return fmt.Errorf("journal page size mismatch: journal=%d, db=%d", header.PageSize, pageSize)
	}

	// Open database file for writing
	dbFile, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open database for recovery: %w", err)
	}
	defer dbFile.Close()

	// Restore each page from journal
	for i := uint32(0); i < header.NumPages; i++ {
		// Read page index
		indexBuf := make([]byte, 4)
		if _, err := io.ReadFull(journalFile, indexBuf); err != nil {
			return fmt.Errorf("read page index %d: %w", i, err)
		}
		pageIdx := unmarshalUint32(indexBuf, 0)

		// Read page data
		pageData := make([]byte, pageSize)
		if _, err := io.ReadFull(journalFile, pageData); err != nil {
			return fmt.Errorf("read page data %d: %w", i, err)
		}

		// Write original page back to database
		offset := int64(pageIdx) * int64(pageSize)
		if _, err := dbFile.WriteAt(pageData, offset); err != nil {
			return fmt.Errorf("restore page %d: %w", pageIdx, err)
		}
	}

	// Sync database
	if err := dbFile.Sync(); err != nil {
		return fmt.Errorf("sync database after recovery: %w", err)
	}

	// Close files before deleting journal
	journalFile.Close()
	dbFile.Close()

	// Delete journal
	if err := os.Remove(journalPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete journal after recovery: %w", err)
	}

	return nil
}

func readJournalHeader(file *os.File) (*JournalHeader, error) {
	header := make([]byte, JournalHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	// Validate magic
	magic := string(header[0:8])
	if magic != JournalMagic {
		return nil, fmt.Errorf("invalid journal magic: got %q, want %q", magic, JournalMagic)
	}

	// Parse header
	h := new(JournalHeader)
	copy(h.Magic[:], header[0:8])
	h.Version = unmarshalUint32(header, 8)
	h.PageSize = unmarshalUint32(header, 12)
	h.NumPages = unmarshalUint32(header, 16)
	h.Checksum = unmarshalUint32(header, 20)

	// Validate checksum
	expectedChecksum := crc32.ChecksumIEEE(header[0:20])
	if h.Checksum != expectedChecksum {
		return nil, fmt.Errorf("journal header checksum mismatch")
	}

	return h, nil
}
