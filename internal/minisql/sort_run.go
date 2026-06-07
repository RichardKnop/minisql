package minisql

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// sortRunBufSize is the I/O buffer size used by runWriter and runReader.
// 64 KiB amortises the per-record syscall cost across ~1 000+ rows per flush,
// matching a common sort-memory threshold without over-allocating per reader.
const sortRunBufSize = 64 * 1024

// runWriter writes sorted Row records to a temporary file for external merge sort.
//
// Record format (per row):
//
//	[4 bytes: value-length, big-endian uint32]
//	[8 bytes: RowID, little-endian uint64]
//	[8 bytes: NullBitmask, little-endian uint64]
//	[value bytes, as produced by Row.Marshal()]
//
// All writes go through a bufio.Writer so that the two Write calls per row
// (header + value) are coalesced into large sequential OS writes rather than
// one syscall per field.
type runWriter struct {
	file *os.File
	buf  *bufio.Writer
}

func newRunWriter() (*runWriter, error) {
	f, err := os.CreateTemp("", "minisql_sort_*.run")
	if err != nil {
		return nil, fmt.Errorf("sort run: create temp file: %w", err)
	}
	return &runWriter{file: f, buf: bufio.NewWriterSize(f, sortRunBufSize)}, nil
}

func (w *runWriter) writeRow(r Row) error {
	valueBytes, err := r.Marshal()
	if err != nil {
		return fmt.Errorf("sort run: marshal row: %w", err)
	}
	var hdr [20]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(valueBytes)))
	binary.LittleEndian.PutUint64(hdr[4:12], uint64(r.Key))
	binary.LittleEndian.PutUint64(hdr[12:20], r.NullBitmask())
	if _, err := w.buf.Write(hdr[:]); err != nil {
		return fmt.Errorf("sort run: write header: %w", err)
	}
	if len(valueBytes) > 0 {
		if _, err := w.buf.Write(valueBytes); err != nil {
			return fmt.Errorf("sort run: write value: %w", err)
		}
	}
	return nil
}

func (w *runWriter) filePath() string { return w.file.Name() }

func (w *runWriter) close() error {
	if err := w.buf.Flush(); err != nil {
		_ = w.file.Close()
		return fmt.Errorf("sort run: flush write buffer: %w", err)
	}
	return w.file.Close()
}

// runReader reads Row records from a sorted temp file written by runWriter.
// Reads are buffered via bufio.Reader so that the two ReadFull calls per row
// (header + value) are served from an in-memory buffer rather than individual
// OS read syscalls.
type runReader struct {
	file    *os.File
	buf     *bufio.Reader
	columns []Column
	current Row
	err     error
	done    bool
}

func newRunReader(path string, columns []Column) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sort run: open %s: %w", path, err)
	}
	rr := &runReader{
		file:    f,
		buf:     bufio.NewReaderSize(f, sortRunBufSize),
		columns: columns,
	}
	rr.advance()
	if rr.err != nil {
		_ = f.Close()
		return nil, rr.err
	}
	return rr, nil
}

// Row returns the current row. Valid only when Done() == false.
func (rr *runReader) Row() Row { return rr.current }

// Done reports whether the reader has been exhausted or encountered an error.
func (rr *runReader) Done() bool { return rr.done }

// Err returns any error encountered during reading (distinct from normal EOF).
func (rr *runReader) Err() error { return rr.err }

// Next advances to the next row.
func (rr *runReader) Next() {
	if rr.done {
		return
	}
	rr.advance()
}

func (rr *runReader) advance() {
	var hdr [20]byte
	_, err := io.ReadFull(rr.buf, hdr[:])
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		rr.done = true
		return
	}
	if err != nil {
		rr.err = fmt.Errorf("sort run: read header: %w", err)
		rr.done = true
		return
	}
	valLen := binary.BigEndian.Uint32(hdr[0:4])
	key := RowID(binary.LittleEndian.Uint64(hdr[4:12]))
	nullBitmask := binary.LittleEndian.Uint64(hdr[12:20])

	// Each row gets its own buffer so that TextPointer.Data (which sub-slices
	// directly into valueBuf) remains valid for the lifetime of the row.
	valueBuf := make([]byte, valLen)
	if valLen > 0 {
		if _, err := io.ReadFull(rr.buf, valueBuf); err != nil {
			rr.err = fmt.Errorf("sort run: read value bytes: %w", err)
			rr.done = true
			return
		}
	}
	row, err := UnmarshalRow(valueBuf, rr.columns, key, nullBitmask)
	if err != nil {
		rr.err = fmt.Errorf("sort run: unmarshal row: %w", err)
		rr.done = true
		return
	}
	rr.current = row
}

func (rr *runReader) close() error { return rr.file.Close() }
