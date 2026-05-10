package minisql

import (
	"bytes"
	"fmt"
)

const (
	// DatabaseHeaderMagic identifies a MiniSQL database file.
	DatabaseHeaderMagic = "minisql\x00"
	// DatabaseFileFormatVersion identifies the current on-disk file header format.
	DatabaseFileFormatVersion = uint32(1)

	databaseHeaderMagicOffset         = 0
	databaseHeaderVersionOffset       = 8
	databaseHeaderPageSizeOffset      = 12
	databaseHeaderFirstFreePageOffset = 16
	databaseHeaderFreePageCountOffset = 20
	databaseHeaderMetadataSize        = 24
)

// DatabaseHeader stores the global database state persisted at the start of the first page.
type DatabaseHeader struct {
	FirstFreePage PageIndex // Points to first free page, 0 if none
	FreePageCount uint32    // Number of free pages available
}

// Size returns the fixed serialised byte size of the database header.
func (h *DatabaseHeader) Size() uint64 {
	return RootPageConfigSize
}

// Marshal serialises the database header to a byte slice.
func (h *DatabaseHeader) Marshal() ([]byte, error) {
	buf := make([]byte, h.Size())
	if err := h.MarshalTo(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// MarshalTo serialises the database header into the provided buffer.
// The buffer must be at least RootPageConfigSize bytes long.
func (h *DatabaseHeader) MarshalTo(buf []byte) error {
	if len(buf) < int(h.Size()) {
		return fmt.Errorf("database header buffer too small: got %d bytes, want at least %d", len(buf), h.Size())
	}
	copy(buf[databaseHeaderMagicOffset:], []byte(DatabaseHeaderMagic))
	marshalUint32(buf, DatabaseFileFormatVersion, databaseHeaderVersionOffset)
	marshalUint32(buf, PageSize, databaseHeaderPageSizeOffset)
	marshalUint32(buf, uint32(h.FirstFreePage), databaseHeaderFirstFreePageOffset)
	marshalUint32(buf, h.FreePageCount, databaseHeaderFreePageCountOffset)
	return nil
}

// UnmarshalDatabaseHeader deserialises a database header from the given byte slice.
func UnmarshalDatabaseHeader(buf []byte, dbHeader *DatabaseHeader) error {
	if len(buf) < int(RootPageConfigSize) {
		return fmt.Errorf("database header too small: got %d bytes, want at least %d", len(buf), RootPageConfigSize)
	}

	if !bytes.Equal(buf[databaseHeaderMagicOffset:databaseHeaderVersionOffset], []byte(DatabaseHeaderMagic)) {
		return fmt.Errorf("invalid database header magic")
	}

	version := unmarshalUint32(buf, databaseHeaderVersionOffset)
	if version != DatabaseFileFormatVersion {
		return fmt.Errorf("unsupported database file format version %d", version)
	}

	storedPageSize := unmarshalUint32(buf, databaseHeaderPageSizeOffset)
	if storedPageSize != PageSize {
		return fmt.Errorf("unsupported database page size %d", storedPageSize)
	}

	dbHeader.FirstFreePage = PageIndex(unmarshalUint32(buf, databaseHeaderFirstFreePageOffset))
	dbHeader.FreePageCount = unmarshalUint32(buf, databaseHeaderFreePageCountOffset)
	return nil
}
