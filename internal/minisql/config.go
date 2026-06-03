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

	databaseHeaderMagicOffset          = 0
	databaseHeaderVersionOffset        = 8
	databaseHeaderPageSizeOffset       = 12
	databaseHeaderFirstFreePageOffset  = 16
	databaseHeaderFreePageCountOffset  = 20
	databaseHeaderEncryptionModeOffset = 24 // 1 byte: 0=none, 1=AES-256-CTR
	databaseHeaderEncryptionSaltOffset = 25 // 32 bytes: random per-database salt
	databaseHeaderMetadataSize         = 57 // bytes 57-99 reserved for future use
)

// Encryption mode constants stored in DatabaseHeader.EncryptionMode.
const (
	EncryptionModeNone      = uint8(0) // no encryption (default)
	EncryptionModeAES256CTR = uint8(1) // AES-256-CTR with HKDF-derived key
)

// DatabaseHeader stores the global database state persisted at the start of the first page.
// Bytes 0-99 of page 0 are always written as plaintext so that the encryption
// salt can be read before the cipher is bootstrapped.
type DatabaseHeader struct {
	FirstFreePage  PageIndex  // Points to first free page, 0 if none
	FreePageCount  uint32     // Number of free pages available
	EncryptionMode uint8      // 0 = none, 1 = AES-256-CTR
	EncryptionSalt [32]byte   // per-database random salt for HKDF key derivation
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
	buf[databaseHeaderEncryptionModeOffset] = h.EncryptionMode
	copy(buf[databaseHeaderEncryptionSaltOffset:], h.EncryptionSalt[:])
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
	dbHeader.EncryptionMode = buf[databaseHeaderEncryptionModeOffset]
	copy(dbHeader.EncryptionSalt[:], buf[databaseHeaderEncryptionSaltOffset:databaseHeaderEncryptionSaltOffset+32])
	return nil
}
